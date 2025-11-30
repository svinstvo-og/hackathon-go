package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// --- In-Memory Database ---

type User struct {
	Username string
	Password string
}

type Order struct {
	ID            string
	Price         int64
	Quantity      int64
	DeliveryStart int64
	DeliveryEnd   int64
	Owner         string // Username
	Status        string // "ACTIVE" (V2), "OPEN" (V1/Legacy), "FILLED", "CANCELLED"
	Side          string // "buy" or "sell"
	Version       int    // 1 or 2
	Timestamp     int64  // Time of submission (Created At)
	ExecutionType string // Execution behaviour (GTC/IOC/FOK)
}

type Trade struct {
	ID            string
	BuyerID       string
	SellerID      string
	Price         int64
	Quantity      int64
	Timestamp     int64
	DeliveryStart int64
	DeliveryEnd   int64
	Version       int
}

var (
	mu           sync.RWMutex
	users              = make(map[string]User)
	tokens             = make(map[string]string)
	dnaSamples         = make(map[string][]string)
	orders             = make(map[string]*Order)
	trades             = make([]*Trade, 0)
	balances           = make(map[string]int64) // actual credit balances
	collaterals        = make(map[string]int64) // max allowed negative balance
	orderCounter int64 = 0
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all connections
	},
}

type tradeStreamHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

var hub = tradeStreamHub{
	clients:    make(map[*websocket.Conn]bool),
	broadcast:  make(chan []byte, 256),
	register:   make(chan *websocket.Conn),
	unregister: make(chan *websocket.Conn),
}

var broadcastCtrl = struct {
	sync.Mutex
	paused bool
	buffer []*Trade
}{}

func (h *tradeStreamHub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.Close()
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				err := client.WriteMessage(websocket.BinaryMessage, message)
				if err != nil {
					go func(c *websocket.Conn) {
						h.unregister <- c
					}(client)
				}
			}
			h.mu.Unlock()
		}
	}
}

func tradeStreamHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}
	hub.register <- conn

	// Keep the connection alive, but we don't need to read from it.
	// The unregister logic will handle cleanup on disconnect.
	for {
		if _, _, err := conn.NextReader(); err != nil {
			hub.unregister <- conn
			break
		}
	}
}

var nowFunc = func() time.Time {
	return time.Now().UTC()
}

func nowMillis() int64 {
	return nowFunc().UnixMilli()
}

// --- GalacticBuf Protocol Constants ---

const (
	ProtoV1 = 0x01
	ProtoV2 = 0x02

	TypeInt    = 0x01
	TypeString = 0x02
	TypeList   = 0x03
	TypeObject = 0x04
	TypeBytes  = 0x05
)

// default "unlimited" collateral (very large number)
const UnlimitedCollateral int64 = 1<<62 - 1

const (
	ExecutionTypeGTC = "GTC"
	ExecutionTypeIOC = "IOC"
	ExecutionTypeFOK = "FOK"
)

func normalizeExecutionType(execType string) string {
	switch strings.ToUpper(execType) {
	case ExecutionTypeIOC:
		return ExecutionTypeIOC
	case ExecutionTypeFOK:
		return ExecutionTypeFOK
	default:
		return ExecutionTypeGTC
	}
}

func parseExecutionTypeInput(raw string) (string, error) {
	if raw == "" {
		return ExecutionTypeGTC, nil
	}
	raw = strings.ToUpper(raw)
	switch raw {
	case ExecutionTypeGTC, ExecutionTypeIOC, ExecutionTypeFOK:
		return raw, nil
	default:
		return "", fmt.Errorf("invalid execution_type")
	}
}

type GValue interface{}

// --- Encoder (Always V2) ---

func EncodeMessage(data map[string]GValue) ([]byte, error) {
	bodyBuffer := new(bytes.Buffer)
	if err := writeFieldsV2(bodyBuffer, data); err != nil {
		return nil, err
	}
	bodyBytes := bodyBuffer.Bytes()

	header := new(bytes.Buffer)
	header.WriteByte(ProtoV2)
	header.WriteByte(byte(len(data)))
	totalLen := 6 + len(bodyBytes)
	binary.Write(header, binary.BigEndian, uint32(totalLen))

	return append(header.Bytes(), bodyBytes...), nil
}

func writeFieldsV2(buf *bytes.Buffer, data map[string]GValue) error {
	for name, val := range data {
		if len(name) > 255 {
			return fmt.Errorf("field name too long")
		}
		buf.WriteByte(byte(len(name)))
		buf.WriteString(name)

		switch v := val.(type) {
		case int64:
			buf.WriteByte(TypeInt)
			binary.Write(buf, binary.BigEndian, v)
		case int:
			buf.WriteByte(TypeInt)
			binary.Write(buf, binary.BigEndian, int64(v))
		case string:
			buf.WriteByte(TypeString)
			binary.Write(buf, binary.BigEndian, uint32(len(v)))
			buf.WriteString(v)
		case []byte:
			buf.WriteByte(TypeBytes)
			binary.Write(buf, binary.BigEndian, uint32(len(v)))
			buf.Write(v)
		case []map[string]GValue:
			buf.WriteByte(TypeList)
			buf.WriteByte(TypeObject)
			binary.Write(buf, binary.BigEndian, uint32(len(v)))
			for _, obj := range v {
				buf.WriteByte(byte(len(obj)))
				if err := writeFieldsV2(buf, obj); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported type for encoding: %T", v)
		}
	}
	return nil
}

// --- Decoder (Dispatcher V1/V2) ---

func DecodeMessage(r io.Reader) (map[string]GValue, error) {
	versionByte := make([]byte, 1)
	if _, err := io.ReadFull(r, versionByte); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, err
	}

	if versionByte[0] == ProtoV1 {
		return decodeV1(r)
	} else if versionByte[0] == ProtoV2 {
		return decodeV2(r)
	} else {
		return nil, fmt.Errorf("unknown protocol version: 0x%x", versionByte[0])
	}
}

// V1 Logic
func decodeV1(r io.Reader) (map[string]GValue, error) {
	headerRem := make([]byte, 3)
	if _, err := io.ReadFull(r, headerRem); err != nil {
		return nil, err
	}
	fieldCount := int(headerRem[0])
	return readFieldsV1(r, fieldCount)
}

func readFieldsV1(r io.Reader, count int) (map[string]GValue, error) {
	result := make(map[string]GValue)
	for i := 0; i < count; i++ {
		var nameLen uint8
		if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}
		fieldName := string(nameBytes)

		var typeInd uint8
		if err := binary.Read(r, binary.BigEndian, &typeInd); err != nil {
			return nil, err
		}

		val, err := readValueV1(r, typeInd)
		if err != nil {
			return nil, err
		}
		result[fieldName] = val
	}
	return result, nil
}

func readValueV1(r io.Reader, typeInd uint8) (GValue, error) {
	switch typeInd {
	case TypeInt:
		var v int64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case TypeString:
		var l uint16
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	case TypeList:
		var elemType uint8
		binary.Read(r, binary.BigEndian, &elemType)
		var count uint16
		binary.Read(r, binary.BigEndian, &count)
		list := make([]GValue, 0, count)
		for k := 0; k < int(count); k++ {
			if elemType == TypeObject {
				var fc uint8
				binary.Read(r, binary.BigEndian, &fc)
				obj, _ := readFieldsV1(r, int(fc))
				list = append(list, obj)
			} else {
				v, _ := readValueV1(r, elemType)
				list = append(list, v)
			}
		}
		return list, nil
	case TypeObject:
		var fc uint8
		binary.Read(r, binary.BigEndian, &fc)
		return readFieldsV1(r, int(fc))
	default:
		return nil, fmt.Errorf("unknown type V1 %x", typeInd)
	}
}

// V2 Logic
func decodeV2(r io.Reader) (map[string]GValue, error) {
	headerRem := make([]byte, 5)
	if _, err := io.ReadFull(r, headerRem); err != nil {
		return nil, err
	}
	fieldCount := int(headerRem[0])
	return readFieldsV2(r, fieldCount)
}

func readFieldsV2(r io.Reader, count int) (map[string]GValue, error) {
	result := make(map[string]GValue)
	for i := 0; i < count; i++ {
		var nameLen uint8
		if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}
		fieldName := string(nameBytes)

		var typeInd uint8
		if err := binary.Read(r, binary.BigEndian, &typeInd); err != nil {
			return nil, err
		}

		val, err := readValueV2(r, typeInd)
		if err != nil {
			return nil, err
		}
		result[fieldName] = val
	}
	return result, nil
}

func readValueV2(r io.Reader, typeInd uint8) (GValue, error) {
	switch typeInd {
	case TypeInt:
		var v int64
		if err := binary.Read(r, binary.BigEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case TypeString:
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		if l > 100*1024*1024 {
			return nil, fmt.Errorf("string too large")
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	case TypeBytes:
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		if l > 100*1024*1024 {
			return nil, fmt.Errorf("bytes too large")
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	case TypeList:
		var elemType uint8
		binary.Read(r, binary.BigEndian, &elemType)
		var count uint32
		binary.Read(r, binary.BigEndian, &count)
		if count > 100000 {
			return nil, fmt.Errorf("list too large")
		}
		list := make([]GValue, 0, count)
		for k := 0; k < int(count); k++ {
			if elemType == TypeObject {
				var fc uint8
				binary.Read(r, binary.BigEndian, &fc)
				obj, _ := readFieldsV2(r, int(fc))
				list = append(list, obj)
			} else {
				v, _ := readValueV2(r, elemType)
				list = append(list, v)
			}
		}
		return list, nil
	case TypeObject:
		var fc uint8
		binary.Read(r, binary.BigEndian, &fc)
		return readFieldsV2(r, int(fc))
	default:
		return nil, fmt.Errorf("unknown type V2 %x", typeInd)
	}
}

// --- Helpers ---

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func getUserFromToken(r *http.Request) (string, bool) {
	authHeader := r.Header.Get("Authorization")
	if len(authHeader) < 7 || authHeader[:7] != "Bearer " {
		return "", false
	}
	token := authHeader[7:]
	mu.RLock()
	defer mu.RUnlock()
	user, ok := tokens[token]
	return user, ok
}

func getUserFromRawToken(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	mu.RLock()
	defer mu.RUnlock()
	user, ok := tokens[token]
	return user, ok
}

func validateDNASample(s string) bool {
	if s == "" || len(s)%3 != 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case 'C', 'G', 'A', 'T':
		default:
			return false
		}
	}
	return true
}

func splitCodons(s string) []string {
	codons := make([]string, 0, len(s)/3)
	for i := 0; i < len(s); i += 3 {
		codons = append(codons, s[i:i+3])
	}
	return codons
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func codonEditDistanceBounded(a, b []string, limit int) int {
	m := len(a)
	n := len(b)
	if limit < 0 {
		limit = 0
	}
	if absInt(m-n) > limit {
		return limit + 1
	}
	if limit == 0 {
		if m != n {
			return limit + 1
		}
		for i := range a {
			if a[i] != b[i] {
				return limit + 1
			}
		}
		return 0
	}

	inf := limit + 1
	prev := make(map[int]int, 2*limit+2)
	for j := 0; j <= n && j <= limit; j++ {
		prev[j] = j
	}

	for i := 1; i <= m; i++ {
		curr := make(map[int]int, 2*limit+2)
		startJ := i - limit
		if startJ < 0 {
			startJ = 0
		}
		endJ := i + limit
		if endJ > n {
			endJ = n
		}

		for j := startJ; j <= endJ; j++ {
			del := inf
			if v, ok := prev[j]; ok {
				del = v + 1
			}
			ins := inf
			if v, ok := curr[j-1]; ok {
				ins = v + 1
			}
			sub := inf
			if j > 0 {
				if v, ok := prev[j-1]; ok {
					sub = v
					if a[i-1] != b[j-1] {
						sub++
					}
				}
			}

			val := del
			if ins < val {
				val = ins
			}
			if sub < val {
				val = sub
			}
			if val <= limit {
				curr[j] = val
			}
		}

		if len(curr) == 0 {
			return limit + 1
		}

		minRow := inf
		for _, v := range curr {
			if v < minRow {
				minRow = v
			}
		}
		if minRow > limit {
			return limit + 1
		}
		prev = curr
	}

	if v, ok := prev[n]; ok && v <= limit {
		return v
	}
	return limit + 1
}

// cashflowForOrder returns the net balance change if this order is fully filled.
// Positive = balance increases, Negative = balance decreases.
// Must be called with mu locked.
func cashflowForOrder(o *Order) int64 {
	if o.Quantity <= 0 {
		return 0
	}
	if o.Side == "buy" {
		// Buyer pays price * qty
		return -o.Price * o.Quantity
	}
	// Seller receives price * qty
	return o.Price * o.Quantity
}

// orderRequiresCollateral returns true if placing this order can *reduce* balance
// and therefore must pass the collateral check.
func orderRequiresCollateral(o *Order) bool {
	if o.Side == "buy" && o.Price > 0 {
		return true
	}
	if o.Side == "sell" && o.Price < 0 {
		return true
	}
	return false
}

// computePotentialBalanceLocked calculates balance after all active orders are
// hypothetically fully filled (plus an optional extra order).
// mu must already be locked (RLock or Lock).
func computePotentialBalanceLocked(username string, extra *Order) int64 {
	bal := balances[username]

	for _, o := range orders {
		if o.Owner != username {
			continue
		}
		if o.Status != "ACTIVE" && o.Status != "OPEN" {
			continue
		}
		bal += cashflowForOrder(o)
	}

	if extra != nil {
		bal += cashflowForOrder(extra)
	}

	return bal
}

// getUserCollateralLocked returns the user's collateral limit,
// defaulting to UnlimitedCollateral if not explicitly set.
// mu must be locked.
func getUserCollateralLocked(username string) int64 {
	if c, ok := collaterals[username]; ok {
		return c
	}
	return UnlimitedCollateral
}

func contractTradingWindow(startMs int64) (openMs, closeMs int64) {
	start := time.UnixMilli(startMs).UTC()
	year, month, day := start.Date()
	midnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	open := midnight.AddDate(0, 0, -15)
	close := start.Add(-time.Minute)
	return open.UnixMilli(), close.UnixMilli()
}

func contractTradingWindowStatus(startMs int64, now time.Time) int {
	open, close := contractTradingWindow(startMs)
	nowMs := now.UTC().UnixMilli()
	if nowMs < open {
		return -1
	}
	if nowMs > close {
		return 1
	}
	return 0
}

// --- HTTP Handlers ---

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	username, _ := data["username"].(string)
	password, _ := data["password"].(string)

	if username == "" || password == "" {
		http.Error(w, "Empty fields", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	if _, exists := users[username]; exists {
		http.Error(w, "Conflict", http.StatusConflict)
		return
	}
	users[username] = User{Username: username, Password: password}
	balances[username] = 0
	collaterals[username] = UnlimitedCollateral
	persistStateLocked()
	w.WriteHeader(http.StatusNoContent)
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	username, _ := data["username"].(string)
	password, _ := data["password"].(string)

	mu.Lock()
	defer mu.Unlock()
	u, exists := users[username]
	if !exists || u.Password != password {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	token := generateToken()
	tokens[token] = username

	resp := map[string]GValue{"token": token}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

func passwordHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	username, _ := data["username"].(string)
	oldPass, _ := data["old_password"].(string)
	newPass, _ := data["new_password"].(string)

	if username == "" || oldPass == "" || newPass == "" {
		http.Error(w, "Empty fields", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	u, exists := users[username]
	if !exists || u.Password != oldPass {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	u.Password = newPass
	users[username] = u

	for token, user := range tokens {
		if user == username {
			delete(tokens, token)
		}
	}
	persistStateLocked()
	w.WriteHeader(http.StatusNoContent)
}

func dnaSubmitHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	username, okU := data["username"].(string)
	password, okP := data["password"].(string)
	dna, okD := data["dna_sample"].(string)
	if !okU || !okP || !okD || username == "" || password == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if !validateDNASample(dna) {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	u, exists := users[username]
	if !exists || u.Password != password {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	existing := dnaSamples[username]
	for _, sample := range existing {
		if sample == dna {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	dnaSamples[username] = append(existing, dna)
	persistStateLocked()
	w.WriteHeader(http.StatusNoContent)
}

func dnaLoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	username, okU := data["username"].(string)
	dna, okD := data["dna_sample"].(string)
	if !okU || !okD || username == "" || dna == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if !validateDNASample(dna) {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	submittedCodons := splitCodons(dna)

	mu.RLock()
	_, userExists := users[username]
	samples := dnaSamples[username]
	mu.RUnlock()

	if !userExists || len(samples) == 0 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	matched := false
	for _, ref := range samples {
		refCodons := splitCodons(ref)
		allowed := len(refCodons) / 100000
		dist := codonEditDistanceBounded(refCodons, submittedCodons, allowed)
		if dist <= allowed {
			matched = true
			break
		}
	}

	if !matched {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	token := generateToken()
	mu.Lock()
	tokens[token] = username
	mu.Unlock()

	resp := map[string]GValue{"token": token}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

// V1 Orders Handler (Legacy)
func ordersV1Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		startStr := q.Get("delivery_start")
		endStr := q.Get("delivery_end")

		if startStr == "" || endStr == "" {
			http.Error(w, "Missing query params", http.StatusBadRequest)
			return
		}

		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "Invalid timestamps", http.StatusBadRequest)
			return
		}

		mu.RLock()
		var filtered []*Order
		for _, o := range orders {
			if o.Version == 1 && o.Status == "OPEN" && o.DeliveryStart == start && o.DeliveryEnd == end {
				filtered = append(filtered, o)
			}
		}
		mu.RUnlock()

		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].Price < filtered[j].Price
		})

		list := make([]map[string]GValue, 0, len(filtered))
		for _, o := range filtered {
			list = append(list, map[string]GValue{
				"order_id":       o.ID,
				"price":          o.Price,
				"quantity":       o.Quantity,
				"delivery_start": o.DeliveryStart,
				"delivery_end":   o.DeliveryEnd,
			})
		}

		resp := map[string]GValue{"orders": list}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
		return
	}

	if r.Method == http.MethodPost {
		username, authOk := getUserFromToken(r)
		if !authOk {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		data, err := DecodeMessage(r.Body)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		price, ok1 := data["price"].(int64)
		quantity, ok2 := data["quantity"].(int64)
		start, ok3 := data["delivery_start"].(int64)
		end, ok4 := data["delivery_end"].(int64)

		if !ok1 || !ok2 || !ok3 || !ok4 {
			http.Error(w, "Missing fields", http.StatusBadRequest)
			return
		}
		if quantity <= 0 {
			http.Error(w, "Quantity must be positive", http.StatusBadRequest)
			return
		}
		const hourMs = 3600000
		if start%hourMs != 0 || end%hourMs != 0 || end <= start || (end-start) != hourMs {
			http.Error(w, "Invalid Contract Timestamps", http.StatusBadRequest)
			return
		}

		mu.Lock()
		orderCounter++
		orderID := fmt.Sprintf("ord-%d", orderCounter)
		newOrder := &Order{
			ID:            orderID,
			Price:         price,
			Quantity:      quantity,
			DeliveryStart: start,
			DeliveryEnd:   end,
			Owner:         username,
			Status:        "OPEN", // V1 legacy status
			Side:          "sell",
			Version:       1,
			Timestamp:     nowMillis(),
			ExecutionType: ExecutionTypeGTC,
		}
		orders[orderID] = newOrder
		mu.Unlock()

		resp := map[string]GValue{"order_id": orderID}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
		return
	}
}

// matchOrder executes the core matching logic.
// It assumes mu is already locked.
func detectSelfMatchLocked(order *Order) *Order {
	for _, resting := range orders {
		if resting == nil || resting.ID == order.ID {
			continue
		}
		if resting.Owner != order.Owner {
			continue
		}
		if resting.Version != 2 {
			continue
		}
		if resting.Status != "ACTIVE" && resting.Status != "OPEN" {
			continue
		}
		if resting.Quantity <= 0 {
			continue
		}
		if resting.DeliveryStart != order.DeliveryStart || resting.DeliveryEnd != order.DeliveryEnd {
			continue
		}
		if resting.Side == order.Side {
			continue
		}
		if order.Side == "buy" && order.Price >= resting.Price {
			return resting
		}
		if order.Side == "sell" && order.Price <= resting.Price {
			return resting
		}
	}
	return nil
}

func matchOrder(order *Order) int64 {
	execType := normalizeExecutionType(order.ExecutionType)
	var filledQty int64 = 0
	var matchingOrders []*Order

	// 1. Identify compatible orders
	for _, o := range orders {
		// Compatible if V2, Active/Open, and contract matches
		isCompatibleStatus := (o.Status == "ACTIVE" || o.Status == "OPEN")
		if o.Version == 2 && isCompatibleStatus &&
			o.DeliveryStart == order.DeliveryStart && o.DeliveryEnd == order.DeliveryEnd {

			if order.Side == "buy" && o.Side == "sell" && order.Price >= o.Price {
				matchingOrders = append(matchingOrders, o)
			} else if order.Side == "sell" && o.Side == "buy" && order.Price <= o.Price {
				matchingOrders = append(matchingOrders, o)
			}
		}
	}

	// 2. Sort by Price-Time Priority
	if order.Side == "buy" {
		// Incoming Buy: Wants cheapest sells first
		sort.Slice(matchingOrders, func(i, j int) bool {
			if matchingOrders[i].Price != matchingOrders[j].Price {
				return matchingOrders[i].Price < matchingOrders[j].Price // Lowest Price First
			}
			return matchingOrders[i].Timestamp < matchingOrders[j].Timestamp // Oldest First (FIFO)
		})
	} else {
		// Incoming Sell: Wants highest bids first
		sort.Slice(matchingOrders, func(i, j int) bool {
			if matchingOrders[i].Price != matchingOrders[j].Price {
				return matchingOrders[i].Price > matchingOrders[j].Price // Highest Price First
			}
			return matchingOrders[i].Timestamp < matchingOrders[j].Timestamp // Oldest First (FIFO)
		})
	}

	if execType == ExecutionTypeFOK {
		required := order.Quantity
		var available int64
		for _, resting := range matchingOrders {
			if order.ID == resting.ID || resting.Quantity <= 0 {
				continue
			}
			available += resting.Quantity
			if available >= required {
				break
			}
		}
		if available < required {
			order.Status = "CANCELLED"
			return 0
		}
	}

	now := time.Now().UnixMilli()

	// 3. Execute Matches
	for _, matchOrder := range matchingOrders {
		if order.Quantity <= 0 {
			break
		}
		// Skip self-matching if modifying an existing order
		if order.ID == matchOrder.ID {
			continue
		}

		tradeQty := order.Quantity
		if matchOrder.Quantity < tradeQty {
			tradeQty = matchOrder.Quantity
		}

		// Trade Price is always the maker's (resting) price
		tradePrice := matchOrder.Price

		var buyerID, sellerID string
		if order.Side == "buy" {
			buyerID = order.Owner
			sellerID = matchOrder.Owner
		} else {
			buyerID = matchOrder.Owner
			sellerID = order.Owner
		}

		tradeID := fmt.Sprintf("trd-%s-%d-%d", matchOrder.ID, now, randInt())
		newTrade := &Trade{
			ID:            tradeID,
			BuyerID:       buyerID,
			SellerID:      sellerID,
			Price:         tradePrice,
			Quantity:      tradeQty,
			Timestamp:     now,
			DeliveryStart: order.DeliveryStart,
			DeliveryEnd:   order.DeliveryEnd,
			Version:       2,
		}
		trades = append(trades, newTrade)

		// Broadcast the trade to WebSocket clients
		broadcastTrade(newTrade)

		// Apply balance changes: buyer pays, seller receives
		amount := tradePrice * tradeQty
		balances[buyerID] -= amount
		balances[sellerID] += amount

		// Update Quantities
		order.Quantity -= tradeQty
		filledQty += tradeQty
		matchOrder.Quantity -= tradeQty

		if matchOrder.Quantity <= 0 {
			matchOrder.Status = "FILLED"
		}
	}

	switch {
	case order.Quantity <= 0:
		order.Status = "FILLED"
	case execType == ExecutionTypeGTC:
		order.Status = "ACTIVE"
	default:
		order.Status = "CANCELLED"
	}

	return filledQty
}

func randInt() int {
	b := make([]byte, 4)
	rand.Read(b)
	return int(binary.BigEndian.Uint32(b))
}

func broadcastTrade(t *Trade) {
	if t.Version != 2 {
		return
	}
	tradeMsg := map[string]GValue{
		"trade_id":       t.ID,
		"buyer_id":       t.BuyerID,
		"seller_id":      t.SellerID,
		"price":          t.Price,
		"quantity":       t.Quantity,
		"delivery_start": t.DeliveryStart,
		"delivery_end":   t.DeliveryEnd,
		"timestamp":      t.Timestamp,
	}
	encoded, err := EncodeMessage(tradeMsg)
	if err != nil {
		log.Printf("failed to encode trade for broadcast: %v", err)
		return
	}
	hub.broadcast <- encoded
}

// V2 Orders Handler (Matching Engine + Order Book)
func ordersV2Handler(w http.ResponseWriter, r *http.Request) {

	// GET /v2/orders (Public Order Book)
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		startStr := q.Get("delivery_start")
		endStr := q.Get("delivery_end")

		if startStr == "" || endStr == "" {
			http.Error(w, "Missing query params", http.StatusBadRequest)
			return
		}

		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "Invalid timestamps", http.StatusBadRequest)
			return
		}
		const hourMs = 3600000
		if start%hourMs != 0 || end%hourMs != 0 || end <= start || (end-start) != hourMs {
			http.Error(w, "Invalid Contract Timestamps", http.StatusBadRequest)
			return
		}
		if contractTradingWindowStatus(start, nowFunc()) != 0 {
			resp := map[string]GValue{"bids": []map[string]GValue{}, "asks": []map[string]GValue{}}
			encoded, _ := EncodeMessage(resp)
			w.Header().Set("Content-Type", "application/x-galacticbuf")
			w.Write(encoded)
			return
		}

		mu.RLock()
		var bids []*Order
		var asks []*Order

		for _, o := range orders {
			// Allow both "ACTIVE" and "OPEN" to be visible for backward compatibility
			isVisible := (o.Status == "ACTIVE" || o.Status == "OPEN")
			if o.Version == 2 && isVisible && o.DeliveryStart == start && o.DeliveryEnd == end {
				if o.Side == "buy" {
					bids = append(bids, o)
				} else if o.Side == "sell" {
					asks = append(asks, o)
				}
			}
		}
		mu.RUnlock()

		// Sort Bids: Highest Price first, then Oldest
		sort.Slice(bids, func(i, j int) bool {
			if bids[i].Price != bids[j].Price {
				return bids[i].Price > bids[j].Price
			}
			return bids[i].Timestamp < bids[j].Timestamp
		})

		// Sort Asks: Lowest Price first, then Oldest
		sort.Slice(asks, func(i, j int) bool {
			if asks[i].Price != asks[j].Price {
				return asks[i].Price < asks[j].Price
			}
			return asks[i].Timestamp < asks[j].Timestamp
		})

		bidsList := make([]map[string]GValue, 0, len(bids))
		for _, o := range bids {
			bidsList = append(bidsList, map[string]GValue{
				"order_id": o.ID,
				"price":    o.Price,
				"quantity": o.Quantity,
			})
		}

		asksList := make([]map[string]GValue, 0, len(asks))
		for _, o := range asks {
			asksList = append(asksList, map[string]GValue{
				"order_id": o.ID,
				"price":    o.Price,
				"quantity": o.Quantity,
			})
		}

		resp := map[string]GValue{
			"bids": bidsList,
			"asks": asksList,
		}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
		return
	}

	// POST /v2/orders (Submit Order with Automatic Matching)
	if r.Method == http.MethodPost {
		username, authOk := getUserFromToken(r)
		if !authOk {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		data, err := DecodeMessage(r.Body)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		side, ok0 := data["side"].(string)
		price, ok1 := data["price"].(int64)
		quantity, ok2 := data["quantity"].(int64)
		start, ok3 := data["delivery_start"].(int64)
		end, ok4 := data["delivery_end"].(int64)
		execTypeRaw, _ := data["execution_type"].(string)

		if !ok0 || !ok1 || !ok2 || !ok3 || !ok4 {
			http.Error(w, "Missing fields", http.StatusBadRequest)
			return
		}
		execType, err := parseExecutionTypeInput(execTypeRaw)
		if err != nil {
			http.Error(w, "Invalid execution_type", http.StatusBadRequest)
			return
		}

		payload := orderSubmitPayload{
			Side:          side,
			Price:         price,
			Quantity:      quantity,
			DeliveryStart: start,
			DeliveryEnd:   end,
			ExecutionType: execType,
		}

		result, apiErr := createOrderV2(username, payload)
		if apiErr != nil {
			http.Error(w, apiErr.message, apiErr.status)
			return
		}

		resp := map[string]GValue{
			"order_id":        result.OrderID,
			"status":          result.Status,
			"filled_quantity": result.FilledQuantity,
		}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
		return
	}

	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func orderOperationHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v2/orders/")
	if id == "" || id == "/" {
		ordersV2Handler(w, r)
		return
	}

	switch r.Method {
	case http.MethodPut:
		modifyOrderHandler(w, r, id)
	case http.MethodDelete:
		cancelOrderHandler(w, r, id)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func modifyOrderHandler(w http.ResponseWriter, r *http.Request, orderID string) {
	username, authOk := getUserFromToken(r)
	if !authOk {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	price, ok1 := data["price"].(int64)
	quantity, ok2 := data["quantity"].(int64)
	if !ok1 || !ok2 {
		http.Error(w, "Missing fields", http.StatusBadRequest)
		return
	}

	result, apiErr := modifyOrderInternal(username, orderModifyPayload{OrderID: orderID, Price: price, Quantity: quantity})
	if apiErr != nil {
		http.Error(w, apiErr.message, apiErr.status)
		return
	}

	resp := map[string]GValue{
		"order_id":        result.OrderID,
		"status":          result.Status,
		"filled_quantity": result.FilledQuantity,
	}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

func cancelOrderHandler(w http.ResponseWriter, r *http.Request, orderID string) {
	username, authOk := getUserFromToken(r)
	if !authOk {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if err := cancelOrderInternal(username, orderCancelPayload{OrderID: orderID}); err != nil {
		http.Error(w, err.message, err.status)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func createOrderV2(username string, payload orderSubmitPayload) (*orderSubmitResult, *apiError) {
	if payload.Side != "buy" && payload.Side != "sell" {
		return nil, newAPIError(http.StatusBadRequest, "Invalid side")
	}
	if payload.Quantity <= 0 {
		return nil, newAPIError(http.StatusBadRequest, "Quantity must be positive")
	}
	const hourMs = 3600000
	if payload.DeliveryStart%hourMs != 0 || payload.DeliveryEnd%hourMs != 0 || payload.DeliveryEnd <= payload.DeliveryStart || (payload.DeliveryEnd-payload.DeliveryStart) != hourMs {
		return nil, newAPIError(http.StatusBadRequest, "Invalid Contract Timestamps")
	}
	status := contractTradingWindowStatus(payload.DeliveryStart, nowFunc())
	if status < 0 {
		return nil, newAPIError(http.StatusTooEarly, "Contract not open")
	}
	if status > 0 {
		return nil, newAPIError(http.StatusUnavailableForLegalReasons, "Contract closed")
	}

	mu.Lock()
	defer mu.Unlock()

	orderCounter++
	orderID := fmt.Sprintf("ord-v2-%d", orderCounter)
	now := nowMillis()

	newOrder := &Order{
		ID:            orderID,
		Price:         payload.Price,
		Quantity:      payload.Quantity,
		DeliveryStart: payload.DeliveryStart,
		DeliveryEnd:   payload.DeliveryEnd,
		Owner:         username,
		Status:        "ACTIVE",
		Side:          payload.Side,
		Version:       2,
		Timestamp:     now,
		ExecutionType: payload.ExecutionType,
	}

	if conflict := detectSelfMatchLocked(newOrder); conflict != nil {
		return nil, newAPIError(http.StatusPreconditionFailed, "Self-match prevented")
	}

	if orderRequiresCollateral(newOrder) {
		coll := getUserCollateralLocked(username)
		if coll < 0 {
			coll = 0
		}
		potential := computePotentialBalanceLocked(username, newOrder)
		if potential < -coll {
			return nil, newAPIError(http.StatusPaymentRequired, "Insufficient collateral")
		}
	}

	filledQty := matchOrder(newOrder)

	if payload.ExecutionType == ExecutionTypeGTC && newOrder.Status == "ACTIVE" {
		orders[orderID] = newOrder
	}
	persistStateLocked()

	return &orderSubmitResult{OrderID: orderID, Status: newOrder.Status, FilledQuantity: filledQty}, nil
}

func modifyOrderInternal(username string, payload orderModifyPayload) (*orderModifyResult, *apiError) {
	if payload.Quantity <= 0 {
		return nil, newAPIError(http.StatusBadRequest, "Quantity must be positive")
	}

	mu.Lock()
	defer mu.Unlock()

	order, exists := orders[payload.OrderID]
	if !exists || order.Status == "CANCELLED" || order.Status == "FILLED" || order.Version != 2 {
		return nil, newAPIError(http.StatusNotFound, "Order not found")
	}
	if order.Owner != username {
		return nil, newAPIError(http.StatusForbidden, "Forbidden")
	}
	if payload.EnforceContract && (order.DeliveryStart != payload.ExpectedStart || order.DeliveryEnd != payload.ExpectedEnd) {
		return nil, newAPIError(http.StatusBadRequest, "Contract mismatch")
	}

	resetPriority := payload.Price != order.Price || payload.Quantity > order.Quantity
	oldPrice, oldQty, oldStatus := order.Price, order.Quantity, order.Status

	order.Price = payload.Price
	order.Quantity = payload.Quantity
	order.Status = "ACTIVE"

	if conflict := detectSelfMatchLocked(order); conflict != nil {
		order.Price = oldPrice
		order.Quantity = oldQty
		order.Status = oldStatus
		return nil, newAPIError(http.StatusPreconditionFailed, "Self-match prevented")
	}

	if orderRequiresCollateral(order) {
		coll := getUserCollateralLocked(username)
		if coll < 0 {
			coll = 0
		}
		potential := computePotentialBalanceLocked(username, nil)
		if potential < -coll {
			order.Price = oldPrice
			order.Quantity = oldQty
			order.Status = oldStatus
			return nil, newAPIError(http.StatusPaymentRequired, "Insufficient collateral")
		}
	}

	if resetPriority {
		now := time.Now().UnixMilli()
		if now <= order.Timestamp {
			now = order.Timestamp + 1
		}
		order.Timestamp = now
	}

	filledQty := matchOrder(order)
	persistStateLocked()

	return &orderModifyResult{OrderID: payload.OrderID, Status: order.Status, FilledQuantity: filledQty}, nil
}

func cancelOrderInternal(username string, payload orderCancelPayload) *apiError {
	mu.Lock()
	defer mu.Unlock()

	order, exists := orders[payload.OrderID]
	if !exists || order.Status == "CANCELLED" || order.Status == "FILLED" || order.Version != 2 {
		return newAPIError(http.StatusNotFound, "Order not found")
	}
	if order.Owner != username {
		return newAPIError(http.StatusForbidden, "Forbidden")
	}
	if payload.EnforceContract && (order.DeliveryStart != payload.ExpectedStart || order.DeliveryEnd != payload.ExpectedEnd) {
		return newAPIError(http.StatusBadRequest, "Contract mismatch")
	}

	order.Status = "CANCELLED"
	persistStateLocked()
	return nil
}

func tradesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		mu.RLock()
		resultTrades := make([]*Trade, len(trades))
		copy(resultTrades, trades)
		mu.RUnlock()

		sort.Slice(resultTrades, func(i, j int) bool {
			return resultTrades[i].Timestamp > resultTrades[j].Timestamp
		})

		list := make([]map[string]GValue, 0, len(resultTrades))
		for _, t := range resultTrades {
			list = append(list, map[string]GValue{
				"trade_id":  t.ID,
				"buyer_id":  t.BuyerID,
				"seller_id": t.SellerID,
				"price":     t.Price,
				"quantity":  t.Quantity,
				"timestamp": t.Timestamp,
			})
		}

		resp := map[string]GValue{"trades": list}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
		return
	}

	// Legacy V1 manual trade execution
	if r.Method == http.MethodPost {
		buyerUser, authOk := getUserFromToken(r)
		if !authOk {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		data, err := DecodeMessage(r.Body)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		orderID, ok := data["order_id"].(string)
		if !ok {
			http.Error(w, "Missing order_id", http.StatusBadRequest)
			return
		}

		mu.Lock()
		defer mu.Unlock()

		order, exists := orders[orderID]
		// V1 endpoint only takes V1 orders
		if !exists || order.Status != "OPEN" || order.Version != 1 {
			http.Error(w, "Order not found or inactive", http.StatusNotFound)
			return
		}

		order.Status = "FILLED"

		now := time.Now().UnixMilli()
		tradeID := fmt.Sprintf("trd-%s-%d", order.ID, now)

		newTrade := &Trade{
			ID:            tradeID,
			BuyerID:       buyerUser,
			SellerID:      order.Owner,
			Price:         order.Price,
			Quantity:      order.Quantity,
			Timestamp:     now,
			DeliveryStart: order.DeliveryStart,
			DeliveryEnd:   order.DeliveryEnd,
			Version:       1,
		}
		trades = append(trades, newTrade)

		// Apply balance changes
		amount := order.Price * order.Quantity
		balances[buyerUser] -= amount
		balances[order.Owner] += amount
		persistStateLocked()

		resp := map[string]GValue{"trade_id": tradeID}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
	}
}

// GET /v2/trades - Public trade history for a contract (V2 only)
func tradesV2Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	startStr := q.Get("delivery_start")
	endStr := q.Get("delivery_end")
	if startStr == "" || endStr == "" {
		http.Error(w, "Missing query params", http.StatusBadRequest)
		return
	}

	start, err1 := strconv.ParseInt(startStr, 10, 64)
	end, err2 := strconv.ParseInt(endStr, 10, 64)
	if err1 != nil || err2 != nil {
		http.Error(w, "Invalid timestamps", http.StatusBadRequest)
		return
	}
	const hourMs = 3600000
	if start%hourMs != 0 || end%hourMs != 0 || end <= start || (end-start) != hourMs {
		http.Error(w, "Invalid Contract Timestamps", http.StatusBadRequest)
		return
	}

	mu.RLock()
	filtered := make([]*Trade, 0)
	for _, t := range trades {
		if t.Version != 2 {
			continue
		}
		if t.DeliveryStart == start && t.DeliveryEnd == end {
			filtered = append(filtered, t)
		}
	}
	mu.RUnlock()

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp > filtered[j].Timestamp
	})

	list := make([]map[string]GValue, 0, len(filtered))
	for _, t := range filtered {
		list = append(list, map[string]GValue{
			"trade_id":       t.ID,
			"buyer_id":       t.BuyerID,
			"seller_id":      t.SellerID,
			"price":          t.Price,
			"quantity":       t.Quantity,
			"delivery_start": t.DeliveryStart,
			"delivery_end":   t.DeliveryEnd,
			"timestamp":      t.Timestamp,
		})
	}

	resp := map[string]GValue{"trades": list}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

// GET /v2/my-trades - Returns auth user's trades for a specific contract
func myTradesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	username, authOk := getUserFromToken(r)
	if !authOk {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	q := r.URL.Query()
	startStr := q.Get("delivery_start")
	endStr := q.Get("delivery_end")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing query params", http.StatusBadRequest)
		return
	}

	start, err1 := strconv.ParseInt(startStr, 10, 64)
	end, err2 := strconv.ParseInt(endStr, 10, 64)
	if err1 != nil || err2 != nil {
		http.Error(w, "Invalid timestamps", http.StatusBadRequest)
		return
	}
	const hourMs = 3600000
	if start%hourMs != 0 || end%hourMs != 0 || end <= start || (end-start) != hourMs {
		http.Error(w, "Invalid Contract Timestamps", http.StatusBadRequest)
		return
	}

	mu.RLock()
	var resultTrades []map[string]GValue
	var myTrades []*Trade

	for _, t := range trades {
		// Filter by Contract
		if t.DeliveryStart != start || t.DeliveryEnd != end {
			continue
		}

		// Filter by User participation
		if t.BuyerID == username || t.SellerID == username {
			myTrades = append(myTrades, t)
		}
	}
	mu.RUnlock()

	// Sort by Timestamp Descending (Newest First)
	sort.Slice(myTrades, func(i, j int) bool {
		return myTrades[i].Timestamp > myTrades[j].Timestamp
	})

	// Construct Response
	for _, t := range myTrades {
		var side string
		var counterparty string

		if t.BuyerID == username {
			side = "buy"
			counterparty = t.SellerID
		} else {
			side = "sell"
			counterparty = t.BuyerID
		}

		resultTrades = append(resultTrades, map[string]GValue{
			"trade_id":       t.ID,
			"side":           side,
			"price":          t.Price,
			"quantity":       t.Quantity,
			"counterparty":   counterparty,
			"delivery_start": t.DeliveryStart,
			"delivery_end":   t.DeliveryEnd,
			"timestamp":      t.Timestamp,
		})
	}

	resp := map[string]GValue{"trades": resultTrades}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

// PUT /collateral/{username}  (admin-only: Bearer password123)
func collateralHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader != "Bearer password123" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := strings.TrimPrefix(r.URL.Path, "/collateral/")
	if username == "" {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	data, err := DecodeMessage(r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	coll, ok := data["collateral"].(int64)
	if !ok {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if coll < 0 {
		coll = 0
	}

	mu.Lock()
	defer mu.Unlock()

	if _, exists := users[username]; !exists {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	collaterals[username] = coll
	persistStateLocked()
	w.WriteHeader(http.StatusNoContent)
}

// GET /balance - user balance, potential balance, collateral
func balanceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	username, ok := getUserFromToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	mu.RLock()
	_, exists := users[username]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	bal := balances[username]
	coll := getUserCollateralLocked(username)
	potential := computePotentialBalanceLocked(username, nil)
	mu.RUnlock()

	resp := map[string]GValue{
		"balance":           bal,
		"potential_balance": potential,
		"collateral":        coll,
	}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.String())
		next(w, r)
	}
}

func myOrdersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	username, authOk := getUserFromToken(r)
	if !authOk {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	mu.RLock()
	var myOrders []*Order
	for _, o := range orders {
		if o.Owner == username && (o.Status == "ACTIVE" || o.Status == "OPEN") {
			myOrders = append(myOrders, o)
		}
	}
	mu.RUnlock()

	sort.Slice(myOrders, func(i, j int) bool {
		return myOrders[i].Timestamp > myOrders[j].Timestamp
	})

	list := make([]map[string]GValue, 0, len(myOrders))
	for _, o := range myOrders {
		list = append(list, map[string]GValue{
			"order_id":       o.ID,
			"side":           o.Side,
			"price":          o.Price,
			"quantity":       o.Quantity,
			"delivery_start": o.DeliveryStart,
			"delivery_end":   o.DeliveryEnd,
			"timestamp":      o.Timestamp,
		})
	}

	resp := map[string]GValue{"orders": list}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
}

// --- API Error Handling ---

type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string {
	return e.message
}

func newAPIError(status int, msg string) *apiError {
	return &apiError{status: status, message: msg}
}

// orderSubmitPayload mirrors POST /v2/orders inputs.
type orderSubmitPayload struct {
	Side          string
	Price         int64
	Quantity      int64
	DeliveryStart int64
	DeliveryEnd   int64
	ExecutionType string
}

type orderSubmitResult struct {
	OrderID        string
	Status         string
	FilledQuantity int64
}

type orderModifyPayload struct {
	OrderID         string
	Price           int64
	Quantity        int64
	ExpectedStart   int64
	ExpectedEnd     int64
	EnforceContract bool
}

type orderModifyResult struct {
	OrderID        string
	Status         string
	FilledQuantity int64
}

type orderCancelPayload struct {
	OrderID         string
	ExpectedStart   int64
	ExpectedEnd     int64
	EnforceContract bool
}

func main() {
	go hub.run()
	setupPersistenceFromEnv()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/register", registerHandler)
	mux.HandleFunc("/login", loginHandler)
	mux.HandleFunc("/user/password", passwordHandler)
	mux.HandleFunc("/dna-submit", dnaSubmitHandler)
	mux.HandleFunc("/dna-login", dnaLoginHandler)

	mux.HandleFunc("/orders", ordersV1Handler)
	mux.HandleFunc("/v2/orders", ordersV2Handler)
	mux.HandleFunc("/v2/orders/", orderOperationHandler)
	mux.HandleFunc("/v2/my-orders", myOrdersHandler)
	mux.HandleFunc("/v2/stream/trades", tradeStreamHandler)

	mux.HandleFunc("/trades", tradesHandler)
	mux.HandleFunc("/v2/trades", tradesV2Handler)
	mux.HandleFunc("/v2/my-trades", myTradesHandler)

	mux.HandleFunc("/collateral/", collateralHandler)
	mux.HandleFunc("/balance", balanceHandler)

	log.Println("Galactic Exchange started on :8080")
	if err := http.ListenAndServe(":8080", loggingMiddleware(mux.ServeHTTP)); err != nil {
		log.Fatal(err)
	}
}
