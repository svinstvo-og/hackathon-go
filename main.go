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
	"sync"
	"time"
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
	Status        string // "OPEN", "FILLED"
	Side          string // "buy" or "sell"
	Version       int    // 1 or 2
}

type Trade struct {
	ID        string
	BuyerID   string
	SellerID  string
	Price     int64
	Quantity  int64
	Timestamp int64
}

var (
	mu           sync.RWMutex
	users        = make(map[string]User)
	tokens       = make(map[string]string)
	orders       = make(map[string]*Order)
	trades       = make([]*Trade, 0)
	orderCounter int64 = 0
)

// --- GalacticBuf Protocol Constants ---

const (
	// Versions
	ProtoV1 = 0x01
	ProtoV2 = 0x02

	// Types
	TypeInt    = 0x01
	TypeString = 0x02
	TypeList   = 0x03
	TypeObject = 0x04
	TypeBytes  = 0x05 // New in V2
)

type GValue interface{}

// --- Encoder (Always V2) ---

// EncodeMessage теперь всегда создает сообщение версии V2
func EncodeMessage(data map[string]GValue) ([]byte, error) {
	bodyBuffer := new(bytes.Buffer)
	if err := writeFieldsV2(bodyBuffer, data); err != nil {
		return nil, err
	}
	bodyBytes := bodyBuffer.Bytes()

	header := new(bytes.Buffer)
	// Byte 0: Protocol Version (0x02)
	header.WriteByte(ProtoV2)
	// Byte 1: Field Count
	header.WriteByte(byte(len(data)))
	// Bytes 2-5: Total Message Length (4 bytes, BigEndian)
	totalLen := 6 + len(bodyBytes) // Header in V2 is 6 bytes
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
			// V2 String Length is 4 bytes (uint32)
			binary.Write(buf, binary.BigEndian, uint32(len(v)))
			buf.WriteString(v)
		case []byte:
			// New Type: Bytes (0x05)
			buf.WriteByte(TypeBytes)
			binary.Write(buf, binary.BigEndian, uint32(len(v)))
			buf.Write(v)
		case []map[string]GValue: // List of Objects
			buf.WriteByte(TypeList)
			buf.WriteByte(TypeObject)
			// V2 List Count is 4 bytes (uint32)
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
	// Peek version byte
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

// --- V1 Decoding Logic (Legacy) ---
func decodeV1(r io.Reader) (map[string]GValue, error) {
	// Read remaining 3 bytes of V1 Header (Count [1], Len [2])
	headerRem := make([]byte, 3)
	if _, err := io.ReadFull(r, headerRem); err != nil {
		return nil, err
	}
	fieldCount := int(headerRem[0])
	// Total length ignored in stream parsing
	return readFieldsV1(r, fieldCount)
}

func readFieldsV1(r io.Reader, count int) (map[string]GValue, error) {
	result := make(map[string]GValue)
	for i := 0; i < count; i++ {
		// Name
		var nameLen uint8
		if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}
		fieldName := string(nameBytes)

		// Type
		var typeInd uint8
		if err := binary.Read(r, binary.BigEndian, &typeInd); err != nil {
			return nil, err
		}

		// Value (V1 Rules)
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
		var l uint16 // V1 uses uint16
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
		var count uint16 // V1 uses uint16
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

// --- V2 Decoding Logic (New) ---
func decodeV2(r io.Reader) (map[string]GValue, error) {
	// Read remaining 5 bytes of V2 Header (Count [1], Len [4])
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
		// Name (Same as V1)
		var nameLen uint8
		if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}
		fieldName := string(nameBytes)

		// Type
		var typeInd uint8
		if err := binary.Read(r, binary.BigEndian, &typeInd); err != nil {
			return nil, err
		}

		// Value (V2 Rules)
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
		var l uint32 // V2 uses uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		// Защита от OOM на хакатоне, если придет битая длина
		if l > 100*1024*1024 { // 100MB limit sanity check
			return nil, fmt.Errorf("string too large for sanity check")
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf), nil
	case TypeBytes: // New in V2
		var l uint32
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		if l > 100*1024*1024 {
			return nil, fmt.Errorf("bytes too large for sanity check")
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil // Return []byte
	case TypeList:
		var elemType uint8
		binary.Read(r, binary.BigEndian, &elemType)
		var count uint32 // V2 uses uint32
		binary.Read(r, binary.BigEndian, &count)
		
		if count > 100000 { // Sanity check
			return nil, fmt.Errorf("list too large for sanity check")
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

// --- HTTP Handlers (Unchanged logic, utilizing updated Decode/Encode) ---

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

	w.WriteHeader(http.StatusNoContent)
}

// V1 Orders
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
			Status:        "OPEN",
			Side:          "sell",
			Version:       1,
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

// V2 Orders
func ordersV2Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

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

	if !ok0 || !ok1 || !ok2 || !ok3 || !ok4 {
		http.Error(w, "Missing fields", http.StatusBadRequest)
		return
	}

	if side != "buy" && side != "sell" {
		http.Error(w, "Invalid side", http.StatusBadRequest)
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
	orderID := fmt.Sprintf("ord-v2-%d", orderCounter)
	newOrder := &Order{
		ID:            orderID,
		Price:         price,
		Quantity:      quantity,
		DeliveryStart: start,
		DeliveryEnd:   end,
		Owner:         username,
		Status:        "OPEN",
		Side:          side,
		Version:       2,
	}
	orders[orderID] = newOrder
	mu.Unlock()

	resp := map[string]GValue{"order_id": orderID}
	encoded, _ := EncodeMessage(resp)
	w.Header().Set("Content-Type", "application/x-galacticbuf")
	w.Write(encoded)
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
		if !exists || order.Status != "OPEN" || order.Version != 1 {
			http.Error(w, "Order not found or inactive", http.StatusNotFound)
			return
		}

		order.Status = "FILLED"
		
		now := time.Now().UnixMilli()
		tradeID := fmt.Sprintf("trd-%s-%d", order.ID, now)
		
		newTrade := &Trade{
			ID:        tradeID,
			BuyerID:   buyerUser,
			SellerID:  order.Owner,
			Price:     order.Price,
			Quantity:  order.Quantity,
			Timestamp: now,
		}
		trades = append(trades, newTrade)

		resp := map[string]GValue{"trade_id": tradeID}
		encoded, _ := EncodeMessage(resp)
		w.Header().Set("Content-Type", "application/x-galacticbuf")
		w.Write(encoded)
	}
}

func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.String())
		next(w, r)
	}
}

func main() {
	mux := http.NewServeMux()
	
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/register", registerHandler)
	mux.HandleFunc("/login", loginHandler)
	mux.HandleFunc("/user/password", passwordHandler)
	mux.HandleFunc("/orders", ordersV1Handler)
	mux.HandleFunc("/v2/orders", ordersV2Handler)
	mux.HandleFunc("/trades", tradesHandler)

	log.Println("Galactic Exchange started on :8080")
	if err := http.ListenAndServe(":8080", loggingMiddleware(mux.ServeHTTP)); err != nil {
		log.Fatal(err)
	}
}