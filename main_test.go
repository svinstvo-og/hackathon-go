package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func resetState() {
	mu.Lock()
	defer mu.Unlock()
	tokens = make(map[string]string)
	users = make(map[string]User)
	dnaSamples = make(map[string][]string)
	orders = make(map[string]*Order)
	trades = make([]*Trade, 0)
	orderCounter = 0
	balances = make(map[string]int64)
	collaterals = make(map[string]int64)
	persistencePath = ""
	nowFunc = func() time.Time { return time.Now().UTC() }
}

func addOrder(o *Order) {
	mu.Lock()
	defer mu.Unlock()
	orders[o.ID] = o
}

func TestOrdersV2Get_OrderBookSortingAndFilters(t *testing.T) {
	resetState()

	nowFunc = func() time.Time { return time.UnixMilli(3600000 * 100).UTC().Add(-time.Minute) }

	const start = int64(3600000 * 100)
	const end = start + 3600000

	// Valid V2 orders for the contract
	addOrder(&Order{ID: "bid-old-high", Version: 2, Status: "ACTIVE", Side: "buy", Price: 200, Quantity: 10, DeliveryStart: start, DeliveryEnd: end, Timestamp: 1})
	addOrder(&Order{ID: "bid-new-high", Version: 2, Status: "ACTIVE", Side: "buy", Price: 200, Quantity: 20, DeliveryStart: start, DeliveryEnd: end, Timestamp: 2})
	addOrder(&Order{ID: "bid-lower", Version: 2, Status: "ACTIVE", Side: "buy", Price: 150, Quantity: 30, DeliveryStart: start, DeliveryEnd: end, Timestamp: 3})

	addOrder(&Order{ID: "ask-old-low", Version: 2, Status: "ACTIVE", Side: "sell", Price: 40, Quantity: 5, DeliveryStart: start, DeliveryEnd: end, Timestamp: 1})
	addOrder(&Order{ID: "ask-new-low", Version: 2, Status: "ACTIVE", Side: "sell", Price: 40, Quantity: 6, DeliveryStart: start, DeliveryEnd: end, Timestamp: 2})
	addOrder(&Order{ID: "ask-higher", Version: 2, Status: "ACTIVE", Side: "sell", Price: 60, Quantity: 7, DeliveryStart: start, DeliveryEnd: end, Timestamp: 3})

	// Should be filtered out: wrong version, wrong status, wrong contract
	addOrder(&Order{ID: "v1-order", Version: 1, Status: "OPEN", Side: "buy", Price: 999, Quantity: 1, DeliveryStart: start, DeliveryEnd: end, Timestamp: 10})
	addOrder(&Order{ID: "closed-v2", Version: 2, Status: "FILLED", Side: "sell", Price: 5, Quantity: 5, DeliveryStart: start, DeliveryEnd: end, Timestamp: 11})
	addOrder(&Order{ID: "wrong-contract", Version: 2, Status: "ACTIVE", Side: "buy", Price: 5, Quantity: 5, DeliveryStart: start + 7200000, DeliveryEnd: end + 7200000, Timestamp: 12})

	req := httptest.NewRequest(http.MethodGet, "/v2/orders", nil)
	q := url.Values{}
	q.Set("delivery_start", "360000000")
	q.Set("delivery_end", "363600000")
	req.URL.RawQuery = q.Encode()

	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	resp := decodeBody(t, rr.Body)

	rawBids, ok := resp["bids"].([]GValue)
	if !ok {
		t.Fatalf("bids missing or wrong type")
	}
	rawAsks, ok := resp["asks"].([]GValue)
	if !ok {
		t.Fatalf("asks missing or wrong type")
	}

	bids := make([]map[string]GValue, len(rawBids))
	for i, v := range rawBids {
		m, ok := v.(map[string]GValue)
		if !ok {
			t.Fatalf("bid element %d wrong type %T", i, v)
		}
		bids[i] = m
	}

	asks := make([]map[string]GValue, len(rawAsks))
	for i, v := range rawAsks {
		m, ok := v.(map[string]GValue)
		if !ok {
			t.Fatalf("ask element %d wrong type %T", i, v)
		}
		asks[i] = m
	}

	// Bids: price desc, then time asc
	expectedBids := []string{"bid-old-high", "bid-new-high", "bid-lower"}
	if len(bids) != len(expectedBids) {
		t.Fatalf("expected %d bids, got %d", len(expectedBids), len(bids))
	}
	for i, id := range expectedBids {
		if bids[i]["order_id"] != id {
			t.Fatalf("bids[%d] expected %s, got %v", i, id, bids[i]["order_id"])
		}
	}

	// Asks: price asc, then time asc
	expectedAsks := []string{"ask-old-low", "ask-new-low", "ask-higher"}
	if len(asks) != len(expectedAsks) {
		t.Fatalf("expected %d asks, got %d", len(expectedAsks), len(asks))
	}
	for i, id := range expectedAsks {
		if asks[i]["order_id"] != id {
			t.Fatalf("asks[%d] expected %s, got %v", i, id, asks[i]["order_id"])
		}
	}
}

func TestOrdersV2Get_InvalidContractAlignment(t *testing.T) {
	resetState()
	req := httptest.NewRequest(http.MethodGet, "/v2/orders?delivery_start=1&delivery_end=2", nil)
	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad alignment, got %d", rr.Code)
	}
}

func TestModifyOrder_PriorityAndMatching(t *testing.T) {
	resetState()
	const start = int64(3600000 * 100)
	const end = start + 3600000

	// 1. Setup: Sell order at 100
	mu.Lock()
	users["user1"] = User{Username: "user1", Password: "pw"}
	tokens["tok1"] = "user1"
	users["user2"] = User{Username: "user2", Password: "pw"}
	tokens["tok2"] = "user2"

	// Existing sell order
	orders["sell1"] = &Order{
		ID: "sell1", Version: 2, Status: "ACTIVE", Side: "sell", Price: 100, Quantity: 10,
		DeliveryStart: start, DeliveryEnd: end, Owner: "user2", Timestamp: 1000,
	}
	// Existing buy order (resting, too low price)
	orders["buy1"] = &Order{
		ID: "buy1", Version: 2, Status: "ACTIVE", Side: "buy", Price: 90, Quantity: 10,
		DeliveryStart: start, DeliveryEnd: end, Owner: "user1", Timestamp: 2000,
	}
	mu.Unlock()

	// 2. Modify buy1 price to 100 -> Should match sell1
	reqBody := map[string]GValue{"price": int64(100), "quantity": int64(10)}
	req := newRequest(t, http.MethodPut, "/v2/orders/buy1", reqBody)
	req.Header.Set("Authorization", "Bearer tok1")
	rr := httptest.NewRecorder()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/orders/", orderOperationHandler)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	mu.Lock()
	// Check if match happened
	if len(trades) != 1 {
		t.Errorf("expected 1 trade, got %d", len(trades))
	}
	if orders["buy1"].Status != "FILLED" {
		t.Errorf("expected buy1 to be FILLED, got %s", orders["buy1"].Status)
	}
	mu.Unlock()
}

func TestModifyOrder_PriorityReset(t *testing.T) {
	resetState()
	const start = int64(3600000 * 100)
	const end = start + 3600000

	mu.Lock()
	users["u"] = User{Username: "u", Password: "p"}
	tokens["tok"] = "u"
	// Order with old timestamp
	orders["o1"] = &Order{
		ID: "o1", Version: 2, Status: "ACTIVE", Side: "buy", Price: 100, Quantity: 10,
		DeliveryStart: start, DeliveryEnd: end, Owner: "u", Timestamp: 1000,
	}
	mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/orders/", orderOperationHandler)

	// Case 1: Decrease Quantity -> Priority Preserved
	req1 := newRequest(t, http.MethodPut, "/v2/orders/o1", map[string]GValue{"price": int64(100), "quantity": int64(5)})
	req1.Header.Set("Authorization", "Bearer tok")
	rr1 := httptest.NewRecorder()
	mux.ServeHTTP(rr1, req1)

	mu.Lock()
	if orders["o1"].Timestamp != 1000 {
		t.Errorf("timestamp should be preserved on qty decrease, got %d", orders["o1"].Timestamp)
	}
	mu.Unlock()

	// Case 2: Increase Quantity -> Priority Reset
	req2 := newRequest(t, http.MethodPut, "/v2/orders/o1", map[string]GValue{"price": int64(100), "quantity": int64(20)})
	req2.Header.Set("Authorization", "Bearer tok")
	rr2 := httptest.NewRecorder()
	mux.ServeHTTP(rr2, req2)

	mu.Lock()
	if orders["o1"].Timestamp <= 1000 {
		t.Errorf("timestamp should be reset on qty increase")
	}
	tsAfterIncrease := orders["o1"].Timestamp
	mu.Unlock()

	// Case 3: Change Price -> Priority Reset
	time.Sleep(1 * time.Millisecond) // Ensure time advances
	req3 := newRequest(t, http.MethodPut, "/v2/orders/o1", map[string]GValue{"price": int64(101), "quantity": int64(20)})
	req3.Header.Set("Authorization", "Bearer tok")
	rr3 := httptest.NewRecorder()
	mux.ServeHTTP(rr3, req3)

	mu.Lock()
	if orders["o1"].Timestamp <= tsAfterIncrease {
		t.Errorf("timestamp should be reset on price change")
	}
	mu.Unlock()
}

func TestCancelOrder(t *testing.T) {
	resetState()
	mu.Lock()
	users["u"] = User{Username: "u", Password: "p"}
	tokens["tok"] = "u"
	orders["o1"] = &Order{
		ID: "o1", Version: 2, Status: "ACTIVE", Side: "buy", Price: 100, Quantity: 10, Owner: "u",
	}
	mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/orders/", orderOperationHandler)

	// 1. Cancel Success
	req1 := httptest.NewRequest(http.MethodDelete, "/v2/orders/o1", nil)
	req1.Header.Set("Authorization", "Bearer tok")
	rr1 := httptest.NewRecorder()
	mux.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rr1.Code)
	}
	mu.Lock()
	if orders["o1"].Status != "CANCELLED" {
		t.Errorf("expected status CANCELLED, got %s", orders["o1"].Status)
	}
	mu.Unlock()

	// 2. Cancel Again -> 404
	req2 := httptest.NewRequest(http.MethodDelete, "/v2/orders/o1", nil)
	req2.Header.Set("Authorization", "Bearer tok")
	rr2 := httptest.NewRecorder()
	mux.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusNotFound {
		t.Errorf("expected 404 for already cancelled, got %d", rr2.Code)
	}
}

func TestTradesV2Handler_Success(t *testing.T) {
	resetState()
	const start = int64(3600000 * 50)
	const end = start + 3600000

	mu.Lock()
	trades = append(trades,
		&Trade{ID: "v2-new", BuyerID: "b", SellerID: "s", Price: 100, Quantity: 5, Timestamp: 2000, DeliveryStart: start, DeliveryEnd: end, Version: 2},
		&Trade{ID: "v2-old", BuyerID: "b2", SellerID: "s2", Price: 90, Quantity: 3, Timestamp: 1000, DeliveryStart: start, DeliveryEnd: end, Version: 2},
		&Trade{ID: "v2-other", BuyerID: "b", SellerID: "s", Price: 80, Quantity: 2, Timestamp: 3000, DeliveryStart: start + 3600000, DeliveryEnd: end + 3600000, Version: 2},
		&Trade{ID: "v1-trade", BuyerID: "b", SellerID: "s", Price: 70, Quantity: 1, Timestamp: 4000, DeliveryStart: start, DeliveryEnd: end, Version: 1},
	)
	mu.Unlock()

	req := newRequestWithContract(t, http.MethodGet, "/v2/trades", start, end, true)
	rr := httptest.NewRecorder()
	tradesV2Handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	resp := decodeBody(t, rr.Body)

	rawTrades, ok := resp["trades"].([]GValue)
	if !ok {
		t.Fatalf("trades missing or wrong type")
	}
	if len(rawTrades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(rawTrades))
	}

	first, ok := rawTrades[0].(map[string]GValue)
	if !ok {
		t.Fatalf("first trade wrong type %T", rawTrades[0])
	}
	second, ok := rawTrades[1].(map[string]GValue)
	if !ok {
		t.Fatalf("second trade wrong type %T", rawTrades[1])
	}

	if first["trade_id"] != "v2-new" || second["trade_id"] != "v2-old" {
		t.Fatalf("trades not sorted newest-first or wrong filtering: %+v, %+v", first["trade_id"], second["trade_id"])
	}
	if first["delivery_start"].(int64) != start || first["delivery_end"].(int64) != end {
		t.Fatalf("expected contract window on first trade")
	}
}

func TestTradesV2Handler_InvalidParams(t *testing.T) {
	resetState()
	req := httptest.NewRequest(http.MethodGet, "/v2/trades", nil)
	rr := httptest.NewRecorder()
	tradesV2Handler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing params, got %d", rr.Code)
	}

	reqBad := newRequestWithContract(t, http.MethodGet, "/v2/trades", 1, 3660000, false)
	rrBad := httptest.NewRecorder()
	tradesV2Handler(rrBad, reqBad)
	if rrBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unaligned params, got %d", rrBad.Code)
	}
}

func mustEncode(t *testing.T, body map[string]GValue) []byte {
	t.Helper()
	b, err := EncodeMessage(body)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	return b
}

func decodeBody(t *testing.T, body io.Reader) map[string]GValue {
	t.Helper()
	resp, err := DecodeMessage(body)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	return resp
}

func newRequest(t *testing.T, method, target string, body map[string]GValue) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(mustEncode(t, body))
	}
	return httptest.NewRequest(method, target, reader)
}

func newRequestWithContract(t *testing.T, method, path string, start, end int64, enforce bool) *http.Request {
	const hourMs = 3600000
	if enforce && (start%hourMs != 0 || end-start != hourMs) {
		t.Fatalf("contract not aligned: %d-%d", start, end)
	}
	t.Helper()
	return httptest.NewRequest(method, fmt.Sprintf("%s?delivery_start=%d&delivery_end=%d", path, start, end), nil)
}

func setupUserWithToken(username, token string) {
	mu.Lock()
	defer mu.Unlock()
	users[username] = User{Username: username, Password: "pw"}
	tokens[token] = username
}

func TestOrdersV2Post_IOCPartialCancel(t *testing.T) {
	resetState()
	const start = int64(3600000 * 200)
	const end = start + 3600000
	nowFunc = func() time.Time { return time.UnixMilli(start).Add(-time.Minute) }

	setupUserWithToken("maker", "makerTok")
	setupUserWithToken("taker", "takerTok")

	mu.Lock()
	orders["rest-sell"] = &Order{
		ID: "rest-sell", Version: 2, Status: "ACTIVE", Side: "sell", Price: 100, Quantity: 5,
		DeliveryStart: start, DeliveryEnd: end, Owner: "maker", Timestamp: nowMillis(), ExecutionType: ExecutionTypeGTC,
	}
	mu.Unlock()

	reqBody := map[string]GValue{
		"side":           "buy",
		"price":          int64(110),
		"quantity":       int64(10),
		"delivery_start": start,
		"delivery_end":   end,
		"execution_type": "IOC",
	}
	req := newRequest(t, http.MethodPost, "/v2/orders", reqBody)
	req.Header.Set("Authorization", "Bearer takerTok")
	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	resp := decodeBody(t, rr.Body)
	if resp["status"] != "CANCELLED" {
		t.Fatalf("expected IOC order to cancel, got %v", resp["status"])
	}
	filled, ok := resp["filled_quantity"].(int64)
	if !ok || filled != 5 {
		t.Fatalf("expected filled_quantity=5, got %v", resp["filled_quantity"])
	}
	orderID, _ := resp["order_id"].(string)

	mu.RLock()
	defer mu.RUnlock()
	if _, exists := orders[orderID]; exists {
		t.Fatalf("IOC order should not rest on the book")
	}
}

func TestOrdersV2Post_FOKAllOrNothing(t *testing.T) {
	resetState()
	const start = int64(3600000 * 300)
	const end = start + 3600000
	nowFunc = func() time.Time { return time.UnixMilli(start).Add(-time.Minute) }

	setupUserWithToken("maker", "makerTok")
	setupUserWithToken("taker", "takerTok")

	mu.Lock()
	orders["rest-buy"] = &Order{
		ID: "rest-buy", Version: 2, Status: "ACTIVE", Side: "buy", Price: 100, Quantity: 5,
		DeliveryStart: start, DeliveryEnd: end, Owner: "maker", Timestamp: nowMillis(), ExecutionType: ExecutionTypeGTC,
	}
	mu.Unlock()

	reqBody := map[string]GValue{
		"side":           "sell",
		"price":          int64(100),
		"quantity":       int64(10),
		"delivery_start": start,
		"delivery_end":   end,
		"execution_type": "FOK",
	}
	req := newRequest(t, http.MethodPost, "/v2/orders", reqBody)
	req.Header.Set("Authorization", "Bearer takerTok")
	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	resp := decodeBody(t, rr.Body)
	if resp["status"] != "CANCELLED" {
		t.Fatalf("expected FOK order to cancel, got %v", resp["status"])
	}
	if fq, _ := resp["filled_quantity"].(int64); fq != 0 {
		t.Fatalf("expected zero fill for unfilled FOK, got %d", fq)
	}
	orderID, _ := resp["order_id"].(string)

	mu.RLock()
	if _, exists := orders[orderID]; exists {
		mu.RUnlock()
		t.Fatalf("FOK order should not rest on the book")
	}
	mu.RUnlock()
}

func TestOrdersV2Post_FOKFullFill(t *testing.T) {
	resetState()
	const start = int64(3600000 * 400)
	const end = start + 3600000
	nowFunc = func() time.Time { return time.UnixMilli(start).Add(-time.Minute) }

	setupUserWithToken("maker1", "makerTok1")
	setupUserWithToken("maker2", "makerTok2")
	setupUserWithToken("taker", "takerTok")

	mu.Lock()
	orders["rest-buy-1"] = &Order{
		ID: "rest-buy-1", Version: 2, Status: "ACTIVE", Side: "buy", Price: 100, Quantity: 4,
		DeliveryStart: start, DeliveryEnd: end, Owner: "maker1", Timestamp: nowMillis(), ExecutionType: ExecutionTypeGTC,
	}
	orders["rest-buy-2"] = &Order{
		ID: "rest-buy-2", Version: 2, Status: "ACTIVE", Side: "buy", Price: 99, Quantity: 6,
		DeliveryStart: start, DeliveryEnd: end, Owner: "maker2", Timestamp: nowMillis() + 1, ExecutionType: ExecutionTypeGTC,
	}
	mu.Unlock()

	reqBody := map[string]GValue{
		"side":           "sell",
		"price":          int64(99),
		"quantity":       int64(10),
		"delivery_start": start,
		"delivery_end":   end,
		"execution_type": "FOK",
	}
	req := newRequest(t, http.MethodPost, "/v2/orders", reqBody)
	req.Header.Set("Authorization", "Bearer takerTok")
	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	resp := decodeBody(t, rr.Body)
	if resp["status"] != "FILLED" {
		t.Fatalf("expected FOK to fill completely, got %v", resp["status"])
	}
	if fq, _ := resp["filled_quantity"].(int64); fq != 10 {
		t.Fatalf("expected filled_quantity=10, got %d", fq)
	}
	orderID, _ := resp["order_id"].(string)

	mu.RLock()
	if _, exists := orders[orderID]; exists {
		mu.RUnlock()
		t.Fatalf("filled FOK order should not rest on the book")
	}
	mu.RUnlock()
}

func TestOrdersV2Post_InvalidExecutionType(t *testing.T) {
	resetState()
	const start = int64(3600000 * 500)
	const end = start + 3600000
	nowFunc = func() time.Time { return time.UnixMilli(start).Add(-time.Minute) }

	setupUserWithToken("trader", "tok")

	reqBody := map[string]GValue{
		"side":           "buy",
		"price":          int64(100),
		"quantity":       int64(1),
		"delivery_start": start,
		"delivery_end":   end,
		"execution_type": "DAY",
	}
	req := newRequest(t, http.MethodPost, "/v2/orders", reqBody)
	req.Header.Set("Authorization", "Bearer tok")
	rr := httptest.NewRecorder()
	ordersV2Handler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid execution_type, got %d", rr.Code)
	}
}
