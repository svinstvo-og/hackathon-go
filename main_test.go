package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func resetState() {
	mu.Lock()
	defer mu.Unlock()
	users = make(map[string]User)
	tokens = make(map[string]string)
	dnaSamples = make(map[string][]string)
	orders = make(map[string]*Order)
	trades = make([]*Trade, 0)
	orderCounter = 0
}

func addOrder(o *Order) {
	mu.Lock()
	defer mu.Unlock()
	orders[o.ID] = o
}

func TestOrdersV2Get_OrderBookSortingAndFilters(t *testing.T) {
	resetState()

	const start = int64(3600000 * 100) // aligned hour boundary
	const end = start + 3600000

	// Valid V2 orders for the contract
	addOrder(&Order{ID: "bid-old-high", Version: 2, Status: "OPEN", Side: "buy", Price: 200, Quantity: 10, DeliveryStart: start, DeliveryEnd: end, Timestamp: 1})
	addOrder(&Order{ID: "bid-new-high", Version: 2, Status: "OPEN", Side: "buy", Price: 200, Quantity: 20, DeliveryStart: start, DeliveryEnd: end, Timestamp: 2})
	addOrder(&Order{ID: "bid-lower", Version: 2, Status: "OPEN", Side: "buy", Price: 150, Quantity: 30, DeliveryStart: start, DeliveryEnd: end, Timestamp: 3})

	addOrder(&Order{ID: "ask-old-low", Version: 2, Status: "OPEN", Side: "sell", Price: 40, Quantity: 5, DeliveryStart: start, DeliveryEnd: end, Timestamp: 1})
	addOrder(&Order{ID: "ask-new-low", Version: 2, Status: "OPEN", Side: "sell", Price: 40, Quantity: 6, DeliveryStart: start, DeliveryEnd: end, Timestamp: 2})
	addOrder(&Order{ID: "ask-higher", Version: 2, Status: "OPEN", Side: "sell", Price: 60, Quantity: 7, DeliveryStart: start, DeliveryEnd: end, Timestamp: 3})

	// Should be filtered out: wrong version, wrong status, wrong contract
	addOrder(&Order{ID: "v1-order", Version: 1, Status: "OPEN", Side: "buy", Price: 999, Quantity: 1, DeliveryStart: start, DeliveryEnd: end, Timestamp: 10})
	addOrder(&Order{ID: "closed-v2", Version: 2, Status: "FILLED", Side: "sell", Price: 5, Quantity: 5, DeliveryStart: start, DeliveryEnd: end, Timestamp: 11})
	addOrder(&Order{ID: "wrong-contract", Version: 2, Status: "OPEN", Side: "buy", Price: 5, Quantity: 5, DeliveryStart: start + 7200000, DeliveryEnd: end + 7200000, Timestamp: 12})

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

	resp, err := DecodeMessage(rr.Body)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

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
