package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistenceCycle(t *testing.T) {
	resetState()

	dir := t.TempDir()
	if err := configurePersistenceDir(dir); err != nil {
		t.Fatalf("configurePersistenceDir failed: %v", err)
	}

	mu.Lock()
	users["alice"] = User{Username: "alice", Password: "pw"}
	balances["alice"] = 42
	collaterals["alice"] = 100
	orders["ord-v2-1"] = &Order{ID: "ord-v2-1", Version: 2, Status: "ACTIVE", Side: "buy", Price: 10, Quantity: 5, Owner: "alice", DeliveryStart: 3600000, DeliveryEnd: 7200000, Timestamp: 1}
	trades = append(trades, &Trade{ID: "trd-1", BuyerID: "alice", SellerID: "bob", Price: 10, Quantity: 5, Timestamp: 2, DeliveryStart: 3600000, DeliveryEnd: 7200000, Version: 2})
	orderCounter = 99
	persistStateLocked()
	mu.Unlock()

	resetState()
	if err := configurePersistenceDir(dir); err != nil {
		t.Fatalf("configurePersistenceDir failed: %v", err)
	}
	if err := loadPersistentState(); err != nil {
		t.Fatalf("loadPersistentState failed: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()

	if len(users) != 1 || users["alice"].Password != "pw" {
		t.Fatalf("users not restored: %+v", users)
	}
	if balances["alice"] != 42 || collaterals["alice"] != 100 {
		t.Fatalf("financials not restored: %v %v", balances["alice"], collaterals["alice"])
	}
	if _, ok := orders["ord-v2-1"]; !ok {
		t.Fatalf("order not restored: %+v", orders)
	}
	if len(trades) != 1 || trades[0].ID != "trd-1" {
		t.Fatalf("trades not restored: %+v", trades)
	}
	if orderCounter != 99 {
		t.Fatalf("orderCounter not restored: %d", orderCounter)
	}
}

func TestPersistenceSkipsFilledOrders(t *testing.T) {
	resetState()
	dir := t.TempDir()
	if err := configurePersistenceDir(dir); err != nil {
		t.Fatalf("configurePersistenceDir failed: %v", err)
	}

	mu.Lock()
	orders["filled"] = &Order{ID: "filled", Version: 2, Status: "FILLED", Side: "buy", DeliveryStart: 3600000, DeliveryEnd: 7200000}
	persistStateLocked()
	mu.Unlock()

	resetState()
	if err := configurePersistenceDir(dir); err != nil {
		t.Fatalf("configurePersistenceDir failed: %v", err)
	}
	if err := loadPersistentState(); err != nil {
		t.Fatalf("loadPersistentState failed: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	if len(orders) != 0 {
		t.Fatalf("filled order should not persist: %+v", orders)
	}
}

func TestPersistenceDisabledWithoutEnv(t *testing.T) {
	resetState()
	dir := t.TempDir()

	mu.Lock()
	users["alice"] = User{Username: "alice", Password: "pw"}
	persistStateLocked() // should no-op because no path configured
	mu.Unlock()

	if _, err := os.Stat(filepath.Join(dir, "state.json")); err == nil {
		t.Fatalf("state file unexpectedly created")
	}
}
