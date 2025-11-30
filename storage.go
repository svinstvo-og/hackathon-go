package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
)

type persistentState struct {
	Users        map[string]User     `json:"users"`
	Balances     map[string]int64    `json:"balances"`
	Collaterals  map[string]int64    `json:"collaterals"`
	DNASamples   map[string][]string `json:"dna_samples"`
	Orders       map[string]*Order   `json:"orders"`
	Trades       []*Trade            `json:"trades"`
	OrderCounter int64               `json:"order_counter"`
}

var persistencePath string

func setupPersistenceFromEnv() {
	dir, ok := os.LookupEnv("PERSISTENT_DIR")
	if !ok || dir == "" {
		return
	}
	if err := configurePersistenceDir(dir); err != nil {
		log.Printf("persistence disabled: %v", err)
		return
	}
	if err := loadPersistentState(); err != nil {
		log.Printf("failed to load persisted state: %v", err)
	}
}

func configurePersistenceDir(dir string) error {
	if dir == "" {
		persistencePath = ""
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	persistencePath = filepath.Join(dir, "state.json")
	return nil
}

func loadPersistentState() error {
	if persistencePath == "" {
		return nil
	}
	data, err := os.ReadFile(persistencePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var snapshot persistentState
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	mu.Lock()
	defer mu.Unlock()

	if snapshot.Users != nil {
		users = snapshot.Users
	} else {
		users = make(map[string]User)
	}
	if snapshot.Balances != nil {
		balances = snapshot.Balances
	} else {
		balances = make(map[string]int64)
	}
	if snapshot.Collaterals != nil {
		collaterals = snapshot.Collaterals
	} else {
		collaterals = make(map[string]int64)
	}
	if snapshot.DNASamples != nil {
		dnaSamples = snapshot.DNASamples
	} else {
		dnaSamples = make(map[string][]string)
	}

	orders = make(map[string]*Order)
	for id, o := range snapshot.Orders {
		if o != nil && o.Version == 2 && (o.Status == "ACTIVE" || o.Status == "OPEN") {
			orders[id] = o
		}
	}

	if snapshot.Trades != nil {
		trades = snapshot.Trades
	} else {
		trades = make([]*Trade, 0)
	}

	orderCounter = snapshot.OrderCounter
	if orderCounter < 0 {
		orderCounter = 0
	}

	return nil
}

func persistStateLocked() {
	if persistencePath == "" {
		return
	}

	snapshot := persistentState{
		Users:        users,
		Balances:     balances,
		Collaterals:  collaterals,
		DNASamples:   dnaSamples,
		Orders:       make(map[string]*Order),
		Trades:       trades,
		OrderCounter: orderCounter,
	}

	for id, o := range orders {
		if o.Version == 2 && (o.Status == "ACTIVE" || o.Status == "OPEN") {
			snapshot.Orders[id] = o
		}
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		log.Printf("persist marshal failed: %v", err)
		return
	}

	tmp := persistencePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("persist write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, persistencePath); err != nil {
		log.Printf("persist rename failed: %v", err)
	}
}
