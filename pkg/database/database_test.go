package database

import (
	"context"
	"own-database-cache/internal/config"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

const configPath = "../../config.json"

func TestTransaction(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath

	db := NewDatabase(testdatabaseDir)
	ctx := context.Background()

	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit first transaction: %v", err)
	}

	txn, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	if err := db.Rollback(ctx, txn); err != nil {
		t.Fatalf("Failed to rollback second transaction: %v", err)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath
	db := NewDatabase(testdatabaseDir)
	ctx := context.Background()

	var wg sync.WaitGroup

	startTransaction := func(name string) {
		defer wg.Done()
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Errorf("%s: Failed to begin transaction: %v", name, err)
			return
		}
		time.Sleep(1 * time.Second)
		if err := db.Commit(ctx, txn); err != nil {
			t.Errorf("%s: Failed to commit transaction: %v", name, err)
		}
	}

	wg.Add(3)
	go startTransaction("Transaction 1")
	go startTransaction("Transaction 2")
	go startTransaction("Transaction 3")

	wg.Wait()
}

func TestExec(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath

	db := NewDatabase(testdatabaseDir)
	ctx := context.Background()
	defer os.RemoveAll(testdatabaseDir)

	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if err := db.Exec(ctx, txn, "CREATE TABLE test (id, name) WITH TYPES (int64, string)"); err != nil {
		t.Fatalf("Failed to execute SQL query: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := db.Exec(ctx, txn, "INSERT INTO test (id, name) VALUES (1, 'Bob')"); err != nil {
		t.Fatalf("Failed to execute SQL query within transaction: %v", err)
	}
	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestQuery(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath

	db := NewDatabase(testdatabaseDir)
	ctx := context.Background()
	defer os.RemoveAll(testdatabaseDir)

	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	db.Exec(ctx, txn, "CREATE TABLE test (id, name) WITH TYPES (int64, string)")
	db.Exec(ctx, txn, "INSERT INTO test (id, name) VALUES (1, 'Bob'), (2, 'Alice')")

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	results, err := db.Query(ctx, "SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("Failed to execute SQL query: %v", err)
	}

	expected := [][]string{
		{"id", "name"},
		{"1", "Bob"},
		{"2", "Alice"},
	}

	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("Expected %v, got %v", expected, results)
	}
}

func TestQueryRow(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath

	db := NewDatabase(testdatabaseDir)
	ctx := context.Background()
	defer os.RemoveAll(testdatabaseDir)

	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	db.Exec(ctx, txn, "CREATE TABLE test (id, name) WITH TYPES (int64, string)")
	db.Exec(ctx, txn, "INSERT INTO test (id, name) VALUES (1, 'Bob'), (2, 'Alice')")

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	row, err := db.QueryRow(ctx, "SELECT id, name FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to execute SQL query: %v", err)
	}
	expectedRow := []string{"1", "Bob"}

	if !reflect.DeepEqual(row, expectedRow) {
		t.Fatalf("Expected %v, got %v", expectedRow, row)
	}
}
