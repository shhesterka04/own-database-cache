package database_test

import (
	"context"
	"own-database-cache/internal/config"
	db "own-database-cache/pkg/database"
	"log"
	"os"
	"reflect"
	"testing"
)

const configPath = "../../config.json"

func setup() (*db.Database, context.Context, func()) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	testdatabaseDir := config.PathConfig.TestDatabaseFilePath

	db := db.NewDatabase(testdatabaseDir)
	ctx := context.Background()

	if err := os.MkdirAll(testdatabaseDir, 0755); err != nil {
		panic("Failed to create database directory: " + err.Error())
	}

	return db, ctx, func() {
		os.RemoveAll(testdatabaseDir)
	}
}

func TestCreateTable(t *testing.T) {
	db, ctx, teardown := setup()
	defer teardown()

	t.Run("CreateTable_Success", func(t *testing.T) {
		config, err := config.LoadConfig(configPath)
		if err != nil {
			log.Fatalf("Error reading config: %v", err)
		}

		testdatabaseDir := config.PathConfig.TestCacheFilePath
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		query := "CREATE TABLE test (id, name, age) WITH TYPES (int64, string, int64)"
		if err := db.Exec(ctx, txn, query); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		if err := db.Commit(ctx, txn); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		if _, err := os.Stat(testdatabaseDir); os.IsNotExist(err) {
			t.Fatal("Table file was not created")
		}
	})

	t.Run("CreateTable_InvalidQuery", func(t *testing.T) {
		query := "CREATE TABLE"
		if err := db.Exec(ctx, nil, query); err == nil {
			t.Fatal("Expected error for invalid query, but got nil")
		}
	})

}

func TestInsert(t *testing.T) {
	db, ctx, teardown := setup()
	defer teardown()

	createTableQuery := "CREATE TABLE test (id, name)  WITH TYPES (int64, string)"

	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := db.Exec(ctx, txn, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Run("Insert_Success", func(t *testing.T) {
		insertQuery := "INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if err := db.Exec(ctx, txn, insertQuery); err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		if err := db.Commit(ctx, txn); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		selectQuery := "SELECT id, name FROM test"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Alice"},
			{"2", "Bob"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Insert_InvalidType", func(t *testing.T) {
		insertQuery := "INSERT INTO test (id, name) VALUES ('three', 3)"

		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if err := db.Exec(ctx, txn, insertQuery); err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		if err := db.Commit(ctx, txn); err == nil {
			t.Fatal("Expected error when committing transaction with invalid type, but got nil")
		}
	})

}

func TestSelect(t *testing.T) {
	db, ctx, teardown := setup()
	defer teardown()

	createTableQuery := "CREATE TABLE test (id, name)  WITH TYPES (int64, string)"
	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if err := db.Exec(ctx, txn, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	insertQuery := "INSERT INTO test (id, name) VALUES (2, 'Bob'), (1, 'Alice'), (3, 'Charlie')"
	txn, err = db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if err := db.Exec(ctx, txn, insertQuery); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Run("Select_All", func(t *testing.T) {
		selectQuery := "SELECT id, name FROM test"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"2", "Bob"},
			{"1", "Alice"},
			{"3", "Charlie"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Select_OrderByString", func(t *testing.T) {
		selectQuery := "SELECT id, name FROM test ORDER BY name"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Alice"},
			{"2", "Bob"},
			{"3", "Charlie"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Select_OrderByInt", func(t *testing.T) {
		selectQuery := "SELECT id, name FROM test ORDER BY id"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Alice"},
			{"2", "Bob"},
			{"3", "Charlie"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Select_Where", func(t *testing.T) {
		selectQuery := "SELECT id, name FROM test WHERE id > 1"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"2", "Bob"},
			{"3", "Charlie"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})
}

func TestDelete(t *testing.T) {
	db, ctx, teardown := setup()
	defer teardown()
	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	createTableQuery := "CREATE TABLE test (id, name) WITH TYPES (int64, string)"
	if err := db.Exec(ctx, txn, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	insertQuery := "INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	if err := db.Exec(ctx, txn, insertQuery); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Run("Delete_WithCondition", func(t *testing.T) {
		deleteQuery := "DELETE FROM test WHERE id > 1"
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if err := db.Exec(ctx, txn, deleteQuery); err != nil {
			t.Fatalf("Failed to delete data: %v", err)
		}

		if err := db.Commit(ctx, txn); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		selectQuery := "SELECT id, name FROM test"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Alice"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})
}

func TestUpdate(t *testing.T) {
	db, ctx, teardown := setup()
	defer teardown()

	createTableQuery := "CREATE TABLE test (id, name) WITH TYPES (int64, string)"
	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if err := db.Exec(ctx, txn, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	insertQuery := "INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	if err := db.Exec(ctx, txn, insertQuery); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	if err := db.Commit(ctx, txn); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Run("Update_WithCondition", func(t *testing.T) {
		updateQuery := "UPDATE test SET name = 'Updated' WHERE id > 1"
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if err := db.Exec(ctx, txn, updateQuery); err != nil {
			t.Fatalf("Failed to update data: %v", err)
		}

		if err := db.Commit(ctx, txn); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		selectQuery := "SELECT id, name FROM test"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Alice"},
			{"2", "Updated"},
			{"3", "Updated"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})

	t.Run("Update_All", func(t *testing.T) {
		updateQuery := "UPDATE test SET name = 'Updated'"
		txn, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if err := db.Exec(ctx, txn, updateQuery); err != nil {
			t.Fatalf("Failed to update data: %v", err)
		}

		if err := db.Commit(ctx, txn); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		selectQuery := "SELECT id, name FROM test"
		result, err := db.Query(ctx, selectQuery)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}

		expected := [][]string{
			{"id", "name"},
			{"1", "Updated"},
			{"2", "Updated"},
			{"3", "Updated"},
		}

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("Expected %v, got %v", expected, result)
		}
	})
}
