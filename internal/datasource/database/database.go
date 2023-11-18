package database

import (
	"context"
	"fmt"
	"own-database-cache/internal/config"
	db "own-database-cache/pkg/database"
	"log"
	"os"
	"time"
)

type Client struct {
	db *db.Database
}

func NewClient(file string) *Client {
	return &Client{
		db: db.NewDatabase(file),
	}
}

const configPath = "config.json"

func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	databaseFile := config.PathConfig.DatabaseFilePath
	fileName := config.PathConfig.FileName

	if _, err := os.Stat(databaseFile + fileName); os.IsNotExist(err) {
		txn, err := c.db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		createTableQuery := "CREATE TABLE file (key, value) WITH TYPES (string, string)"
		if err := c.db.Exec(ctx, txn, createTableQuery); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		if err := c.db.Commit(ctx, txn); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

	} else if err != nil {
		return fmt.Errorf("error checking file existence: %w", err)
	}

	txn, err := c.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	value = fmt.Sprintf("\"%s\"", value)

	sql := fmt.Sprintf("INSERT INTO file (key, value) VALUES ('%s', '%v')", key, value)

	if err := c.db.Exec(ctx, txn, sql); err != nil {
		_ = c.db.Rollback(ctx, txn)
		return err
	}

	return c.db.Commit(ctx, txn)
}

func (c *Client) Get(ctx context.Context, key string) (any, error) {
	sql := fmt.Sprintf("SELECT key, value FROM file WHERE key = %s", key)

	row, err := c.db.QueryRow(ctx, sql)
	if err != nil {
		return nil, err
	}

	return row[1], nil
}
