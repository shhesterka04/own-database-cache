package app

import (
	"context"
	"errors"
	"fmt"
	"own-database-cache/internal/config"
	"own-database-cache/internal/datasource/cache"
	"own-database-cache/internal/datasource/database"
	"time"
)

func Process(ctx context.Context, cacheClient *cache.Client, databaseClient *database.Client) error {
	config, err := config.LoadConfig("config.json")
	if err != nil {
		return fmt.Errorf("Error reading config: %v", err)
	}

	expirationTimeCache := time.Duration(config.ExpirationTimeCache) * time.Second

	bestUser := "best user, expired after 5 seconds"
	key := "user:12345:profile"

	if err := databaseClient.Set(ctx, key, bestUser, expirationTimeCache); err != nil {
		return fmt.Errorf("database Set error: %w", err)
	}

	if err := cacheClient.Set(ctx, key, bestUser, 5*time.Second); err != nil {
		return fmt.Errorf("cache Set error: %w", err)
	}

	got, err := cacheClient.Get(ctx, key)
	if err != nil {
		return err
	}
	if !checkValue(got, bestUser) {
		return errors.New("cached value does not match")
	}

	select {
	case <-time.After(7 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	got, err = cacheClient.Get(ctx, key)
	if err != nil {
		return err
	}
	if got != nil {
		return errors.New("unexpected cache hit: data should have expired")
	}

	gotAgain, err := databaseClient.Get(ctx, key)
	if err != nil {
		return err
	}
	if !checkValue(gotAgain, bestUser) {
		return errors.New("database value does not match")
	}

	fmt.Println("Success!")
	return nil
}

func checkValue(got any, want string) bool {
	gotStr, ok := got.(string)
	if !ok {
		return false
	}
	return gotStr == want
}
