package cache

import (
	"context"
	"encoding/json"
	"time"

	pkg "own-database-cache/pkg/cache"
)

type Client struct {
	cache *pkg.Cache
}

func NewClient(file string) *Client {
	return &Client{
		cache: pkg.NewCache(file),
	}
}

func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	serializedValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.cache.Set(ctx, key, string(serializedValue), expiration)
}

func (c *Client) Get(ctx context.Context, key string) (any, error) {
	serializedValue, found, err := c.cache.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	var value any
	err = json.Unmarshal([]byte(serializedValue), &value)
	if err != nil {
		return nil, err
	}

	return value, nil
}
