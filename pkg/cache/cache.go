package pkg

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)



type CacheItem struct {
	Value      string
	Expiration int64
}

type Cache struct {
	items map[string]CacheItem
	mu    sync.RWMutex
	file  string
}

func NewCache(file string) *Cache {
	cache := &Cache{
		items: make(map[string]CacheItem),
		file:  file,
	}
	if err := cache.loadFromFile(); err != nil {
		fmt.Printf("Error loading cache from file: %v\n", err)
	}
	return cache
}

func (c *Cache) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = CacheItem{
		Value:      value,
		Expiration: time.Now().Add(expiration).Unix(),
	}

	return c.saveToFile()
}

func (c *Cache) Get(ctx context.Context, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.items[key]
	if !found || time.Now().Unix() > item.Expiration {
		if found {
			delete(c.items, key)
		}
		return "", false, nil
	}

	return item.Value, true, nil
}

func (c *Cache) saveToFile() error {
	file, err := os.Create(c.file)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for key, item := range c.items {
		record := []string{key, item.Value, strconv.FormatInt(item.Expiration, 10)}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()

	return writer.Error()
}

func (c *Cache) loadFromFile() error {
	file, err := os.Open(c.file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, record := range records {
		if len(record) != 3 {
			return fmt.Errorf("некорректная длина записи")
		}
		expiration, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			return fmt.Errorf("некорректное время истечения срока действия для ключа %s", record[0])
		}
		c.items[record[0]] = CacheItem{
			Value:      record[1],
			Expiration: expiration,
		}
	}

	return nil
}
