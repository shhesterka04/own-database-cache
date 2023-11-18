package pkg

import (
	"context"
	"own-database-cache/internal/config"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const configPath = "../../config.json"

func TestCacheSetAndGet(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath
	file := cacheDir + "test_cache.csv"
	defer os.Remove(file)

	cache := NewCache(file)

	ctx := context.Background()
	key := "testKey"
	value := "testValue"
	expiration := 5 * time.Second

	err = cache.Set(ctx, key, value, expiration)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	gotValue, found, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !found {
		t.Fatalf("Get() = %v, want %v", found, true)
	}
	if gotValue != value {
		t.Fatalf("Get() = %v, want %v", gotValue, value)
	}
}

func TestCacheExpiration(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache.csv"
	defer os.Remove(file)

	cache := NewCache(file)

	ctx := context.Background()
	key := "testKey"
	value := "testValue"
	expiration := 1 * time.Second

	cache.Set(ctx, key, value, expiration)

	time.Sleep(2 * time.Second)

	_, found, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if found {
		t.Fatal("Expected value to be expired and not found")
	}

}

func TestCacheConcurrency(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache.csv"
	defer os.Remove(file)

	cache := NewCache(file)

	ctx := context.Background()
	key := "testKey"
	value := "testValue"
	expiration := 5 * time.Minute

	var wg sync.WaitGroup
	concurrencyLevel := 100

	for i := 0; i < concurrencyLevel; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cache.Set(ctx, key+strconv.Itoa(i), value, expiration)
			_, found, err := cache.Get(ctx, key+strconv.Itoa(i))
			if err != nil {
				t.Errorf("Get() error = %v", err)
			}
			if !found {
				t.Errorf("Expected to find the value for key %v", key+strconv.Itoa(i))
			}
		}(i)
	}

	wg.Wait()
}

func TestCachePersistence(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache_persistence.csv"
	defer os.Remove(file)

	cache := NewCache(file)
	ctx := context.Background()
	key := "persistKey"
	value := "persistValue"
	expiration := 5 * time.Second

	err = cache.Set(ctx, key, value, expiration)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	newCache := NewCache(file)
	gotValue, found, err := newCache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !found {
		t.Fatal("Get() did not find the value after reloading from file")
	}
	if gotValue != value {
		t.Fatalf("Get() = %v, want %v", gotValue, value)
	}
}

func TestCacheFileErrorHandling(t *testing.T) {
	file := "/path/to/nonexistent/directory/test_cache.csv"

	cache := NewCache(file)
	ctx := context.Background()
	key := "errorKey"
	value := "errorValue"
	expiration := 5 * time.Second

	err := cache.Set(ctx, key, value, expiration)
	if err == nil {
		t.Fatal("Set() should fail when unable to create the file")
	}
}

func TestCacheAutomaticExpiration(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache_auto_expiration.csv"
	defer os.Remove(file)

	cache := NewCache(file)
	ctx := context.Background()
	key := "expireKey"
	value := "expireValue"
	expiration := 1 * time.Second

	cache.Set(ctx, key, value, expiration)
	time.Sleep(expiration + 1*time.Second)
	cache.Get(ctx, key)

	if _, found := cache.items[key]; found {
		t.Fatal("Item should have been expired and not present in the cache")
	}
}

func TestCacheConcurrentWriteAndDelete(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache_concurrent_write_delete.csv"
	defer os.Remove(file)

	cache := NewCache(file)
	ctx := context.Background()
	key := "concurrentKey"
	value := "concurrentValue"
	expiration := 5 * time.Minute

	var wg sync.WaitGroup
	concurrencyLevel := 100

	for i := 0; i < concurrencyLevel; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			cache.Set(ctx, key, value, expiration)
		}(i)
		go func(i int) {
			defer wg.Done()
			cache.Get(ctx, key)
		}(i)
	}

	wg.Wait()
}

func TestCacheDataValidation(t *testing.T) {
	config, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Error reading config: %v", err)
	}

	cacheDir := config.PathConfig.TestCacheFilePath

	file := cacheDir + "test_cache_data_validation.csv"
	defer os.Remove(file)

	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		err := os.MkdirAll(cacheDir, os.ModePerm)
		if err != nil {
			t.Fatalf("Unable to create cache directory: %v", err)
		}
	}

	invalidData := []byte("invalidKey,invalidValue,notATimestamp\n")
	err = os.WriteFile(file, invalidData, 0666)
	if err != nil {
		t.Fatalf("Unable to write invalid data to file: %v", err)
	}

	cache := NewCache(file)
	if err := cache.loadFromFile(); err == nil {
		t.Fatal("Expected error when loading invalid data, but got nil")
	}
}
