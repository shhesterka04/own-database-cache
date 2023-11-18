package config

import (
	"encoding/json"
	"os"
)

type PathConfig struct {
	DatabaseFilePath     string `json:"databaseFilePath"`
	CacheFilePath        string `json:"cacheFilePath"`
	FileName             string `json:"fileName"`
	TestDatabaseFilePath string `json:"testDatabaseFilePath"`
	TestCacheFilePath    string `json:"testCacheFilePath"`
}

type Config struct {
	PathConfig          PathConfig `json:"path"`
	ExpirationTimeCache int        `json:"expirationTimeCache"`
}

func LoadConfig(configPath string) (*Config, error) {
	configFile, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(configFile, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
