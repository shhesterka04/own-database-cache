package main

import (
	"context"
	"fmt"
	"own-database-cache/internal/app"
	"own-database-cache/internal/config"
	"own-database-cache/internal/datasource/cache"
	"own-database-cache/internal/datasource/database"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	config, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	cacheFile := config.PathConfig.CacheFilePath
	databaseFile := config.PathConfig.DatabaseFilePath
	fileName := config.PathConfig.FileName

	cacheClient := cache.NewClient(cacheFile + fileName)
	databaseClient := database.NewClient(databaseFile)

	if err := app.Process(ctx, cacheClient, databaseClient); err != nil {
		fmt.Println("Error:", err)
	}
}
