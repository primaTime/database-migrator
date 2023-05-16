package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"

	. "github.com/primatime/database-migrator/config"
	"github.com/primatime/database-migrator/functions"
	"github.com/primatime/database-migrator/progress"
	"github.com/primatime/database-migrator/utils"

	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
)

func main() {
	var configPath string
	var recreateConfig bool

	flag.StringVar(&configPath, "config", "config.json", "Path to the configuration file")
	flag.BoolVar(&recreateConfig, "recreateConfig", false, "Only automatically recreate config from source database structure")
	flag.Parse()

	if configPath == "" {
		log.Fatal("Configuration file path must be provided")
	}

	config, err := ReadConfig(configPath)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	// Connect to source database
	sourceDB, err := sql.Open(config.Source.Driver, config.Source.DSN)
	if err != nil {
		log.Fatalf("Source connection error: %v", err)
	}
	defer sourceDB.Close()

	if recreateConfig {
		functions.RecreateStructure(config.Source.Driver, sourceDB, config, configPath)
		fmt.Println("Config recreation complete.")
		return
	}

	// Connect to target database
	targetDB, err := sql.Open(config.Target.Driver, config.Target.DSN)
	if err != nil {
		log.Fatalf("Target connection error: %v", err)
	}
	defer targetDB.Close()

	progressData := make(progress.Data)
	for _, table := range config.Tables {
		rowCount, err := utils.GetRowCount(sourceDB, table.Name, config.Source.Schema)

		if err != nil {
			log.Fatalf("Error getting row count for table %s: %v", table.Name, err)
		}

		progressData[table.Name] = &progress.DataItem{
			Migrated: 0,
			Total:    rowCount,
		}
	}

	processedTables := make(map[string]bool)
	var wg sync.WaitGroup

	for len(processedTables) < len(config.Tables) {
		tablesToProcess := utils.FilterTables(config.Tables, processedTables)

		for _, table := range tablesToProcess {
			wg.Add(1)
			go func(table Table) {
				defer wg.Done()
				functions.MigrateTable(sourceDB, targetDB, table, config.BatchSize, progressData, config.Source.Driver, config.Source.Schema, config.Target.Schema)
			}(table)

			processedTables[table.Name] = true
		}

		wg.Wait()
	}

	fmt.Println("Data migration complete.")
}
