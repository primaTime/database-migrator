package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror"
	_ "github.com/lib/pq"
)

type Config struct {
	Source    DatabaseConfig `json:"source"`
	Target    DatabaseConfig `json:"target"`
	BatchSize int            `json:"batchSize"`
	Tables    []Table        `json:"tables"`
}

type DatabaseConfig struct {
	DSN    string `json:"dsn"`
	Schema string `json:"schema"`
	Driver string `json:"driver"`
}

type Table struct {
	Name         string   `json:"name"`
	Columns      []string `json:"columns"`
	Dependencies []string `json:"dependencies"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.json", "Path to the configuration file")
	flag.Parse()

	if configPath == "" {
		log.Fatal("Configuration file path must be provided")
	}

	config, err := readConfig(configPath)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	// Connect to source database
	sourceDB, err := sql.Open(config.Source.Driver, config.Source.DSN)
	if err != nil {
		log.Fatalf("Source connection error: %v", err)
	}
	defer sourceDB.Close()

	// Connect to target database
	targetDB, err := sql.Open(config.Target.Driver, config.Target.DSN)
	if err != nil {
		log.Fatalf("Target connection error: %v", err)
	}
	defer targetDB.Close()

	progress := make(map[string]*struct {
		migrated int64
		total    int64
	})
	for _, table := range config.Tables {
		rowCount, err := getRowCount(sourceDB, table.Name, config.Source.Schema)

		if err != nil {
			log.Fatalf("Error getting row count for table %s: %v", table.Name, err)
		}

		progress[table.Name] = &struct {
			migrated int64
			total    int64
		}{
			migrated: 0,
			total:    rowCount,
		}
	}

	processedTables := make(map[string]bool)
	var wg sync.WaitGroup

	for len(processedTables) < len(config.Tables) {
		tablesToProcess := filterTables(config.Tables, processedTables)

		for _, table := range tablesToProcess {
			wg.Add(1)
			go func(table Table) {
				defer wg.Done()
				migrateTable(sourceDB, targetDB, table, config.BatchSize, progress, config.Source.Driver, config.Source.Schema, config.Target.Schema)
			}(table)

			processedTables[table.Name] = true
		}

		wg.Wait()
	}

	fmt.Println("Data migration complete.")
}

func filterTables(tables []Table, processedTables map[string]bool) []Table {
	result := make([]Table, 0)

	for _, table := range tables {
		if processedTables[table.Name] {
			continue
		}

		ready := true
		for _, dep := range table.Dependencies {
			if !processedTables[dep] {
				ready = false
				break
			}
		}

		if ready {
			result = append(result, table)
		}
	}

	return result
}

func readConfig(configFile string) (Config, error) {
	config := Config{}
	file, err := os.Open(configFile)
	if err != nil {
		return config, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(bytes, &config)
	return config, err
}

func migrateTable(sourceDB, targetDB *sql.DB, table Table, batchSize int, progress map[string]*struct {
	migrated int64
	total    int64
}, sourceDriver string, sourceSchema string, targetSchema string) {
	columns := table.Columns
	columnsJoined := joinColumns(columns)

	startTime := time.Now()

	// Prepare the insert statement for target database
	insertStmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES", targetSchema, table.Name, columnsJoined) + " %s"

	// Start progress display for this table
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for range progressTicker.C {
			printProgress(table.Name, progress, startTime)
		}
	}()

	var offset int64 = 0
	for {
		// Retrieve data from source using pagination
		var query string

		switch sourceDriver {
		case "godror":
			query = fmt.Sprintf(`
					SELECT %s FROM (
						SELECT t.*, ROWNUM rnum FROM (
							SELECT %s FROM %s.%s
						) t
						WHERE ROWNUM <= %d
					)
					WHERE rnum > %d`, columnsJoined, columnsJoined, sourceSchema, table.Name, offset+int64(batchSize), offset)
		case "postgres":
			query = fmt.Sprintf(`
					SELECT %s FROM (
						SELECT t.*, ROW_NUMBER() OVER () AS rnum FROM (
							SELECT %s FROM %s.%s
						) t
					) AS subquery
					WHERE rnum <= %d
					OFFSET %d`, columnsJoined, columnsJoined, sourceSchema, table.Name, offset+int64(batchSize), offset)
		}

		rows, err := sourceDB.Query(query)
		if err != nil {
			log.Fatalf("Error querying Source database for table %s: %v", table.Name, err)
		}

		values := make([]string, 0, len(columns)*batchSize)
		rowBatch := 0

		for rows.Next() {
			columnValues := make([]interface{}, len(columns))
			columnPointers := make([]interface{}, len(columns))

			for i := range columns {
				columnPointers[i] = &columnValues[i]
			}

			err = rows.Scan(columnPointers...)
			if err != nil {
				log.Fatalf("Error scanning row from table %s: %v", table.Name, err)
			}

			stringValues := make([]string, 0)
			for _, v := range columnValues {
				stringValues = append(stringValues, fmt.Sprintf("'%s'", fmt.Sprint(v)))
			}

			values = append(values, fmt.Sprintf("(%s)", joinColumns(stringValues)))
			rowBatch++
		}

		if rowBatch > 0 {
			insertQuery := fmt.Sprintf(insertStmt, strings.Join(values, ", "))
			_, err = targetDB.Exec(insertQuery)
			if err != nil {
				log.Fatalf("Error inserting batch into table %s: %v", table.Name, err)
			}

			atomic.AddInt64(&progress[table.Name].migrated, int64(rowBatch))
		}

		rows.Close()

		if err = rows.Err(); err != nil {
			log.Fatalf("Error iterating through rows for table %s: %v", table.Name, err)
		}

		if rowBatch < batchSize {
			break
		}

		offset += int64(batchSize)
	}

	progressTicker.Stop()                          // stop the progress ticker
	printProgress(table.Name, progress, startTime) // print the final progress
}

func joinColumns(columns []string) string {
	return strings.Join(columns, ", ")
}

func getRowCount(db *sql.DB, tableName string, schema string) (int64, error) {
	var rowCount int64
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName)).Scan(&rowCount)
	return rowCount, err
}

func printProgress(tableName string, progress map[string]*struct {
	migrated int64
	total    int64
}, startTime time.Time) {
	p := progress[tableName]
	migratedRows := atomic.LoadInt64(&p.migrated)
	remainingRows := p.total - migratedRows
	completionPercentage := float64(migratedRows) / float64(p.total) * 100

	if migratedRows != 0 {
		elapsedTime := time.Since(startTime)
		timePerRow := elapsedTime / time.Duration(migratedRows)
		estimatedRemainingTime := time.Duration(remainingRows) * timePerRow

		fmt.Printf("Data migration progress for table %s: %d/%d rows migrated (%.2f%%), Estimated time left: %v\n",
			tableName, migratedRows, p.total, completionPercentage, estimatedRemainingTime)
	} else {
		fmt.Printf("Data migration progress for table %s: %d/%d rows migrated (%.2f%%)",
			tableName, migratedRows, p.total, completionPercentage)
	}

}
