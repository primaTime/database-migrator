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
	OracleDSN      string  `json:"oracleDSN"`
	OracleSchema   string  `json:"oracleSchema"`
	PostgresDSN    string  `json:"postgresDSN"`
	PostgresSchema string  `json:"postgresSchema"`
	BatchSize      int     `json:"batchSize"`
	Tables         []Table `json:"tables"`
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

	// Connect to Oracle database
	oracleDB, err := sql.Open("godror", config.OracleDSN)
	if err != nil {
		log.Fatalf("Oracle connection error: %v", err)
	}
	defer oracleDB.Close()

	// Connect to PostgreSQL database
	postgresDB, err := sql.Open("postgres", config.PostgresDSN)
	if err != nil {
		log.Fatalf("PostgreSQL connection error: %v", err)
	}
	defer postgresDB.Close()

	progress := make(map[string]*struct {
		migrated int64
		total    int64
	})
	for _, table := range config.Tables {
		rowCount, err := getRowCount(oracleDB, table.Name, config.OracleSchema)

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
				migrateTable(oracleDB, postgresDB, table, config.BatchSize, progress, config.OracleSchema, config.PostgresSchema)
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

func migrateTable(oracleDB, postgresDB *sql.DB, table Table, batchSize int, progress map[string]*struct {
	migrated int64
	total    int64
}, oracleSchema string, postgresSchema string) {
	columns := table.Columns
	columnsJoined := joinColumns(columns)
	placeholderValues := createPlaceholders(len(columns))

	startTime := time.Now()

	// Prepare the insert statement for PostgreSQL
	insertStmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", postgresSchema, table.Name, columnsJoined, placeholderValues)
	stmt, err := postgresDB.Prepare(insertStmt)
	if err != nil {
		log.Fatalf("Error preparing insert statement for table %s: %v", table.Name, err)
	}
	defer stmt.Close()

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
		// Retrieve data from Oracle using pagination
		rows, err := oracleDB.Query(fmt.Sprintf(`
			SELECT * FROM (
				SELECT t.*, ROWNUM rnum FROM (
					SELECT %s FROM %s.%s
				) t
				WHERE ROWNUM <= %d
			)
			WHERE rnum > %d`, columnsJoined, oracleSchema, table.Name, offset+int64(batchSize), offset))
		if err != nil {
			log.Fatalf("Error querying Oracle database for table %s: %v", table.Name, err)
		}

		values := make([]interface{}, 0, len(columns)*batchSize)
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

			values = append(values, columnValues...)
			rowBatch++
		}

		if rowBatch > 0 {
			_, err = stmt.Exec(values...)
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

func createPlaceholders(columnCount int) string {
	placeholders := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ", ")
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

	elapsedTime := time.Since(startTime)
	timePerRow := elapsedTime / time.Duration(migratedRows)
	estimatedRemainingTime := time.Duration(remainingRows) * timePerRow

	fmt.Printf("Data migration progress for table %s: %d/%d rows migrated (%.2f%%), Estimated time left: %v\n",
		tableName, migratedRows, p.total, completionPercentage, estimatedRemainingTime)
}
