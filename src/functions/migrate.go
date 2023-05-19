package functions

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/primatime/database-migrator/config"
	"github.com/primatime/database-migrator/progress"
	"github.com/primatime/database-migrator/utils"
)

func MigrateTable(sourceDB, targetDB *sql.DB, table Table, batchSize int, progressData progress.Data, sourceDriver string, sourceSchema string, targetSchema string) {
	columns := table.Columns
	columnsJoined := utils.JoinColumns(columns)

	startTime := time.Now()

	// Prepare the insert statement for target database
	insertStmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES", targetSchema, table.Name, columnsJoined) + " %s"

	// Start progress display for this table
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	go func() {
		for range progressTicker.C {
			progress.Print(table.Name, progressData, startTime)
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
				if v == nil {
					stringValues = append(stringValues, "NULL")
				} else if t, ok := v.(time.Time); ok {
					stringValues = append(stringValues, t.UTC().Format("'2006-01-02 15:04:05.00 +00:00'"))
				} else {
					stringValues = append(stringValues, fmt.Sprintf("'%s'", fmt.Sprint(v)))
				}
			}

			values = append(values, fmt.Sprintf("(%s)", utils.JoinColumns(stringValues)))
			rowBatch++
		}

		if rowBatch > 0 {
			insertQuery := fmt.Sprintf(insertStmt, strings.Join(values, ", "))
			_, err = targetDB.Exec(insertQuery)
			if err != nil {
				log.Fatalf("Error inserting batch into table %s: %v", table.Name, err)
			}

			atomic.AddInt64(&progressData[table.Name].Migrated, int64(rowBatch))
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

	progressTicker.Stop()                               // stop the progress ticker
	progress.Print(table.Name, progressData, startTime) // print the final progress
}
