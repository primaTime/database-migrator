package utils

import (
	"database/sql"
	"fmt"
	"strings"

	. "github.com/primatime/database-migrator/config"
)

func FilterTables(tables []Table, processedTables map[string]bool) []Table {
	result := make([]Table, 0)

	for _, table := range tables {
		if processedTables[table.Name] {
			continue
		}

		ready := true
		for _, dep := range table.Dependencies {
			if dep == table.Name {
				continue
			}

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

func GetRowCount(db *sql.DB, tableName string, schema string) (int64, error) {
	var rowCount int64
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName)).Scan(&rowCount)
	return rowCount, err
}

func JoinColumns(columns []string) string {
	return strings.Join(columns, ", ")
}
