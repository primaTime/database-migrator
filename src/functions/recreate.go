package functions

import (
	"database/sql"
	"fmt"
	"log"

	. "github.com/primatime/database-migrator/config"
)

func RecreateStructure(driver string, sourceDB *sql.DB, config Config, configPath string) {
	schema := config.Source.Schema

	var tablesQuery string

	switch driver {
	case "godror":
		tablesQuery = fmt.Sprintf("SELECT table_name FROM all_tables WHERE owner = UPPER('%s')", schema)
	case "postgres":
		tablesQuery = fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schema)
	}

	tableRows, err := sourceDB.Query(tablesQuery)
	if err != nil {
		log.Fatalf("Error fetching tables: %v", err)
	}
	defer tableRows.Close()

	tables := make([]Table, 0)
	for tableRows.Next() {
		var tableName string
		err := tableRows.Scan(&tableName)
		if err != nil {
			log.Fatalf("Error scanning table name: %v", err)
		}

		// query all columns of the table
		var columnsQuery string

		switch driver {
		case "godror":
			columnsQuery = fmt.Sprintf("SELECT column_name FROM all_tab_columns WHERE table_name = '%s' AND owner = UPPER('%s') ORDER BY COLUMN_ID", tableName, schema)
		case "postgres":
			columnsQuery = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'", tableName, schema)
		}

		columnRows, err := sourceDB.Query(columnsQuery)
		if err != nil {
			log.Fatalf("Error fetching columns for table %s: %v", tableName, err)
		}

		columns := make([]string, 0)
		for columnRows.Next() {
			var columnName string
			err := columnRows.Scan(&columnName)
			if err != nil {
				log.Fatalf("Error scanning column name: %v", err)
			}
			columns = append(columns, columnName)
		}

		// query all dependencies (foreign key relations) of the table
		var dependenciesQuery string
		switch driver {
		case "godror":
			dependenciesQuery = fmt.Sprintf(`
			SELECT 
				a.table_name
			FROM 
				all_constraints a
			JOIN
				all_constraints b
			ON
				b.r_constraint_name = a.constraint_name
			WHERE 
				b.table_name = '%s' AND b.owner = UPPER('%s') AND b.constraint_type = 'R'
		`, tableName, schema)
		case "postgres":
			dependenciesQuery = fmt.Sprintf(`
			SELECT 
				ccu.table_name AS foreign_table_name
			FROM 
				information_schema.table_constraints AS tc 
			JOIN 
				information_schema.key_column_usage AS kcu
			  ON 
				tc.constraint_name = kcu.constraint_name
			  AND tc.table_schema = kcu.table_schema
			JOIN 
				information_schema.constraint_column_usage AS ccu
			  ON 
				ccu.constraint_name = tc.constraint_name
			  AND ccu.table_schema = tc.table_schema
			WHERE 
				tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s' AND tc.table_schema = '%s'
		`, tableName, schema)
		}
		dependencyRows, err := sourceDB.Query(dependenciesQuery)
		if err != nil {
			log.Fatalf("Error fetching dependencies for table %s: %v", tableName, err)
		}

		dependencies := make([]string, 0)
		for dependencyRows.Next() {
			var dependencyName string
			err := dependencyRows.Scan(&dependencyName)
			if err != nil {
				log.Fatalf("Error scanning dependency name: %v", err)
			}
			dependencies = append(dependencies, dependencyName)
		}

		tables = append(tables, Table{Name: tableName, Columns: columns, Dependencies: dependencies})
	}

	config.Tables = tables

	WriteConfig(configPath, config)

	fmt.Println("Config file created successfully.")
}
