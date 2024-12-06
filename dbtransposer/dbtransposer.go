package dbtransposer

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// InsertRecord dynamically constructs an INSERT query and executes it using database/sql.
func InsertRecord(db *sql.DB, tableName string, record interface{}) error {
	// Use reflection to inspect the record struct
	val := reflect.ValueOf(record)
	typ := reflect.TypeOf(record)

	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", typ.Kind())
	}

	// Prepare slices for columns, values, and placeholders
	var columns []string
	var values []interface{}
	var placeholders []string

	// Iterate over struct fields
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)        // Get field metadata
		column := field.Tag.Get("db") // Extract the `db` tag
		if column == "" {
			continue // Skip fields without a `db` tag
		}
		columns = append(columns, column)                      // Add column name
		values = append(values, val.Field(i).Interface())      // Add field value
		placeholders = append(placeholders, fmt.Sprintf(":%d", i+1)) // Add placeholder
	}

	if len(columns) == 0 {
		return fmt.Errorf("no db tags found on struct fields")
	}

	// Construct the SQL query
	columnList := strings.Join(columns, ", ")
	placeholderList := strings.Join(placeholders, ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholderList)

	// Execute the query
	_, err := db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}

