package dbtransposer

import (
	"data-ingestor/config"
	"data-ingestor/mapreduce"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"log"
	"reflect"
	"strings"
)

type TransposerFunctionsInterface interface {
	// InsertRecords Map function paired with ExtractSQLData
	InsertRecords(tx *sql.Tx, tableName string, batch interface{}) error
	ExtractSQLData(record interface{}) (columns []string, rows [][]interface{}, err error)

	// ProcessMapResults is the Reducer function
	ProcessMapResults(results []mapreduce.MapResult) error
}

type TransposerFunctions struct {
	CONFIG *config.Config
	Logger *zap.Logger
}

var _ TransposerFunctionsInterface = (*TransposerFunctions)(nil)

func (mp *TransposerFunctions) InsertRecords(tx *sql.Tx, tableName string, obj interface{}) error {
	//for obj := range batch {
		mp.Logger.Info("Received object in InsertRecords", zap.Any("record", obj))
		columns, rows, err := mp.ExtractSQLData(obj)
		if err != nil {
			mp.Logger.Error("Failed to extract SQL data",
				zap.Any("record", obj), // Log the full object
				zap.Error(err))
			return fmt.Errorf("failed to extract SQL data: %w", err)
		}

	mp.Logger.Info("Extracted rows from data", zap.Any("rows", rows), zap.Int("row_count", len(rows)))

	// Build the base query
		query := fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES `,
			tableName,
			strings.Join(columns, ", "),
		)

		// Add placeholders for all rows
		var allPlaceholders []string
		var allValues []interface{}
		placeholderIndex := 1

	mp.Logger.Info("Extracted rows from data", zap.Any("rows", rows), zap.Int("row_count", len(rows)))

	for _, row := range rows {
		// Create placeholders for the current row
		rowPlaceholders := []string{}
		for range row {
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", placeholderIndex))
			placeholderIndex++
		}

		// Append placeholders for the current row
		allPlaceholders = append(allPlaceholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))

		// Append the actual values for the current row
		allValues = append(allValues, row...)

		// Log the placeholders and values for debugging
		mp.Logger.Info("===== Row being processed (Number of Rows) =====", zap.Any("Count", len(rows)), zap.Any("Row", row))
		mp.Logger.Info("All placeholders so far", zap.Strings("Placeholders", allPlaceholders))
		mp.Logger.Info("All values so far", zap.Any("Values", allValues))
	}

	// Combine the query with placeholders
		query += strings.Join(allPlaceholders, ", ")

		//mp.Logger.Info("Query After Combining: %v", query)
		// Execute the query
		mp.Logger.Info("All Values to Execute in SQL", zap.Any("All Values", allValues))
		_, err = tx.Exec(query, allValues...)
		if err != nil {
			mp.Logger.Error("Failed to execute SQL query",
				zap.String("query", query),
				zap.Any("record", obj), // Log the full object
				zap.Error(err))
			return fmt.Errorf("failed to insert records: %w", err)
		}

		// Log successful execution
		mp.Logger.Info("Successfully executed SQL query",
			zap.String("query", query),
			zap.Any("record", obj)) // Log the full object
	//}

	return nil
}

//// InsertRecords inserts a batch of MistAMSData records into the database.
//func InsertRecords(tx *sql.Tx, batch []interface{}) error {
//	// Prepare the SQL statement
//	query := `
//		INSERT INTO SFLW_RECS (
//			user, dt_created, dt_submitted, ast_name, location,
//			status, json_hash, local_id, filename, fnumber, scan_time
//		) VALUES (
//			:user, :dt_created, :dt_submitted, :ast_name, :location,
//			:status, :json_hash, :local_id, :filename, :fnumber, :scan_time
//		)`
//	stmt, err := tx.Prepare(query)
//	if err != nil {
//		return fmt.Errorf("failed to prepare statement: %w", err)
//	}
//	defer stmt.Close()
//
//	// Iterate over the batch and execute the query for each record
//	for _, obj := range batch {
//		// Assert the type of the item
//		record, ok := obj.(models.Record)
//		if !ok {
//			return fmt.Errorf("invalid record type: %T", record)
//		}
//
//		_, err := stmt.Exec(
//			sql.Named("user", record.User),
//			sql.Named("dt_created", record.DateCreated),
//			sql.Named("dt_submitted", record.DateSubmitted),
//			sql.Named("ast_name", record.AssetName), // Nullable
//			sql.Named("location", record.Location),
//			sql.Named("status", record.Status),
//			sql.Named("json_hash", record.JsonHash),
//			sql.Named("local_id", record.LocalID), // Nullable
//			sql.Named("filename", record.FileName),
//			sql.Named("fnumber", record.FNumber),
//			sql.Named("scan_time", record.ScanTime),
//		)
//		if err != nil {
//			log.Printf("Failed to insert record %+v: %v", record, err)
//			return fmt.Errorf("failed to insert record: %w", err)
//		}
//	}
//	return nil
//}

func (mp *TransposerFunctions) ExtractSQLData(record interface{}) ([]string, [][]interface{}, error) {
	v := reflect.ValueOf(record)
	t := reflect.TypeOf(record)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected a struct but got %s", v.Kind())
	}

	baseRow := []interface{}{}
	columns := []string{}
	rows := [][]interface{}{}

	// Iterate over fields in the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		dbTag := field.Tag.Get("db")

		if field.Anonymous {
			// Handle embedded anonymous structs
			nestedColumns, nestedRows, nestedErr := mp.ExtractSQLData(value.Interface())
			if nestedErr != nil {
				return nil, nil, nestedErr
			}

			columns = append(columns, nestedColumns...)
			if len(nestedRows) > 0 {
				baseRow = append(baseRow, nestedRows[0]...)
			}
		} else if value.Kind() == reflect.Slice {
			// Handle slices: generate rows for each slice element
			for j := 0; j < value.Len(); j++ {
				element := value.Index(j).Interface()
				elementValue := reflect.ValueOf(element)
				// Create a copy of the base row to avoid overwriting
				row := make([]interface{}, len(baseRow))
				copy(row, baseRow)

				// Set the slice element values into the appropriate indices
				for k := 0; k < elementValue.NumField(); k++ {
					sliceField := elementValue.Type().Field(k)
					sliceDBTag := sliceField.Tag.Get("db")
					if sliceDBTag == "" || sliceDBTag == "-" {
						continue // Skip fields without a "db" tag
					}

					// Find the index of the slice field in the column list
					mp.Logger.Info("Setting Slice Elements in Indices", zap.Any("columns", columns))
					for colIdx, colName := range columns {
						if colName == fmt.Sprintf(`"%s"`, sliceDBTag) {
							row[colIdx] = elementValue.Field(k).Interface()
							break
						}
					}
				}
				// Add the completed row
				rows = append(rows, row)
			}
		} else {
			// Add normal fields to base row
			if dbTag == "-" || dbTag == "" {
				continue // Skip fields without a valid "db" tag
			}
			columns = append(columns, fmt.Sprintf(`"%s"`, dbTag))
			baseRow = append(baseRow, value.Interface())
		}
	}

	mp.Logger.Info("Rows finishing ExtractSQLData", zap.Any("Rows", rows), zap.Any("Columns", columns))
	// If no slices were processed, use the base row as a single entry
	if len(rows) == 0 {
		rows = [][]interface{}{baseRow}
		mp.Logger.Info("Setting rows to base row and finishing ExtractSQLData", zap.Any("Rows", rows), zap.Any("Columns", columns))

	}

	return columns, rows, nil
}

//func (mp *TransposerFunctions) StreamMapFunc(tx *sql.Tx, record interface{}) error {
//	// Use ExtractSQLData to prepare SQL data dynamically
//	columns, rows, err := mp.ExtractSQLData(record)
//	if err != nil {
//		return fmt.Errorf("failed to extract SQL data: %w", err)
//	}
//
//	// Prepare SQL query
//	query := fmt.Sprintf(`INSERT INTO SFLW_RECS (%s) VALUES (%s)`,
//		strings.Join(columns, ", "),
//		strings.Repeat("$1, ", len(columns)-1)+"$"+strconv.Itoa(len(columns)),
//	)
//
//	// Execute the query for each row
//	for _, row := range rows {
//		_, err := tx.Exec(query, row...)
//		if err != nil {
//			return fmt.Errorf("failed to execute query: %w", err)
//		}
//	}
//	return nil
//}

func (mp *TransposerFunctions) ProcessMapResults(results []mapreduce.MapResult) error {
	// Define the Reduce function
	// Preemptively check for errors or nil transactions
	hasError := false
	for _, result := range results {
		if result.Tx == nil {
			log.Printf("Batch %d failed to start a transaction: %v", result.BatchID, result.Err)
			hasError = true
			continue
		}

		if result.Err != nil {
			log.Printf("Batch %d failed: %v", result.BatchID, result.Err)
			hasError = true
		}
	}

	// Rollback all transactions if any errors are found
	if hasError {
		log.Println("Errors detected during the map phase. Rolling back all transactions.")
		for _, result := range results {
			if result.Tx != nil {
				if err := result.Tx.Rollback(); err != nil {
					log.Printf("Failed to rollback transaction for batch %d: %v", result.BatchID, err)
				} else {
					log.Printf("Transaction for batch %d rolled back successfully", result.BatchID)
				}
			}
		}
		return fmt.Errorf("map phase completed with errors; all transactions rolled back")
	}

	// Commit all transactions if no errors are found
	for _, result := range results {
		if result.Tx != nil {
			if err := result.Tx.Commit(); err != nil {
				log.Printf("Failed to commit transaction for batch %d: %v", result.BatchID, err)
				return fmt.Errorf("failed to commit transaction for batch %d: %w", result.BatchID, err)
			}
			log.Printf("Transaction for batch %d committed successfully", result.BatchID)
		}
	}

	log.Println("All transactions committed successfully")
	return nil
}