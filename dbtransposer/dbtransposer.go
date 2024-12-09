package dbtransposer

import (
	"data-ingestor/mapreduce"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
)

func InsertRecords(tx *sql.Tx, tableName string, batch []interface{}) error {
	for _, obj := range batch {
		// Extract SQL data from the record
		columns, placeholders, values, err := ExtractSQLData(obj)
		if err != nil {
			return fmt.Errorf("failed to extract SQL data: %w", err)
		}

		// Dynamically generate the query
		query := fmt.Sprintf(
			`INSERT INTO %s (%s) VALUES (%s)`,
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		// Execute the query with the extracted values
		_, err = tx.Exec(query, values...)
		if err != nil {
			log.Printf("Failed to insert record %+v: %v", obj, err)
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	return nil
}



//func InsertRecords(tx *sql.Tx, batch []interface{}) error {
//	// Prepare the SQL statement with positional parameters
//	query := `
//		INSERT INTO SFLW_RECS (
//			"user", dt_created, dt_submitted, ast_name, location,
//			status, json_hash, local_id, filename, fnumber, scan_time
//		) VALUES (
//			$1, $2, $3, $4, $5,
//			$6, $7, $8, $9, $10, $11
//		)`
//	stmt, err := tx.Prepare(query)
//	if err != nil {
//		return fmt.Errorf("failed to prepare statement: %w", err)
//	}
//	defer stmt.Close()
//
//	// Iterate over the batch and process each record
//	for _, obj := range batch {
//		// Assert the type of the item
//		record, ok := obj.(models.Record)
//		if !ok {
//			return fmt.Errorf("invalid record type: %T", obj)
//		}
//
//		// If FNumbers exists, insert one row per FNumber
//		if len(record.FNumbers) > 0 {
//			for _, fNumberEntry := range record.FNumbers {
//				_, err := stmt.Exec(
//					record.User,                // $1
//					record.DateCreated,         // $2
//					record.DateSubmitted,       // $3
//					record.AssetName,           // $4 (nullable)
//					record.Location,            // $5
//					record.Status,              // $6
//					record.JsonHash,            // $7
//					record.LocalID,             // $8 (nullable)
//					record.FileName,            // $9
//					fNumberEntry.FNumber,       // $10
//					fNumberEntry.ScanTime,      // $11
//				)
//				if err != nil {
//					log.Printf("Failed to insert record with FNumber %+v: %v", fNumberEntry, err)
//					return fmt.Errorf("failed to insert record: %w", err)
//				}
//			}
//		} else {
//			// If no FNumbers, insert a single row with empty FNumber and ScanTime
//			_, err := stmt.Exec(
//				record.User,          // $1
//				record.DateCreated,   // $2
//				record.DateSubmitted, // $3
//				record.AssetName,     // $4 (nullable)
//				record.Location,      // $5
//				record.Status,        // $6
//				record.JsonHash,      // $7
//				record.LocalID,       // $8 (nullable)
//				record.FileName,      // $9
//				"",                   // $10 (empty FNumber)
//				"",                   // $11 (empty ScanTime)
//			)
//			if err != nil {
//				log.Printf("Failed to insert record without FNumber: %+v: %v", record, err)
//				return fmt.Errorf("failed to insert record: %w", err)
//			}
//		}
//	}
//
//	return nil
//}


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

func ProcessMapResults(results []mapreduce.MapResult) error {
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

func ExtractSQLData(record interface{}) (columns []string, placeholders []string, values []interface{}, err error) {
	v := reflect.ValueOf(record)
	t := reflect.TypeOf(record)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, nil, nil, fmt.Errorf("expected a struct but got %s", v.Kind())
	}

	placeholderIndex := 1

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// Check if the field has a "db" tag
		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue // Skip fields without a "db" tag or explicitly ignored
		}

		if field.Anonymous {
			// Handle embedded anonymous structs
			nestedColumns, nestedPlaceholders, nestedValues, nestedErr := ExtractSQLData(value.Interface())
			if nestedErr != nil {
				return nil, nil, nil, nestedErr
			}

			columns = append(columns, nestedColumns...)
			placeholders = append(placeholders, nestedPlaceholders...)
			values = append(values, nestedValues...)
		} else if field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Array || value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
			// Handle slices/arrays by flattening
			for j := 0; j < value.Len(); j++ {
				sliceValue := value.Index(j).Interface()
				columns = append(columns, fmt.Sprintf(`"%s"`, dbTag))
				placeholders = append(placeholders, fmt.Sprintf("$%d", placeholderIndex))
				placeholderIndex++
				fmt.Printf("Slice Value: %v", sliceValue)
				values = append(values, sliceValue)
			}
		} else {
			// Add normal fields
			columns = append(columns, fmt.Sprintf(`"%s"`, dbTag))
			placeholders = append(placeholders, fmt.Sprintf("$%d", placeholderIndex))
			placeholderIndex++
			values = append(values, value.Interface())
		}
	}

	fmt.Printf("Columns: %v\nPlaceholders: %v\nValues: %v\n", columns, placeholders, values)
	return columns, placeholders, values, nil
}


