package dbtransposer

import (
	"data-ingestor/config"
	"data-ingestor/mapreduce"
	"database/sql"
	"fmt"
	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
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
	KeyColumnMapping map[string]map[string]string // Map for key-column mappings
}

var _ TransposerFunctionsInterface = (*TransposerFunctions)(nil)

// InsertRecords inserts records into the specified database table.
// It accepts a database transaction, table name, and an object containing the data to be inserted.
// The function dynamically constructs the SQL query based on the object's fields and values.
//
// Parameters:
// - tx: The database transaction used for executing the SQL query.
// - tableName: The name of the table to insert the records into.
// - obj: The object containing the data to be inserted.
//
// Returns:
// - An error if the SQL query execution fails or data extraction fails.
func (mp *TransposerFunctions) InsertRecords(tx *sql.Tx, tableName string, obj interface{}) error {
	// Log the start of the insertion process
	mp.Logger.Info("Received object in InsertRecords", zap.Any("object", obj))

	// Extract SQL columns and rows from the object using ExtractSQLData
	columns, rows, err := mp.ExtractSQLData(obj)
	if err != nil {
		// Log and return an error if data extraction fails
		mp.Logger.Error("Failed to extract SQL data",
			zap.Any("object", obj), // Log the full object
			zap.Error(err))
		return fmt.Errorf("failed to extract SQL data: %w", err)
	}

	// Build the base INSERT query with the table name and columns
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES `,
		tableName,
		strings.Join(columns, ", "),
	)

	// Variables to hold the placeholders and values for all rows
	var allPlaceholders []string
	var allValues []interface{}
	placeholderIndex := 1

	// Log the extracted rows and their count for debugging
	mp.Logger.Info("Extracted rows from data", zap.Any("rows", rows), zap.Int("row_count", len(rows)))

	// Iterate through the rows to generate placeholders and values
	for _, row := range rows {
		// Create a slice for placeholders for the current row
		rowPlaceholders := []string{}
		for range row {
			// Generate placeholder strings (e.g., $1, $2, ...)
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", placeholderIndex))
			placeholderIndex++
		}

		// Append the placeholders for the current row
		allPlaceholders = append(allPlaceholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))

		// Append the actual values for the current row
		allValues = append(allValues, row...)

		// Log detailed information about the current row being processed
		mp.Logger.Info("Row being processed", zap.Any("Row", row))
		mp.Logger.Info("All placeholders so far", zap.Strings("Placeholders", allPlaceholders))
		mp.Logger.Info("All values so far", zap.Any("Values", allValues))
	}

	// Combine the query with all generated placeholders
	query += strings.Join(allPlaceholders, ", ")

	// Log the final SQL query and values before execution
	mp.Logger.Info("Final SQL query being executed", zap.String("query", query))
	mp.Logger.Info("All Values to Execute in SQL", zap.Any("All Values", allValues))

	// Execute the SQL query with the collected values
	_, err = tx.Exec(query, allValues...)
	if err != nil {
		// Log and return an error if query execution fails
		mp.Logger.Error("Failed to execute SQL query",
			zap.String("query", query),
			zap.Any("record", obj), // Log the full object
			zap.Error(err))
		return fmt.Errorf("failed to insert records: %w", err)
	}

	// Log successful execution of the SQL query
	mp.Logger.Info("Successfully executed SQL query",
		zap.String("query", query),
		zap.Any("record", obj)) // Log the full object

	return nil
}


// InsertRecordsUsingSchema inserts records into the specified database table.
// It accepts a database transaction, table name, and an object containing the data to be inserted.
// The function dynamically constructs the SQL query based on the object's fields and values.
//
// Parameters:
// - tx: The database transaction used for executing the SQL query.
// - tableName: The name of the table to insert the records into.
// - obj: The object containing the data to be inserted.
//
// Returns:
// - An error if the SQL query execution fails or data extraction fails.
func (mp *TransposerFunctions) InsertRecordsUsingSchema(tx *sql.Tx, tableName string, obj map[string]interface{}) error {
	// Log the start of the insertion process
	mp.Logger.Info("Received object in InsertRecords", zap.Any("object", obj))

	columns, placeholderCount, err := mp.ExtractSQLDataFromExcel("db-template.xlsx", "Sheet1", "A3:K3", 3)

	mp.Logger.Info("Extracted SQL Data (From Excel)", zap.Any("templateFile", "db-template.xlsx"), zap.Any("placeholderCount", placeholderCount), zap.Any("columns", columns))

	// Extract SQL columns and rows from the object using ExtractSQLData
	columns, rows, err := mp.ExtractSQLDataUsingSchema(obj, "Record")
	if err != nil {
		// Log and return an error if data extraction fails
		mp.Logger.Error("Failed to extract SQL data",
			zap.Any("object", obj), // Log the full object
			zap.Error(err))
		return fmt.Errorf("failed to extract SQL data: %w", err)
	}

	// Build the base INSERT query with the table name and columns
	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES `,
		tableName,
		strings.Join(columns, ", "),
	)

	// Variables to hold the placeholders and values for all rows
	var allPlaceholders []string
	var allValues []interface{}
	placeholderIndex := 1

	// Log the extracted rows and their count for debugging
	mp.Logger.Info("Extracted rows from data", zap.Any("rows", rows), zap.Int("row_count", len(rows)))

	// Iterate through the rows to generate placeholders and values
	for _, row := range rows {
		// Create a slice for placeholders for the current row
		rowPlaceholders := []string{}
		for range row {
			// Generate placeholder strings (e.g., $1, $2, ...)
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", placeholderIndex))
			placeholderIndex++
		}

		// Append the placeholders for the current row
		allPlaceholders = append(allPlaceholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))

		// Append the actual values for the current row
		allValues = append(allValues, row...)

		// Log detailed information about the current row being processed
		mp.Logger.Info("Row being processed", zap.Any("Row", row))
		mp.Logger.Info("All placeholders so far", zap.Strings("Placeholders", allPlaceholders))
		mp.Logger.Info("All values so far", zap.Any("Values", allValues))
	}

	// Combine the query with all generated placeholders
	query += strings.Join(allPlaceholders, ", ")

	// Log the final SQL query and values before execution
	mp.Logger.Info("Final SQL query being executed", zap.String("query", query))
	mp.Logger.Info("All Values to Execute in SQL", zap.Any("All Values", allValues))

	// Execute the SQL query with the collected values
	_, err = tx.Exec(query, allValues...)
	if err != nil {
		// Log and return an error if query execution fails
		mp.Logger.Error("Failed to execute SQL query",
			zap.String("query", query),
			zap.Any("record", obj), // Log the full object
			zap.Error(err))
		return fmt.Errorf("failed to insert records: %w", err)
	}

	// Log successful execution of the SQL query
	mp.Logger.Info("Successfully executed SQL query",
		zap.String("query", query),
		zap.Any("record", obj)) // Log the full object

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

// ExtractSQLData extracts column names and rows for SQL insertion from a struct.
// This function processes struct fields recursively, handling:
// - Anonymous embedded structs
// - Slices, where each element generates a new row
// - Regular fields with `db` tags
//
// Parameters:
//   - record: The struct instance to process
//
// Returns:
//   - columns: A list of column names derived from `db` tags in the struct
//   - rows: A 2D slice where each inner slice represents a row of values
//   - error: An error, if any occurs during processing
func (mp *TransposerFunctions) ExtractSQLData(record interface{}) ([]string, [][]interface{}, error) {
	// Reflect on the provided record
	v := reflect.ValueOf(record)
	t := reflect.TypeOf(record)

	// Handle pointer types by de-referencing
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// Ensure the record is a struct
	if v.Kind() != reflect.Struct {
		mp.Logger.Error("Object is not a Struct", zap.Any("object_type", v.Kind()))
		return nil, nil, fmt.Errorf("expected a struct but got %s", v.Kind())
	}

	// Initialize base row, columns, and rows
	baseRow := []interface{}{}
	columns := []string{}
	rows := [][]interface{}{}

	// Iterate over fields in the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// Get the `db` tag for the field
		dbTag := field.Tag.Get("db")

		if field.Anonymous {
			// Handle embedded anonymous structs
			mp.Logger.Info("Processing anonymous struct", zap.String("Field", field.Name))
			nestedColumns, nestedRows, nestedErr := mp.ExtractSQLData(value.Interface())
			if nestedErr != nil {
				return nil, nil, nestedErr
			}

			// Append nested columns and rows
			columns = append(columns, nestedColumns...)
			if len(nestedRows) > 0 {
				rows = append(rows, nestedRows...)
			}
		} else if value.Kind() == reflect.Slice {
			// Handle slices: generate rows for each slice element
			mp.Logger.Info("Processing slice field", zap.String("Field", field.Name))
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

					// Match slice field with the column index and set value
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
			// Add fields that have "db" tags to columns and base row
			if dbTag == "-" || dbTag == "" {
				continue // Skip fields without a valid "db" tag
			}
			columns = append(columns, fmt.Sprintf(`"%s"`, dbTag))
			baseRow = append(baseRow, value.Interface())
		}
	}

	// Handle cases with no slices: use the base row as a single entry
	if len(rows) == 0 && len(baseRow) > 0 {
		rows = [][]interface{}{baseRow}
		mp.Logger.Info("No slices found, using base row as a single entry", zap.Any("BaseRow", baseRow))
	} else if len(rows) > 0 {
		mp.Logger.Info("Rows generated from slices", zap.Any("Rows", rows))
	}

	// Log the final state
	mp.Logger.Info("Finished extracting SQL data",
		zap.Any("Columns", columns),
		zap.Any("Rows", rows),
	)

	return columns, rows, nil
}

// ExtractSQLDataUsingSchema extracts SQL column names and rows from a record based on a JSON schema.
// This function processes:
// - Nested mappings defined in the schema
// - Slices, generating rows for each element
// - Regular fields based on the schema
//
// Parameters:
//   - record: The data to be processed (struct or compatible type).
//   - schema: The JSON schema defining the mappings (e.g., db, json, xml tags).
//
// Returns:
//   - columns: A list of column names defined by the schema.
//   - rows: A 2D slice of values for SQL insertion.
//   - error: An error, if any issues occur during processing.
func (mp *TransposerFunctions) ExtractSQLDataUsingSchema(record map[string]interface{}, modelName string) ([]string, [][]interface{}, error) {
	// Retrieve the key-column mapping for the given model
	columnMapping, exists := mp.KeyColumnMapping[modelName]
	if !exists {
		mp.Logger.Error("No key-column mapping found for model", zap.String("modelName", modelName))
		return nil, nil, fmt.Errorf("no key-column mapping found for model %s", modelName)
	}

	// Initialize columns and rows
	columns := []string{}
	rows := [][]interface{}{}

	// Flatten the record into columns and values
	row := []interface{}{}
	for key, value := range record {
		// Get the corresponding column name
		column, ok := columnMapping[key]
		if !ok {
			mp.Logger.Warn("Skipping unmapped key", zap.String("key", key))
			continue // Skip keys that don't have a mapping
		}

		// Append the column name and value
		columns = append(columns, fmt.Sprintf(`"%s"`, column))
		row = append(row, value)
	}

	// Add the row to rows
	rows = append(rows, row)

	// Log the extracted data
	mp.Logger.Info("Extracted SQL data",
		zap.Strings("Columns", columns),
		zap.Any("Rows", rows),
	)

	return columns, rows, nil
}


// ExtractSQLDataFromExcel processes an Excel file to determine SQL column names and placeholders based on a range and line.
// This function handles:
// - Identifying the number of columns in a specified range.
// - Counting non-empty cells in a specific line to determine placeholders.
//
// Parameters:
//   - filePath: Path to the Excel file.
//   - sheetName: Name of the sheet to process.
//   - rangeSpec: Cell range to analyze for column names (e.g., "A1:Z1").
//   - line: The line number to analyze for placeholders.
//
// Returns:
//   - columns: A list of column names.
//   - placeholderCount: The number of placeholders based on the line.
//   - error: An error, if any issues occur during processing.
func (mp *TransposerFunctions) ExtractSQLDataFromExcel(filePath, sheetName, rangeSpec string, line int) ([]string, int, error) {
	// Open the Excel file
	file, err := excelize.OpenFile(filePath)
	if err != nil {
		mp.Logger.Error("Failed to open Excel file", zap.String("filePath", filePath), zap.Error(err))
		return nil, 0, fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer file.Close()

	// Read the specified range to get column names
	rows, err := file.GetRows(sheetName)
	if err != nil {
		mp.Logger.Error("Failed to read rows from sheet", zap.String("sheetName", sheetName), zap.Error(err))
		return nil, 0, fmt.Errorf("failed to read rows from sheet: %w", err)
	}

	columns := []string{}
	placeholderCount := 0

	// Process the specified range for column names
	for _, cell := range rows[0] { // Assuming first row of the range
		if cell != "" {
			columns = append(columns, cell)
		}
	}

	// Log the extracted columns
	mp.Logger.Info("Extracted columns from Excel",
		zap.String("filePath", filePath),
		zap.String("sheetName", sheetName),
		zap.String("rangeSpec", rangeSpec),
		zap.Strings("Columns", columns),
	)

	// Process the specified line to determine placeholders
	if line <= len(rows) {
		for _, cell := range rows[line-1] { // Adjusting for 0-based index
			if cell != "" {
				placeholderCount++
			}
		}
	}

	// Log the placeholder count
	mp.Logger.Info("Determined placeholder count",
		zap.String("filePath", filePath),
		zap.String("sheetName", sheetName),
		zap.Int("line", line),
		zap.Int("PlaceholderCount", placeholderCount),
	)

	return columns, placeholderCount, nil
}


// ProcessMapResults handles the results of the map phase and ensures proper transaction management.
// It checks for errors in the map phase, rolls back transactions in case of errors, or commits them if all map results are successful.
//
// Parameters:
// - results: A slice of MapResult objects containing the results of the map phase.
//
// Returns:
// - An error if any transactions failed or if committing a transaction fails.
func (mp *TransposerFunctions) ProcessMapResults(results []mapreduce.MapResult) error {
	// Preemptively check for errors or nil transactions in the map results
	hasError := false

	// Iterate through each map result to identify errors or failed transactions
	for _, result := range results {
		if result.Tx == nil {
			// Log an error if the transaction is nil
			mp.Logger.Error("Failed to start a transaction",
				zap.Int("Worker ID", result.BatchID),
				zap.Error(result.Err),
			)
			hasError = true
			continue
		}

		if result.Err != nil {
			// Log an error if the map phase encountered an error
			mp.Logger.Error("Transaction encountered an error",
				zap.Int("Worker ID", result.BatchID),
				zap.Error(result.Err),
			)
			hasError = true
		}
	}

	// Rollback all transactions if any errors are found during the map phase
	if hasError {
		mp.Logger.Warn("Errors detected during the map phase. Rolling back all transactions.")

		for _, result := range results {
			if result.Tx != nil {
				// Attempt to rollback the transaction
				if err := result.Tx.Rollback(); err != nil {
					// Log an error if the rollback fails
					mp.Logger.Error("Failed to rollback transaction",
						zap.Int("Worker ID", result.BatchID),
						zap.Error(err),
					)
				} else {
					// Log success if the rollback completes
					mp.Logger.Info("Transaction rolled back successfully",
						zap.Int("Worker ID", result.BatchID),
					)
				}
			}
		}
		// Return an error indicating that the map phase encountered issues
		return fmt.Errorf("map phase completed with errors; all transactions rolled back")
	}

	// Commit all transactions if no errors are found
	for _, result := range results {
		if result.Tx != nil {
			// Attempt to commit the transaction
			if err := result.Tx.Commit(); err != nil {
				// Log an error if the commit fails
				mp.Logger.Error("Failed to commit transaction",
					zap.Int("Worker ID", result.BatchID),
					zap.Error(err),
				)
				// Return an error indicating that a commit failed
				return fmt.Errorf("failed to commit transaction for batch %d: %w", result.BatchID, err)
			}

			// Log success if the commit completes
			mp.Logger.Info("Transaction committed successfully",
				zap.Int("Worker ID", result.BatchID),
			)
		}
	}
	// Log a summary indicating all transactions were committed successfully
	mp.Logger.Info("All transactions committed successfully")
	return nil
}

