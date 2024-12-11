package dbtransposer

import (
	"database/sql"
	"fmt"
	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
	"strings"
)

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

	mp.Logger.Info("Extracted SQL Data (From Excel)",
		zap.Any("templateFile", "db-template.xlsx"),
		zap.Any("placeholderCount", placeholderCount),
		zap.Any("columns", columns))

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
	// Initialize columns and rows
	columns := []string{}
	rows := [][]interface{}{}

	// Flatten the record into columns and values
	row := []interface{}{}
	for key, value := range record {
		// Append the column name and value
		columns = append(columns, fmt.Sprintf(`"%s"`, key))
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

	// Process the specified line to determine placeholders
	if line <= len(rows) {
		for _, cell := range rows[line-1] { // Adjusting for 0-based index
			if cell != "" {
				columns = append(columns, cell)
				placeholderCount++
			}
		}
	}

	// Log the placeholder count
	mp.Logger.Info("Determined Columns and Count from Excel",
		zap.String("filePath", filePath),
		zap.String("sheetName", sheetName),
		zap.String("rangeSpec", rangeSpec),
		zap.Int("line", line),
		zap.Strings("columns", columns),
		zap.Int("placeholderCount", placeholderCount),
	)

	return columns, placeholderCount, nil
}

