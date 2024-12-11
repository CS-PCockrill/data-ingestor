package fileloader

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
)

// StreamDecodeFileWithSchema streams the file content record-by-record, applying a schema for column mapping.
//
// Parameters:
// - filePath: The path to the file to stream.
// - recordChan: A channel to send the streamed records.
// - modelName: The name of the model to map the file content to.
// - columns: A list of column names defining the structure to map the file's content.
//
// Returns:
// - An error if streaming or file processing fails.
func (l *LoaderFunctions) StreamDecodeFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string, columns []string) error {
	// Log the start of the streaming process
	l.Logger.Info("Starting file streaming with schema",
		zap.String("filePath", filePath),
		zap.String("modelName", modelName),
		zap.Strings("columns", columns),
	)

	// Detect the file type (JSON or XML)
	fileType, err := l.detectFileType(filePath)
	if err != nil {
		// Log and return the error if file type detection fails
		l.Logger.Error("Failed to detect file type",
			zap.String("filePath", filePath),
			zap.Error(err),
		)
		return fmt.Errorf("failed to detect file type: %w", err)
	}

	// Process the file based on its type
	switch fileType {
	case "json":
		return l.StreamJSONFileWithSchema(filePath, recordChan, columns)
	case "xml":
		return l.StreamXMLFileWithSchema(filePath, recordChan, modelName, columns)
	default:
		// Log and return the error for unsupported file types
		l.Logger.Error("Unsupported file type",
			zap.String("filePath", filePath),
			zap.String("fileType", fileType),
		)
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}


// StreamJSONFileWithSchema handles JSON files with a top-level key containing the records.
// Supports flattening of nested arrays within each record and validates against allowed columns.
//
// Parameters:
// - filePath: The path to the JSON file to be streamed.
// - recordChan: A channel to send the streamed records.
// - columns: A slice of allowed column names to validate the keys.
//
// Returns:
// - An error if streaming or JSON processing fails.
func (l *LoaderFunctions) StreamJSONFileWithSchema(filePath string, recordChan chan map[string]interface{}, columns []string) error {
	// Log the start of JSON streaming
	l.Logger.Info("Starting JSON streaming for file with top-level key", zap.String("filePath", filePath))

	// Open the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open JSON file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open JSON file: %w", err)
	}
	//defer file.Close() // Ensure file closure

	l.Logger.Debug("Loaded allowed columns for validation", zap.Strings("columns", columns))

	// Initialize JSON decoder
	decoder := json.NewDecoder(file)

	// Decode the top-level JSON structure
	var topLevel map[string]interface{}
	if err := decoder.Decode(&topLevel); err != nil {
		l.Logger.Error("Failed to decode top-level JSON structure", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to decode top-level JSON structure: %w", err)
	}

	// Extract the array under the "Records" key (FIXME: Records is a placeholder, change to however the JSON files are structured to get to the list of records)
	records, ok := topLevel["Records"].([]interface{})
	if !ok {
		l.Logger.Error("Top-level key 'Records' is missing or not an array", zap.String("filePath", filePath))
		return fmt.Errorf("top-level key 'Records' is missing or not an array")
	}

	// Process each record in the "Records" array
	for _, record := range records {
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			l.Logger.Warn("Skipping non-object element in 'Records' array", zap.Any("element", record))
			continue
		}

		nestedRows, baseRecord := l.ParseAndFlattenJSONElement(recordMap, columns)

		// If no nested rows, send the base record as-is
		if len(nestedRows) == 0 {
			l.Logger.Debug("Streaming base record", zap.Any("record", baseRecord))
			recordChan <- baseRecord
		} else {
			// Stream each row generated from nested elements
			for _, row := range nestedRows {
				l.Logger.Debug("Streaming flattened row", zap.Any("row", row))
				recordChan <- row
			}
		}
	}

	// Log successful completion
	l.Logger.Info("Finished streaming JSON file with top-level key", zap.String("filePath", filePath))
	return nil
}


// StreamXMLFileWithSchema streams records from an XML file, processing and flattening them according to the provided schema.
// This function dynamically handles nested elements and validates extracted fields against the specified columns.
//
// Parameters:
// - filePath: The path to the XML file to be streamed.
// - recordChan: A channel to send the parsed and flattened records.
// - modelName: The name of the model being processed (currently used for contextual logging).
// - columns: A list of valid column names to validate against.
//
// Returns:
// - An error if any issues occur during file processing or parsing.
func (l *LoaderFunctions) StreamXMLFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string, columns []string) error {
	// Log the start of XML streaming
	l.Logger.Info("Starting XML streaming", zap.String("filePath", filePath), zap.String("modelName", modelName))

	// Open the XML file
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open XML file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open XML file: %w", err)
	}

	// Initialize the XML decoder
	decoder := xml.NewDecoder(file)
	l.Logger.Debug("Initialized XML decoder", zap.String("filePath", filePath))

	for {
		// Read the next XML token
		token, err := decoder.Token()
		if err == io.EOF {
			// Log EOF and exit loop when the end of the file is reached
			l.Logger.Info("Reached EOF for XML file", zap.String("filePath", filePath))
			break
		}
		if err != nil {
			// Log and return the error if token reading fails
			l.Logger.Error("Failed to read XML token", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to read XML token: %w", err)
		}

		// Check for the start of a <Record> element
		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			l.Logger.Debug("Processing <Record> element", zap.String("element", se.Name.Local))

			// Parse and flatten the <Record> element
			flattenedRecords, err := l.ParseAndFlattenXMLElementWithColumns(decoder, se, columns)
			if err != nil {
				// Log and return the error if parsing fails
				l.Logger.Error("Failed to parse <Record> element", zap.String("filePath", filePath), zap.Error(err))
				return fmt.Errorf("failed to parse <Record>: %w", err)
			}

			// Log the successfully parsed record(s)
			l.Logger.Info("Extracted Record(s)", zap.String("filePath", filePath), zap.Any("records", flattenedRecords))

			// Send each flattened record to the channel
			for _, rec := range flattenedRecords {
				l.Logger.Debug("Sending record to channel", zap.Any("record", rec))
				recordChan <- rec
			}
		}
	}

	// Log successful completion of XML streaming
	l.Logger.Info("Finished streaming XML file", zap.String("filePath", filePath))
	return nil
}


func (l *LoaderFunctions) FlattenXMLToMaps(filePath string, columns []string) ([]map[string]interface{}, error) {
	// Open the XML file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open XML file: %w", err)
	}

	decoder := xml.NewDecoder(file)
	var records []map[string]interface{}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read XML token: %w", err)
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			// Parse and flatten the <Record> element
			flattenedRecords, err := l.ParseAndFlattenXMLElementWithColumns(decoder, se, columns)
			if err != nil {
				return nil, fmt.Errorf("failed to parse <Record>: %w", err)
			}
			records = append(records, flattenedRecords...)
		}
	}
	return records, nil
}

// FlattenJSONToMaps handles JSON files with a top-level key containing the records.
// Supports flattening of nested arrays within each record and validates against allowed columns.
//
// Parameters:
// - filePath: The path to the JSON file to be streamed.
// - recordChan: A channel to send the streamed records.
// - columns: A slice of allowed column names to validate the keys.
//
// Returns:
// - An error if streaming or JSON processing fails.
func (l *LoaderFunctions) FlattenJSONToMaps(filePath string, columns []string) ([]map[string]interface{}, error) {
	// Log the start of JSON streaming
	l.Logger.Info("Starting JSON streaming for file with top-level key", zap.String("filePath", filePath))

	// Open the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open JSON file", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}
	//defer file.Close() // Ensure file closure

	l.Logger.Debug("Loaded allowed columns for validation", zap.Strings("columns", columns))

	// Initialize JSON decoder
	decoder := json.NewDecoder(file)

	// Decode the top-level JSON structure
	var topLevel map[string]interface{}
	if err := decoder.Decode(&topLevel); err != nil {
		l.Logger.Error("Failed to decode top-level JSON structure", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to decode top-level JSON structure: %w", err)
	}

	// Extract the array under the "Records" key
	records, ok := topLevel["Records"].([]interface{})
	if !ok {
		l.Logger.Error("Top-level key 'Records' is missing or not an array", zap.String("filePath", filePath))
		return nil, fmt.Errorf("top-level key 'Records' is missing or not an array")
	}

	rows := []map[string]interface{}{}
	// Process each record in the "Records" array
	for _, record := range records {
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			l.Logger.Warn("Skipping non-object element in 'Records' array", zap.Any("element", record))
			continue
		}

		nestedRows, baseRecord := l.ParseAndFlattenJSONElement(recordMap, columns)
		// If no nested rows, send the base record as-is
		if len(nestedRows) == 0 {
			l.Logger.Debug("Loading base record", zap.Any("record", baseRecord))
			rows = append(rows, baseRecord)
		} else {
			// Stream each row generated from nested elements
			rows = append(rows, nestedRows...)
		}
	}

	// Log successful completion
	l.Logger.Info("Finished loading JSON file with top-level key", zap.String("filePath", filePath))
	return rows, nil
}

func (l *LoaderFunctions) ParseAndFlattenJSONElement(recordMap map[string]interface{}, columns []string) (nestedRows []map[string]interface{}, baseRecord map[string]interface{}) {
	// Create a set for quick validation of allowed columns
	columnSet := make(map[string]struct{})
	for _, col := range columns {
		columnSet[col] = struct{}{}
	}
	l.Logger.Debug("Loaded allowed columns for validation", zap.Strings("columns", columns))

	// Initialize baseRecord to avoid nil map issues
	baseRecord = make(map[string]interface{})

	// Separate base fields and process nested arrays
	for key, value := range recordMap {
		// Validate the key against the allowed columns
		//if _, allowed := columnSet[key]; !allowed {
		//	l.Logger.Warn("Skipping unmapped key", zap.String("key", key))
		//	continue
		//}

		switch v := value.(type) {
		case []interface{}: // Handle arrays dynamically
			for _, nested := range v {
				if nestedMap, ok := nested.(map[string]interface{}); ok {
					flattenedRow := make(map[string]interface{})
					// Copy base fields to the new row
					for baseKey, baseValue := range recordMap {
						if baseKey != key { // Exclude the current array key
							// Validate the baseKey
							if _, allowed := columnSet[baseKey]; allowed {
								flattenedRow[baseKey] = baseValue
							} else {
								l.Logger.Warn("Skipping unmapped base key", zap.String("baseKey", baseKey))
							}
						}
					}
					// Add nested fields to the row
					for nestedKey, nestedValue := range nestedMap {
						if _, allowed := columnSet[nestedKey]; allowed {
							flattenedRow[nestedKey] = nestedValue
						} else {
							l.Logger.Warn("Skipping unmapped nested key", zap.String("nestedKey", nestedKey))
						}
					}
					nestedRows = append(nestedRows, flattenedRow)
				} else {
					l.Logger.Warn("Skipping unsupported nested element in array", zap.String("key", key))
				}
			}
		default:
			// Add primitive fields to the base record
			baseRecord[key] = value
		}
	}

	// Validate baseRecord keys against allowed columns
	validatedBaseRecord := make(map[string]interface{})
	for key, value := range baseRecord {
		if _, allowed := columnSet[key]; allowed {
			validatedBaseRecord[key] = value
		} else {
			l.Logger.Warn("Skipping unmapped key in base record", zap.String("key", key))
		}
	}
	baseRecord = validatedBaseRecord

	return nestedRows, baseRecord
}


// ParseAndFlattenXMLElementWithColumns parses and flattens an XML element, dynamically handling nested structures.
// It validates the extracted fields against a provided list of column names.
//
// Parameters:
// - decoder: The XML decoder to read tokens from.
// - start: The starting XML element to process.
// - columns: A list of valid column names to validate against.
//
// Returns:
// - A slice of flattened records (maps) for insertion or processing.
// - An error if any issues occur during parsing or validation.
func (l *LoaderFunctions) ParseAndFlattenXMLElementWithColumns(decoder *xml.Decoder, start xml.StartElement, columns []string) ([]map[string]interface{}, error) {
	// Initialize structures for nested and resulting rows
	var nestedRecords []map[string]interface{}
	var resultRows []map[string]interface{}

	// Create a set of valid column names for efficient validation
	columnSet := make(map[string]bool)
	for _, col := range columns {
		columnSet[col] = true
	}
	l.Logger.Debug("Initialized column validation set", zap.Strings("columns", columns))

	// Recursive function to parse nested XML elements
	var parseElement func(start xml.StartElement) (map[string]interface{}, error)
	parseElement = func(start xml.StartElement) (map[string]interface{}, error) {
		flatRecord := make(map[string]interface{})
		currentKey := start.Name.Local // Track the current XML element name

		l.Logger.Debug("Parsing XML element", zap.String("element", currentKey))

		for {
			token, err := decoder.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				l.Logger.Error("Error reading XML token", zap.Error(err), zap.String("currentKey", currentKey))
				return nil, fmt.Errorf("error reading token: %w", err)
			}

			switch t := token.(type) {
			case xml.StartElement:
				l.Logger.Debug("Encountered nested start element", zap.String("element", t.Name.Local))
				// Recursively parse nested elements
				nested, err := parseElement(t)
				if err != nil {
					l.Logger.Error("Error parsing nested element", zap.Error(err), zap.String("nestedElement", t.Name.Local))
					return nil, err
				}
				// Handle repeated elements by storing them as slices
				if existing, exists := flatRecord[t.Name.Local]; exists {
					if slice, ok := existing.([]map[string]interface{}); ok {
						flatRecord[t.Name.Local] = append(slice, nested)
					} else {
						flatRecord[t.Name.Local] = []map[string]interface{}{existing.(map[string]interface{}), nested}
					}
				} else {
					flatRecord[t.Name.Local] = nested
				}

			case xml.CharData:
				// Capture character data as the value for the current element
				content := strings.TrimSpace(string(t))
				if content != "" {
					flatRecord[currentKey] = content
					l.Logger.Debug("Captured character data", zap.String("key", currentKey), zap.String("value", content))
				}

			case xml.EndElement:
				// Return when the current element ends
				if t.Name.Local == currentKey {
					l.Logger.Debug("Completed parsing element", zap.String("element", currentKey), zap.Any("record", flatRecord))
					return flatRecord, nil
				}
			}
		}
		return flatRecord, nil
	}

	// Parse the starting <Record> element
	l.Logger.Info("Starting to parse <Record> element", zap.String("element", start.Name.Local))
	record, err := parseElement(start)
	if err != nil {
		l.Logger.Error("Failed to parse <Record> element", zap.Error(err))
		return nil, fmt.Errorf("failed to parse <Record>: %w", err)
	}

	// Dynamically handle all nested repeated elements by creating new rows
	for key, value := range record {
		if nestedSlice, ok := value.([]map[string]interface{}); ok {
			// For each nested element, create a new row
			for _, nested := range nestedSlice {
				flattened := make(map[string]interface{})
				// Copy base fields to the new row
				for k, v := range record {
					if k != key { // Skip the repeated element field itself
						flattened[k] = v
					}
				}
				// Add the fields from the nested element
				for k, v := range nested {
					flattened[k] = v
				}
				l.Logger.Debug("Generated new row for repeated element", zap.String("element", key), zap.Any("row", flattened))
				// Append the new row directly to the result rows
				resultRows = append(resultRows, flattened)
			}
		}
	}

	// If no nested repeated elements, add the base record as a single row
	if len(resultRows) == 0 {
		l.Logger.Debug("No repeated elements found, adding base record", zap.Any("record", record))
		nestedRecords = append(nestedRecords, record)
	} else {
		nestedRecords = append(nestedRecords, resultRows...)
	}

	// Validate keys against columns and flatten nested maps
	for i, record := range nestedRecords {
		flat := make(map[string]interface{})
		for k, v := range record {
			if nestedMap, ok := v.(map[string]interface{}); ok {
				for nestedKey, nestedValue := range nestedMap {
					if columnSet[nestedKey] {
						flat[nestedKey] = nestedValue
					} else {
						l.Logger.Warn("Skipping invalid nested column", zap.String("nestedKey", nestedKey))
					}
				}
			} else if columnSet[k] {
				flat[k] = v
			} else {
				l.Logger.Warn("Skipping invalid column", zap.String("key", k))
			}
		}
		nestedRecords[i] = flat
		l.Logger.Debug("Validated and flattened record", zap.Any("record", flat))
	}

	// Log final nested records
	l.Logger.Info("Completed parsing and flattening XML element", zap.Any("finalRecords", nestedRecords))
	return nestedRecords, nil
}


func (l *LoaderFunctions) ExportToJSON(records []map[string]interface{}, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(records); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}
	fmt.Printf("Successfully exported to JSON: %s\n", outputPath)
	return nil
}

func (l *LoaderFunctions) ExportToCSV(records []map[string]interface{}, outputPath string) error {
	// Create the output CSV file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}

	// Initialize the CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Check if records are available
	if len(records) == 0 {
		return fmt.Errorf("no records available to export")
	}

	// Extract and write headers
	headers := []string{}
	for key := range records[0] {
		headers = append(headers, key)
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write CSV headers: %w", err)
	}

	// Write rows
	for _, record := range records {
		row := []string{}
		for _, header := range headers {
			value, exists := record[header]
			if !exists {
				row = append(row, "")
				continue
			}

			// Convert value to string
			row = append(row, fmt.Sprintf("%v", value))
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	fmt.Printf("Successfully exported to CSV: %s\n", outputPath)
	return nil
}


func (l *LoaderFunctions) ExportToExcel(records []map[string]interface{}, outputPath string) error {
	f := excelize.NewFile()

	// Write headers and rows
	sheetName := "Sheet1"
	if len(records) > 0 {
		headers := []string{}
		for key := range records[0] {
			headers = append(headers, key)
		}
		for colIndex, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(colIndex+1, 1)
			f.SetCellValue(sheetName, cell, header)
		}

		// Write rows
		for rowIndex, record := range records {
			for colIndex, header := range headers {
				cell, _ := excelize.CoordinatesToCellName(colIndex+1, rowIndex+2)
				f.SetCellValue(sheetName, cell, record[header])
			}
		}
	}

	// Save the Excel file
	if err := f.SaveAs(outputPath); err != nil {
		return fmt.Errorf("failed to save Excel file: %w", err)
	}
	fmt.Printf("Successfully exported to Excel: %s\n", outputPath)
	return nil
}
