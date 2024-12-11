package fileloader

import (
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


func (l *LoaderFunctions) StreamJSONFileWithSchema(filePath string, recordChan chan map[string]interface{}, columns []string) error {
	// Log the start of JSON streaming
	l.Logger.Info("Streaming JSON file", zap.String("filePath", filePath))

	// Open the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open JSON file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open JSON file: %w", err)
	}

	decoder := json.NewDecoder(file)

	// Decode either an array or individual objects
	for decoder.More() {
		var record map[string]interface{}
		if err := decoder.Decode(&record); err != nil {
			l.Logger.Error("Failed to decode JSON record", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to decode JSON record: %w", err)
		}
		recordChan <- record
	}

	// Log successful completion
	l.Logger.Info("Finished streaming JSON file", zap.String("filePath", filePath))
	return nil
}

// StreamXMLFileWithSchema
//
// TODO - Can we add Tag Names which says what tag are we looking for per entry (e.g., 'Record' in test-loader.xml)
func (l *LoaderFunctions) StreamXMLFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string, columns []string) error {
	l.Logger.Info("Streaming XML file", zap.String("filePath", filePath))

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open XML file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open XML file: %w", err)
	}

	decoder := xml.NewDecoder(file)

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			l.Logger.Error("Failed to read XML token", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to read XML token: %w", err)
		}

		// Process <Record> elements
		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			// Parse and flatten the <Record> element
			flattenedRecords, err := l.ParseAndFlattenXMLElementWithColumns(decoder, se, columns)
			if err != nil {
				return fmt.Errorf("failed to parse <Record>: %w", err)
			}

			l.Logger.Info("Extracted Record(s)", zap.Any("Record", flattenedRecords))
			for _, rec := range flattenedRecords {
				recordChan <- rec
			}
		}
	}

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

func (l *LoaderFunctions) ParseAndFlattenXMLElementWithColumns(decoder *xml.Decoder, start xml.StartElement, columns []string) ([]map[string]interface{}, error) {
	var nestedRecords []map[string]interface{}
	var resultRows []map[string]interface{}

	// Validate columns against extracted keys
	columnSet := make(map[string]bool)
	for _, col := range columns {
		columnSet[col] = true
	}

	// Recursive function to parse nested XML elements
	var parseElement func(start xml.StartElement) (map[string]interface{}, error)
	parseElement = func(start xml.StartElement) (map[string]interface{}, error) {
		flatRecord := make(map[string]interface{})
		currentKey := start.Name.Local // Current element name

		for {
			token, err := decoder.Token()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("error reading token: %w", err)
			}

			switch t := token.(type) {
			case xml.StartElement:
				// Recursively parse nested elements
				nested, err := parseElement(t)
				if err != nil {
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
				// Store character data as the value for the current element
				content := strings.TrimSpace(string(t))
				if content != "" {
					flatRecord[currentKey] = content
				}

			case xml.EndElement:
				// Break out when the current element ends
				if t.Name.Local == currentKey {
					return flatRecord, nil
				}
			}
		}
		return flatRecord, nil
	}

	// Parse the starting <Record> element
	record, err := parseElement(start)
	if err != nil {
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
				// Append the new row directly to the result rows
				resultRows = append(resultRows, flattened)
			}
		}
	}

	// If no nested repeated elements, add the base record as a single row
	nestedRecords = append(nestedRecords, resultRows...)

	l.Logger.Info("Nested Records", zap.Any("Records", nestedRecords), zap.Any("Column Set", columnSet))
	// Ensure keys are flat and validate against columns
	for i, record := range nestedRecords {
		flat := make(map[string]interface{})
		for k, v := range record {
			if nestedMap, ok := v.(map[string]interface{}); ok {
				for nestedKey, nestedValue := range nestedMap {
					if columnSet[nestedKey] {
						flat[nestedKey] = nestedValue // Flatten nested map into parent map
					}
				}
			} else if columnSet[k] {
				flat[k] = v // Retain non-nested fields as-is
			}
		}
		nestedRecords[i] = flat
	}

	return nestedRecords, nil
}


//func (l *LoaderFunctions) ParseAndFlattenXMLElement(decoder *xml.Decoder, start xml.StartElement) ([]map[string]interface{}, error) {
//	var nestedRecords []map[string]interface{}
//	var resultRows []map[string]interface{}
//
//	// Recursive function to parse nested XML elements
//	var parseElement func(start xml.StartElement) (map[string]interface{}, error)
//	parseElement = func(start xml.StartElement) (map[string]interface{}, error) {
//		flatRecord := make(map[string]interface{})
//		currentKey := start.Name.Local // Current element name
//
//		for {
//			token, err := decoder.Token()
//			if err == io.EOF {
//				break
//			}
//			if err != nil {
//				return nil, fmt.Errorf("error reading token: %w", err)
//			}
//
//			switch t := token.(type) {
//			case xml.StartElement:
//				// Recursively parse nested elements
//				nested, err := parseElement(t)
//				if err != nil {
//					return nil, err
//				}
//				// Handle repeated elements by storing them as slices
//				if existing, exists := flatRecord[t.Name.Local]; exists {
//					if slice, ok := existing.([]map[string]interface{}); ok {
//						flatRecord[t.Name.Local] = append(slice, nested)
//					} else {
//						flatRecord[t.Name.Local] = []map[string]interface{}{existing.(map[string]interface{}), nested}
//					}
//				} else {
//					flatRecord[t.Name.Local] = nested
//				}
//
//			case xml.CharData:
//				// Store character data as the value for the current element
//				content := strings.TrimSpace(string(t))
//				if content != "" {
//					flatRecord[currentKey] = content
//				}
//
//			case xml.EndElement:
//				// Break out when the current element ends
//				if t.Name.Local == currentKey {
//					return flatRecord, nil
//				}
//			}
//		}
//		return flatRecord, nil
//	}
//
//	// Parse the starting <Record> element
//	record, err := parseElement(start)
//	if err != nil {
//		return nil, fmt.Errorf("failed to parse <Record>: %w", err)
//	}
//
//	// Dynamically handle all nested repeated elements by creating new rows
//	for key, value := range record {
//		if nestedSlice, ok := value.([]map[string]interface{}); ok {
//			// For each nested element, create a new row
//			for _, nested := range nestedSlice {
//				flattened := make(map[string]interface{})
//				// Copy base fields to the new row
//				for k, v := range record {
//					if k != key { // Skip the repeated element field itself
//						flattened[k] = v
//					}
//				}
//				// Add the fields from the nested element
//				for k, v := range nested {
//					flattened[k] = v
//				}
//				// Append the new row directly to the result rows
//				resultRows = append(resultRows, flattened)
//			}
//		}
//	}
//
//	// If no nested repeated elements, add the base record as a single row
//	nestedRecords = append(nestedRecords, resultRows...)
//
//	// Ensure keys are flat (remove nested maps)
//	for i, record := range nestedRecords {
//		flat := make(map[string]interface{})
//		for k, v := range record {
//			if nestedMap, ok := v.(map[string]interface{}); ok {
//				for nestedKey, nestedValue := range nestedMap {
//					flat[nestedKey] = nestedValue // Flatten nested map into parent map
//				}
//			} else {
//				flat[k] = v // Retain non-nested fields as-is
//			}
//		}
//		nestedRecords[i] = flat
//	}
//
//	return nestedRecords, nil
//}


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

//func (l *LoaderFunctions) ExportToCSV(records []map[string]interface{}, outputPath string) error {
//	file, err := os.Create(outputPath)
//	if err != nil {
//		return fmt.Errorf("failed to create CSV file: %w", err)
//	}
//
//	writer := csv.NewWriter(file)
//	defer writer.Flush()
//
//	// Write headers
//	if len(records) > 0 {
//		headers := []string{}
//		for key := range records[0] {
//			headers = append(headers, key)
//		}
//		if err := writer.Write(headers); err != nil {
//			return fmt.Errorf("failed to write CSV headers: %w", err)
//		}
//
//		// Write rows
//		for _, record := range records {
//			row := []string{}
//			for _, header := range headers {
//				row = append(row, record[header])
//			}
//			if err := writer.Write(row); err != nil {
//				return fmt.Errorf("failed to write CSV row: %w", err)
//			}
//		}
//	}
//	fmt.Printf("Successfully exported to CSV: %s\n", outputPath)
//	return nil
//}

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
