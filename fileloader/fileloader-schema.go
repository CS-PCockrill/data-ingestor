package fileloader

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
	"github.com/xuri/excelize/v2"
	"strings"
)

// StreamDecodeFileWithSchema is the streaming equivalent of DecodeFile for MapReduce.
// It streams the file content into a channel record-by-record.
//
// Parameters:
// - filePath: The path to the file to stream.
// - recordChan: A channel to send the streamed records.
// - modelName: The name of the model to map the file content to.
//
// Returns:
// - An error if streaming or file processing fails.
func (l *LoaderFunctions) StreamDecodeFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string) error {
	// Log the start of the streaming process
	l.Logger.Info("Starting file streaming", zap.String("filePath", filePath), zap.String("modelName", modelName))

	// Detect the file type (JSON or XML)
	fileType, err := l.detectFileType(filePath)
	if err != nil {
		// Log and return the error if file type detection fails
		l.Logger.Error("Failed to detect file type", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to detect file type: %w", err)
	}

	// Process the file based on its type
	switch fileType {
	case "json":
		return l.StreamJSONFileWithSchema(filePath, recordChan)
	case "xml":
		return l.StreamXMLFileWithSchema(filePath, recordChan, modelName)
	default:
		// Log and return the error for unsupported file types
		l.Logger.Error("Unsupported file type", zap.String("filePath", filePath), zap.String("fileType", fileType))
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}


func (l *LoaderFunctions) StreamJSONFileWithSchema(filePath string, recordChan chan map[string]interface{}) error {
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

func (l *LoaderFunctions) StreamXMLFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string) error {
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
			// Parse the <Record> element into a map
			record, err := ParseXMLElementWithFlattening(decoder, se)
			if err != nil {
				return fmt.Errorf("failed to parse <Record>: %w", err)
			}

			l.Logger.Info("Extracted Record", zap.Any("Record", record))
			for _, rec := range record {
				recordChan <- rec
			}
		}
	}

	l.Logger.Info("Finished streaming XML file", zap.String("filePath", filePath))
	return nil
}

func ParseXMLElementWithFlattening(decoder *xml.Decoder, start xml.StartElement) ([]map[string]interface{}, error) {
	baseRecord := make(map[string]interface{})
	var repeatedFields []map[string]interface{}

	// Decode the XML element
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}

		switch t := token.(type) {
		case xml.StartElement:
			// Parse nested elements
			if t.Name.Local == "fnumbers" {
				repeatedField := make(map[string]interface{})
				if err := decoder.DecodeElement(&repeatedField, &t); err != nil {
					return nil, fmt.Errorf("failed to decode <fnumbers>: %w", err)
				}
				repeatedFields = append(repeatedFields, repeatedField)
			} else {
				// Decode non-repeated fields
				var value string
				if err := decoder.DecodeElement(&value, &t); err != nil {
					return nil, fmt.Errorf("failed to decode element %s: %w", t.Name.Local, err)
				}
				baseRecord[t.Name.Local] = value
			}

		case xml.EndElement:
			if t.Name.Local == "Record" {
				// End of the <Record> element
				if len(repeatedFields) > 0 {
					// Flatten repeated fields
					var results []map[string]interface{}
					for _, repeatedField := range repeatedFields {
						flattenedRecord := make(map[string]interface{})
						// Copy base fields
						for k, v := range baseRecord {
							flattenedRecord[k] = v
						}
						// Add repeated field
						for k, v := range repeatedField {
							flattenedRecord[k] = v
						}
						results = append(results, flattenedRecord)
					}
					return results, nil
				}
				return []map[string]interface{}{baseRecord}, nil
			}
		}
	}
}

func (l *LoaderFunctions) FlattenXMLToMaps(filePath string) ([]map[string]string, error) {
	// Open the XML file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open XML file: %w", err)
	}

	decoder := xml.NewDecoder(file)
	var records []map[string]string

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
			flattenedRecords, err := l.ParseAndFlattenXMLElement(decoder, se)
			if err != nil {
				return nil, fmt.Errorf("failed to parse <Record>: %w", err)
			}
			records = append(records, flattenedRecords...)
		}
	}
	return records, nil
}

func (l *LoaderFunctions) ParseAndFlattenXMLElement(decoder *xml.Decoder, start xml.StartElement) ([]map[string]string, error) {
	baseRecord := make(map[string]string) // Holds flat fields
	nestedRecords := []map[string]string{}

	// Helper to create a new record by merging baseRecord and additional fields
	mergeRecord := func(extraFields map[string]string) map[string]string {
		newRecord := make(map[string]string)
		for k, v := range baseRecord {
			newRecord[k] = v
		}
		for k, v := range extraFields {
			newRecord[k] = v
		}
		return newRecord
	}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch t := token.(type) {
		case xml.StartElement:
			// Handle nested elements
			var elementData map[string]string
			if err := decoder.DecodeElement(&elementData, &t); err != nil {
				return nil, fmt.Errorf("failed to decode nested element: %w", err)
			}

			// If nested element is a list, treat it as a repeated record
			if t.Name.Local == "fnumbers" {
				nestedRecords = append(nestedRecords, elementData)
			} else {
				// Add flat fields to the base record
				baseRecord[t.Name.Local] = elementData[t.Name.Local]
			}

		case xml.CharData:
			// Add text content to the current base record
			content := strings.TrimSpace(string(t))
			if len(content) > 0 {
				currentKey := start.Name.Local
				baseRecord[currentKey] = content
			}

		case xml.EndElement:
			if t.Name.Local == "Record" {
				// If no nested records, return just the base record
				if len(nestedRecords) == 0 {
					return []map[string]string{baseRecord}, nil
				}

				// Merge base record with each nested record
				var results []map[string]string
				for _, nested := range nestedRecords {
					results = append(results, mergeRecord(nested))
				}
				return results, nil
			}
		}
	}

	return nil, errors.New("unexpected end of XML while parsing <Record>")
}



func (l *LoaderFunctions) ExportToJSON(records []map[string]string, outputPath string) error {
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

func (l *LoaderFunctions) ExportToCSV(records []map[string]string, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if len(records) > 0 {
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
				row = append(row, record[header])
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
	}
	fmt.Printf("Successfully exported to CSV: %s\n", outputPath)
	return nil
}

func (l *LoaderFunctions) ExportToExcel(records []map[string]string, outputPath string) error {
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
