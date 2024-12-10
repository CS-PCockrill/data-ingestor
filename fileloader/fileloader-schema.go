package fileloader

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
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
			record, err := ParseXMLElementGenerically(decoder)
			if err != nil {
				l.Logger.Error("Failed to decode XML record", zap.String("filePath", filePath), zap.Error(err))
				return fmt.Errorf("failed to decode XML record: %w", err)
			}

			l.Logger.Info("Parsed Record", zap.Any("Record", record))

			// Check and handle slices dynamically
			emitRecords := FlattenNestedSlices(record)
			for _, emitRecord := range emitRecords {
				l.Logger.Debug("Emitting flattened record", zap.Any("record", emitRecord))
				recordChan <- emitRecord
			}
		}
	}

	l.Logger.Info("Finished streaming XML file", zap.String("filePath", filePath))
	return nil
}


func ParseXMLElementGenerically(decoder *xml.Decoder) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	stack := []string{}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read XML token: %w", err)
		}

		switch t := token.(type) {
		case xml.StartElement:
			// Push current element to stack
			stack = append(stack, t.Name.Local)
			// Prepare for nested slice
			key := strings.Join(stack, ".")
			if _, exists := result[key]; !exists {
				result[key] = []map[string]interface{}{}
			}

		case xml.CharData:
			// Handle text data for the current element
			if len(stack) > 0 {
				key := stack[len(stack)-1]
				if _, exists := result[key]; exists {
					result[key] = string(t)
				}
			}

		case xml.EndElement:
			// Pop the current element from the stack
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid elements found in the XML")
	}

	return result, nil
}

func FlattenNestedSlices(record map[string]interface{}) []map[string]interface{} {
	var results []map[string]interface{}

	// Separate non-slice fields and slice fields
	baseRecord := make(map[string]interface{})
	slices := map[string][]map[string]interface{}{}

	for key, value := range record {
		switch v := value.(type) {
		case []map[string]interface{}:
			// Handle slices
			slices[key] = v
		default:
			// Handle base fields
			baseRecord[key] = v
		}
	}

	// Generate rows for each slice
	if len(slices) > 0 {
		for sliceKey, sliceValues := range slices {
			for _, sliceValue := range sliceValues {
				flattenedRecord := make(map[string]interface{})
				// Include base fields in each row
				for k, v := range baseRecord {
					flattenedRecord[k] = v
				}
				// Include slice fields in each row
				for k, v := range sliceValue {
					flattenedRecord[fmt.Sprintf("%s.%s", sliceKey, k)] = v
				}
				results = append(results, flattenedRecord)
			}
		}
	} else {
		// If no slices, use the base record as a single row
		results = append(results, baseRecord)
	}

	return results
}
