package fileloader

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
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