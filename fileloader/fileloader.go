package fileloader

import (
	"data-ingestor/config"
	"data-ingestor/models"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
)

// LoaderFunctionsInterface defines the methods for decoding and streaming files.
type LoaderFunctionsInterface interface {
	DecodeFile(filePath, modelName string) ([]interface{}, error)
	StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error
}

// LoaderFunctions provides the implementation of LoaderFunctionsInterface.
type LoaderFunctions struct {
	CONFIG *config.Config // Application configuration
	Logger *zap.Logger    // Structured logger
}

// Ensure LoaderFunctions implements LoaderFunctionsInterface.
var _ LoaderFunctionsInterface = (*LoaderFunctions)(nil)

// DecodeFile decodes the entire file into a list of records.
//
// Parameters:
//   - filePath: Path to the input file.
//   - modelName: The model name ("Records" or "MistAMS").
//
// Returns:
//   - A slice of records as []interface{}, or an error if decoding fails.
func (l *LoaderFunctions) DecodeFile(filePath, modelName string) ([]interface{}, error) {
	l.Logger.Info("Decoding file", zap.String("filePath", filePath), zap.String("modelName", modelName))
	return l.createModel(modelName, filePath)
}

// StreamDecodeFile streams records from a file for MapReduce.
//
// Parameters:
//   - filePath: Path to the input file.
//   - recordChan: Channel to send the streamed records.
//   - modelName: The model name ("Records" or "MistAMS").
//
// Returns:
//   - An error if streaming fails.
func (l *LoaderFunctions) StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error {
	l.Logger.Info("Starting to stream decode file", zap.String("filePath", filePath), zap.String("modelName", modelName))

	fileType, err := l.detectFileType(filePath)
	if err != nil {
		l.Logger.Error("Failed to detect file type", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to detect file type: %w", err)
	}

	switch fileType {
	case "json":
		return l.StreamJSONFile(filePath, recordChan, modelName)
	case "xml":
		return l.StreamXMLFile(filePath, recordChan, modelName)
	default:
		l.Logger.Error("Unsupported file type", zap.String("fileType", fileType))
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// StreamJSONFile streams records from a JSON file.
//
// Parameters:
//   - filePath: Path to the input JSON file.
//   - recordChan: Channel to send the streamed records.
//   - modelName: The model name ("Records" or "MistAMS").
//
// Returns:
//   - An error if streaming fails.
func (l *LoaderFunctions) StreamJSONFile(filePath string, recordChan chan interface{}, modelName string) error {
	l.Logger.Info("Streaming JSON file", zap.String("filePath", filePath), zap.String("modelName", modelName))

	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open JSON file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open JSON file: %w", err)
	}

	decoder := json.NewDecoder(file)
	if modelName == "Records" {
		// Decode top-level array
		var records []models.Record
		if err := decoder.Decode(&records); err != nil {
			l.Logger.Error("Failed to decode JSON array", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to decode JSON: %w", err)
		}
		for _, record := range records {
			recordChan <- record
		}
	} else {
		// Decode individual objects
		for decoder.More() {
			var record models.Record
			if err := decoder.Decode(&record); err != nil {
				l.Logger.Error("Failed to decode JSON record", zap.String("filePath", filePath), zap.Error(err))
				return fmt.Errorf("failed to decode JSON record: %w", err)
			}
			recordChan <- record
		}
	}
	close(recordChan)
	l.Logger.Info("Finished streaming JSON file", zap.String("filePath", filePath))
	return nil
}

// StreamXMLFile streams records from an XML file.
//
// Parameters:
//   - filePath: Path to the input XML file.
//   - recordChan: Channel to send the streamed records.
//   - modelName: The model name ("Records" or "MistAMS").
//
// Returns:
//   - An error if streaming fails.
func (l *LoaderFunctions) StreamXMLFile(filePath string, recordChan chan interface{}, modelName string) error {
	l.Logger.Info("Streaming XML file", zap.String("filePath", filePath), zap.String("modelName", modelName))

	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open XML file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open XML file: %w", err)
	}

	decoder := xml.NewDecoder(file)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			l.Logger.Info("End of XML file reached", zap.String("filePath", filePath))
			break
		}
		if err != nil {
			l.Logger.Error("Error reading XML token", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to read XML token: %w", err)
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			var record models.Record
			if err := decoder.DecodeElement(&record, &se); err != nil {
				l.Logger.Error("Failed to decode XML record", zap.Error(err))
				return fmt.Errorf("failed to decode XML record: %w", err)
			}
			recordChan <- record
		}
	}
	close(recordChan)
	l.Logger.Info("Finished streaming XML file", zap.String("filePath", filePath))
	return nil
}


// createModel processes the specified file and creates a list of parsed records based on the model name.
//
// Parameters:
//   - modelName: The name of the model to parse ("MistAMS" or "Record").
//   - filePath: Path to the input file.
//
// Returns:
//   - A slice of records as []interface{}, or an error if parsing fails.
func (l *LoaderFunctions) createModel(modelName string, filePath string) ([]interface{}, error) {
	l.Logger.Info("Creating model from file", zap.String("modelName", modelName), zap.String("filePath", filePath))

	// Detect file type (JSON or XML)
	fileType, err := l.detectFileType(filePath)
	if err != nil {
		l.Logger.Error("Failed to detect file type", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to detect file type: %w", err)
	}

	var records []interface{}

	switch modelName {
	case "MistAMS":
		// Top-level "Data" model
		l.Logger.Info("Processing MistAMS model", zap.String("filePath", filePath))
		var data models.Data
		if err := l.unmarshalFile(filePath, fileType, &data); err != nil {
			l.Logger.Error("Failed to unmarshal file for MistAMS", zap.String("filePath", filePath), zap.Error(err))
			return nil, fmt.Errorf("failed to unmarshal file: %w", err)
		}
		// Convert records to []interface{} for MapReduce
		for _, record := range data.Records {
			records = append(records, record)
		}

	case "Record":
		l.Logger.Info("Processing Record model", zap.String("filePath", filePath))
		if fileType == "xml" {
			// Parse consecutive <Record> elements (XML only)
			rawRecords, err := l.parseXMLConsecutiveRecords(filePath)
			if err != nil {
				l.Logger.Error("Failed to parse consecutive XML records", zap.String("filePath", filePath), zap.Error(err))
				return nil, fmt.Errorf("failed to parse consecutive records: %w", err)
			}
			for _, record := range rawRecords {
				records = append(records, record)
			}
		} else if fileType == "json" {
			// Directly parse an array of records (JSON only)
			rawRecords, err := l.parseJSONArray(filePath)
			if err != nil {
				l.Logger.Error("Failed to parse JSON array", zap.String("filePath", filePath), zap.Error(err))
				return nil, fmt.Errorf("failed to parse JSON array: %w", err)
			}
			for _, record := range rawRecords {
				records = append(records, record)
			}
		} else {
			l.Logger.Error("Unsupported file type for Record model", zap.String("fileType", fileType))
			return nil, fmt.Errorf("unsupported file type for 'Record': %s", fileType)
		}

	default:
		l.Logger.Error("Unknown model type", zap.String("modelName", modelName))
		return nil, fmt.Errorf("unknown model type: %s", modelName)
	}

	l.Logger.Info("Successfully created model", zap.String("modelName", modelName), zap.Int("recordCount", len(records)))
	return records, nil
}

// unmarshalFile unmarshals the contents of a file into the provided struct.
//
// Parameters:
//   - filePath: Path to the input file.
//   - fileType: Type of the file ("json" or "xml").
//   - v: Pointer to the target struct for unmarshalling.
//
// Returns:
//   - An error if unmarshalling fails.
func (l *LoaderFunctions) unmarshalFile(filePath, fileType string, v interface{}) error {
	l.Logger.Info("Unmarshalling file", zap.String("filePath", filePath), zap.String("fileType", fileType))

	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open file: %w", err)
	}

	switch fileType {
	case "json":
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(v); err != nil {
			l.Logger.Error("Failed to decode JSON file", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to decode JSON file: %w", err)
		}
	case "xml":
		decoder := xml.NewDecoder(file)
		if err := decoder.Decode(v); err != nil {
			l.Logger.Error("Failed to decode XML file", zap.String("filePath", filePath), zap.Error(err))
			return fmt.Errorf("failed to decode XML file: %w", err)
		}
	default:
		l.Logger.Error("Unsupported file type", zap.String("fileType", fileType))
		return fmt.Errorf("unsupported file type: %s", fileType)
	}

	l.Logger.Info("Successfully unmarshalled file", zap.String("filePath", filePath))
	return nil
}

// parseXMLConsecutiveRecords parses consecutive <Record> elements from an XML file.
//
// Parameters:
//   - filePath: Path to the input XML file.
//
// Returns:
//   - A slice of parsed records, or an error if parsing fails.
func (l *LoaderFunctions) parseXMLConsecutiveRecords(filePath string) ([]models.Record, error) {
	l.Logger.Info("Parsing consecutive XML records", zap.String("filePath", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	var records []models.Record
	decoder := xml.NewDecoder(file)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			l.Logger.Error("Error reading XML token", zap.String("filePath", filePath), zap.Error(err))
			return nil, fmt.Errorf("error reading XML: %w", err)
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			var record models.Record
			if err := decoder.DecodeElement(&record, &se); err != nil {
				l.Logger.Error("Error decoding XML record", zap.Error(err))
				return nil, fmt.Errorf("error decoding record: %w", err)
			}
			records = append(records, record)
		}
	}

	l.Logger.Info("Successfully parsed XML records", zap.String("filePath", filePath), zap.Int("recordCount", len(records)))
	return records, nil
}

// parseJSONArray parses an array of records from a JSON file.
//
// Parameters:
//   - filePath: Path to the input JSON file.
//
// Returns:
//   - A slice of parsed records, or an error if parsing fails.
func (l *LoaderFunctions) parseJSONArray(filePath string) ([]models.Record, error) {
	l.Logger.Info("Parsing JSON array", zap.String("filePath", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	var rawRecords []models.Record
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&rawRecords); err != nil {
		l.Logger.Error("Error decoding JSON array", zap.String("filePath", filePath), zap.Error(err))
		return nil, fmt.Errorf("failed to decode JSON array: %w", err)
	}

	l.Logger.Info("Successfully parsed JSON records", zap.String("filePath", filePath), zap.Int("recordCount", len(rawRecords)))
	return rawRecords, nil
}


// detectFileType detects whether the file is JSON or XML based on the extension or content.
func (l *LoaderFunctions) detectFileType(filePath string) (string, error) {
	if strings.HasSuffix(filePath, ".json") {
		return "json", nil
	} else if strings.HasSuffix(filePath, ".xml") {
		return "xml", nil
	}

	return "", errors.New("unsupported file format: must be .json or .xml")
}



