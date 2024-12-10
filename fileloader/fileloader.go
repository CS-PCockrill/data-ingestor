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

type LoaderFunctionsInterface interface {
	DecodeFile(filePath, modelName string) ([]interface{}, error)
	StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error
}

type LoaderFunctions struct {
	CONFIG *config.Config
	Logger *zap.Logger
}

var _ LoaderFunctionsInterface = (*LoaderFunctions)(nil)

func (l *LoaderFunctions) DecodeFile(filePath, modelName string) ([]interface{}, error) {
	return l.createModel(modelName, filePath)
}

// StreamDecodeFile is the streaming equivalent of DecodeFile for MapReduce.
func (l *LoaderFunctions) StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error {
	fileType, err := l.detectFileType(filePath)
	if err != nil {
		return fmt.Errorf("failed to detect file type: %w", err)
	}

	switch fileType {
	case "json":
		return l.StreamJSONFile(filePath, recordChan, modelName)
	case "xml":
		return l.StreamXMLFile(filePath, recordChan, modelName)
	default:
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// StreamJSONFile streams records from a JSON file.
func (l *LoaderFunctions) StreamJSONFile(filePath string, recordChan chan interface{}, modelName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open JSON file: %w", err)
	}

	decoder := json.NewDecoder(file)
	if modelName == "Records" {
		// Top-level array
		var records []models.Record
		if err := decoder.Decode(&records); err != nil {
			return fmt.Errorf("failed to decode JSON: %w", err)
		}
		for _, record := range records {
			recordChan <- record
		}
	} else {
		// Individual objects
		for decoder.More() {
			var record models.Record
			if err := decoder.Decode(&record); err != nil {
				return fmt.Errorf("failed to decode JSON record: %w", err)
			}
			recordChan <- record
		}
	}
	return nil
}

// StreamXMLFile streams records from an XML file.
func (l *LoaderFunctions) StreamXMLFile(filePath string, recordChan chan interface{}, modelName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open XML file: %w", err)
	}

	decoder := xml.NewDecoder(file)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			fmt.Printf("IO-EOF for file: %v", filePath)
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read XML token: %w", err)
		}

		// FIXME: Record is hardcoded and should be mapped to a config variable/input parameter
		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			var record models.Record
			if err := decoder.DecodeElement(&record, &se); err != nil {
				return fmt.Errorf("failed to decode XML record: %w", err)
			}
			fmt.Printf("Record -- %v", record)

			recordChan <- record
		}
	}
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

// StreamFile opens the specified file and streams records into the provided channel.
// This function supports both XML and JSON file types.
//
// Parameters:
//   - filePath: The path to the input file.
//   - recordChan: A channel to stream parsed records.
//
// Returns:
//   - An error if file opening or streaming fails.
func (l *LoaderFunctions) StreamFile(filePath string, recordChan chan interface{}) error {
	l.Logger.Info("Starting file streaming", zap.String("filePath", filePath))

	// Detect the file type (XML or JSON)
	fileType, _ := l.detectFileType(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open file: %w", err)
	}

	// Dispatch to the appropriate streaming method based on file type
	switch fileType {
	case "xml":
		l.Logger.Info("Detected XML file type", zap.String("filePath", filePath))
		return l.streamXML(file, recordChan)
	case "json":
		l.Logger.Info("Detected JSON file type", zap.String("filePath", filePath))
		return l.streamJSON(file, recordChan)
	default:
		l.Logger.Error("Unsupported file type", zap.String("fileType", fileType))
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// streamXML streams records from an XML file into the provided channel.
//
// Parameters:
//   - file: The file pointer to the open XML file.
//   - recordChan: A channel to stream parsed records.
//
// Returns:
//   - An error if XML parsing fails.
func (l *LoaderFunctions) streamXML(file *os.File, recordChan chan interface{}) error {
	decoder := xml.NewDecoder(file)
	for {
		// Read the next token from the XML
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				l.Logger.Info("Completed streaming XML file")
				close(recordChan) // Close the channel on EOF
				return nil
			}
			l.Logger.Error("Error reading XML token", zap.Error(err))
			return fmt.Errorf("error reading XML token: %w", err)
		}

		// Check if the token is a start element named "Record"
		if startElement, ok := token.(xml.StartElement); ok && startElement.Name.Local == "Record" {
			var record models.Record // Replace with your record type
			if err := decoder.DecodeElement(&record, &startElement); err != nil {
				l.Logger.Error("Error decoding XML record", zap.Error(err))
				return fmt.Errorf("error decoding record: %w", err)
			}
			l.Logger.Debug("Streaming XML record", zap.Any("record", record))
			recordChan <- record
		}
	}
}

// streamJSON streams records from a JSON file into the provided channel.
//
// Parameters:
//   - file: The file pointer to the open JSON file.
//   - recordChan: A channel to stream parsed records.
//
// Returns:
//   - An error if JSON parsing fails.
func (l *LoaderFunctions) streamJSON(file *os.File, recordChan chan interface{}) error {
	decoder := json.NewDecoder(file)

	// Ensure the top-level JSON element is an array
	tok, err := decoder.Token()
	if err != nil {
		l.Logger.Error("Error reading JSON token", zap.Error(err))
		return fmt.Errorf("error reading JSON token: %w", err)
	}
	if tok != json.Delim('[') {
		l.Logger.Error("Invalid JSON structure; expected array", zap.Any("token", tok))
		return fmt.Errorf("expected JSON array, got %v", tok)
	}

	// Stream each element of the JSON array
	for decoder.More() {
		var record models.Record // Replace with your record type
		if err := decoder.Decode(&record); err != nil {
			l.Logger.Error("Error decoding JSON record", zap.Error(err))
			return fmt.Errorf("error decoding record: %w", err)
		}
		l.Logger.Debug("Streaming JSON record", zap.Any("record", record))
		recordChan <- record
	}
	close(recordChan) // Close the channel when done
	l.Logger.Info("Completed streaming JSON file")
	return nil
}



