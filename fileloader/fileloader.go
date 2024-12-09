package fileloader

import (
	"data-ingestor/config"
	"data-ingestor/models"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
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
}

var _ LoaderFunctionsInterface = (*LoaderFunctions)(nil)

func (l *LoaderFunctions) DecodeFile(filePath, modelName string) ([]interface{}, error) {
	return l.createModel(modelName, filePath)
}

// StreamDecodeFile is the streaming equivalent of DecodeFile for MapReduce.
func (l *LoaderFunctions) StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error {
	fileType, err := detectFileType(filePath)
	if err != nil {
		return fmt.Errorf("failed to detect file type: %w", err)
	}

	switch fileType {
	case "json":
		return StreamJSONFile(filePath, recordChan, modelName)
	case "xml":
		return StreamXMLFile(filePath, recordChan, modelName)
	default:
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// StreamJSONFile streams records from a JSON file.
func StreamJSONFile(filePath string, recordChan chan interface{}, modelName string) error {
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
func StreamXMLFile(filePath string, recordChan chan interface{}, modelName string) error {
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


func (l *LoaderFunctions) createModel(modelName string, filePath string) ([]interface{}, error) {
	// Detect file type (JSON or XML)
	fileType, err := detectFileType(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to detect file type: %w", err)
	}

	// Define a container for the parsed records
	var records []interface{}

	switch modelName {
	case "MistAMS":
		// Top-level "Data" model
		var data models.Data
		if err := unmarshalFile(filePath, fileType, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal file: %w", err)
		}
		// Convert records to []interface{} for MapReduce
		for _, record := range data.Records {
			records = append(records, record)
		}

	case "Record":
		if fileType == "xml" {
			// Parse consecutive <Record> elements (XML only)
			rawRecords, err := l.parseXMLConsecutiveRecords(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to parse consecutive records: %w", err)
			}
			// Convert to []interface{}
			for _, record := range rawRecords {
				records = append(records, record)
			}
		} else if fileType == "json" {
			// Directly parse an array of records (JSON only)
			rawRecords, err := parseJSONArray(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to parse JSON array: %w", err)
			}
			// Convert to []interface{}
			for _, record := range rawRecords {
				records = append(records, record)
			}
		} else {
			return nil, fmt.Errorf("unsupported file type for 'Record': %s", fileType)
		}

	default:
		return nil, fmt.Errorf("unknown model type: %s", modelName)
	}

	return records, nil
}

func unmarshalFile(filePath, fileType string, v interface{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	switch fileType {
	case "json":
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(v); err != nil {
			return fmt.Errorf("failed to decode JSON file: %w", err)
		}
	case "xml":
		decoder := xml.NewDecoder(file)
		if err := decoder.Decode(v); err != nil {
			return fmt.Errorf("failed to decode XML file: %w", err)
		}
	default:
		return fmt.Errorf("unsupported file type: %s", fileType)
	}

	return nil
}

// parseXMLConsecutiveRecords Parses consecutive <Record> elements in an XML file
func (l *LoaderFunctions) parseXMLConsecutiveRecords(filePath string) ([]models.Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
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
			return nil, fmt.Errorf("error reading XML: %w", err)
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
			var record models.Record
			if err := decoder.DecodeElement(&record, &se); err != nil {
				return nil, fmt.Errorf("error decoding record: %w", err)
			}
			records = append(records, record)
		}
	}
	return records, nil
}


func parseJSONArray(filePath string) ([]models.Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	var rawRecords []models.Record
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&rawRecords); err != nil {
		return nil, fmt.Errorf("failed to decode JSON array: %w", err)
	}

	return rawRecords, nil
}

// detectFileType detects whether the file is JSON or XML based on the extension or content.
func detectFileType(filePath string) (string, error) {
	if strings.HasSuffix(filePath, ".json") {
		return "json", nil
	} else if strings.HasSuffix(filePath, ".xml") {
		return "xml", nil
	}
	return "", errors.New("unsupported file format: must be .json or .xml")
}

// StreamFile opens the file and streams records one by one
func StreamFile(filePath string, recordChan chan interface{}) error {
	fileType, _ := detectFileType(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	//defer file.Close()

	switch fileType {
	case "xml":
		return streamXML(file, recordChan)
	case "json":
		return streamJSON(file, recordChan)
	default:
		return fmt.Errorf("unsupported file type: %s", fileType)
	}
}

func streamXML(file *os.File, recordChan chan interface{}) error {
	decoder := xml.NewDecoder(file)
	for {
		token, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				close(recordChan)
				return nil
			}
			return fmt.Errorf("error reading XML token: %w", err)
		}

		if startElement, ok := token.(xml.StartElement); ok && startElement.Name.Local == "Record" {
			var record models.Record // Replace with your record type
			if err := decoder.DecodeElement(&record, &startElement); err != nil {
				return fmt.Errorf("error decoding record: %w", err)
			}
			recordChan <- record
		}
	}
}

func streamJSON(file *os.File, recordChan chan interface{}) error {
	decoder := json.NewDecoder(file)

	tok, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading JSON token: %w", err)
	}
	if tok != json.Delim('[') {
		return fmt.Errorf("expected JSON array, got %v", tok)
	}

	for decoder.More() {
		var record models.Record // Replace with your record type
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("error decoding record: %w", err)
		}
		recordChan <- record
	}
	close(recordChan)
	return nil
}


