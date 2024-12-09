package fileloader

import (
	"data-ingestor/models"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

func DecodeFile(filePath, modelName string) ([]interface{}, error) {
	return createModel(modelName, filePath)
}

func createModel(modelName string, filePath string) ([]interface{}, error) {
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
			rawRecords, err := ParseXMLConsecutiveRecords(filePath)
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

// ParseXMLConsecutiveRecords Parses consecutive <Record> elements in an XML file
func ParseXMLConsecutiveRecords(filePath string) ([]models.Record, error) {
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



