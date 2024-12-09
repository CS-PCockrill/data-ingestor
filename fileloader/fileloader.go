package fileloader

import (
	"data-ingestor/models"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// detectFileType detects whether the file is JSON or XML based on the extension or content.
func detectFileType(filePath string) (string, error) {
	if strings.HasSuffix(filePath, ".json") {
		return "json", nil
	} else if strings.HasSuffix(filePath, ".xml") {
		return "xml", nil
	}
	return "", errors.New("unsupported file format: must be .json or .xml")
}

// unmarshalFile unmarshals the file content into the provided struct based on file type.
func unmarshalFile(filePath string, v interface{}) error {
	fileType, err := detectFileType(filePath)
	if err != nil {
		return err
	}

	// Open the XML file
	fmt.Printf("Opening file at path: %v", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return err
	}

	switch fileType {
	case "json":
		// Parse the XML into the Data struct
		decoder := json.NewDecoder(file)
		err = decoder.Decode(v)
		if err != nil {
			fmt.Printf("Error decoding XML: %v\n", err)
			return err
		}

	case "xml":
		// Parse the XML into the Data struct
		decoder := xml.NewDecoder(file)
		err = decoder.Decode(v)
		if err != nil {
			fmt.Printf("Error decoding XML: %v\n", err)
			return err
		}

	default:
		return errors.New("unsupported file type")
	}

	return nil
}


func createModel(modelName string) (interface{}, error) {
	switch modelName {
	case "MistAMSData":
		return &models.Record{}, nil
	default:
		return nil, fmt.Errorf("unknown model type: %s", modelName)
	}
}


func DecodeFile(filePath string, modelName string) ([]interface{}, error) {
	// Create a new instance of the target model
	model, err := createModel(modelName)
	if err != nil {
		return nil, err
	}

	// Read and unmarshal the file
	var records []interface{}
	if err := unmarshalFile(filePath, model); err != nil {
		return nil, fmt.Errorf("failed to unmarshal file: %w", err)
	}

	// Reflect to process the "Records" field dynamically
	val := reflect.ValueOf(model).Elem()
	recordsField := val.FieldByName("Records")
	if !recordsField.IsValid() {
		return nil, fmt.Errorf("field 'Records' not found in model %s", modelName)
	}

	// Collect records into the result slice
	for i := 0; i < recordsField.Len(); i++ {
		records = append(records, recordsField.Index(i).Interface())
	}

	return records, nil
}

