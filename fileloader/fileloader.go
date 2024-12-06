package fileloader

import (
	"encoding/xml"
	"errors"
	"fmt"
	"os"
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

// UnmarshalFile unmarshals the file content into the provided struct based on file type.
func UnmarshalFile(filePath string, v interface{}) error {
	_, err := detectFileType(filePath)
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

	// Parse the XML into the Data struct
	decoder := xml.NewDecoder(file)
	err = decoder.Decode(v)
	if err != nil {
		fmt.Printf("Error decoding XML: %v\n", err)
		return err
	}

	return nil
	//switch fileType {
	//case "json":
	//	return json.Unmarshal(data, v)
	//case "xml":
	//	return xml.Unmarshal(data, v)
	//default:
	//	return errors.New("unsupported file type")
	//}
}

