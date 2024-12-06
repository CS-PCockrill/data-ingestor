package fileloader

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
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

// UnmarshalFile unmarshalls the file content into the provided struct based on file type.
func UnmarshalFile(filePath string, v interface{}) error {
	fileType, err := detectFileType(filePath)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	//defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	switch fileType {
	case "json":
		return json.Unmarshal(data, v)
	case "xml":
		return xml.Unmarshal(data, v)
	default:
		return errors.New("unsupported file type")
	}
}

