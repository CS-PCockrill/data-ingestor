package fileloader

import (
	"data-ingestor/config"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LoaderFunctionsInterface interface {
	//DecodeFile(filePath, modelName string) ([]interface{}, error)
	//StreamDecodeFile(filePath string, recordChan chan interface{}, modelName string) error

	StreamDecodeFileWithSchema(filePath string, recordChan chan map[string]interface{}, modelName string, columns []string) error

	FlattenXMLToMaps(filePath string, columns []string) ([]map[string]interface{}, error)
	//ParseAndFlattenXMLElement(decoder *xml.Decoder, start xml.StartElement) ([]map[string]interface{}, error)
	ExportToJSON(records []map[string]interface{}, outputPath string) error
	//ExportToCSV(records []map[string]string, outputPath string) error
	ExportToExcel(records []map[string]interface{}, outputPath string) error

}

type LoaderFunctions struct {
	CONFIG *config.Config
	Logger *zap.Logger

}

var _ LoaderFunctionsInterface = (*LoaderFunctions)(nil)

// DecodeFile loads the entire file and maps its content to a specified model.
// It utilizes the createModel function to convert file content into a list of records.
//
// Parameters:
// - filePath: The path to the file to decode.
// - modelName: The name of the model to map the file content to.
//
// Returns:
// - A slice of interface{} containing the decoded records.
// - An error if decoding fails.
//func (l *LoaderFunctions) DecodeFile(filePath, modelName string) ([]interface{}, error) {
//	// Log the start of the decoding process
//	l.Logger.Info("Starting file decoding", zap.String("filePath", filePath), zap.String("modelName", modelName))
//
//	// Use the createModel function to process the file
//	result, err := l.createModel(modelName, filePath)
//	if err != nil {
//		// Log and return the error if decoding fails
//		l.Logger.Error("Failed to decode file", zap.String("filePath", filePath), zap.Error(err))
//		return nil, err
//	}
//
//	// Log success with the count of decoded records
//	l.Logger.Info("Successfully decoded file", zap.String("filePath", filePath), zap.Int("recordCount", len(result)))
//	return result, nil
//}

// createModel processes the specified file and creates a list of parsed records based on the model name.
//
// Parameters:
//   - modelName: The name of the model to parse ("MistAMS" or "Record").
//   - filePath: Path to the input file.
//
// Returns:
//   - A slice of records as []interface{}, or an error if parsing fails.
//func (l *LoaderFunctions) createModel(modelName string, filePath string) ([]interface{}, error) {
//	l.Logger.Info("Creating model from file", zap.String("modelName", modelName), zap.String("filePath", filePath))
//
//	// Detect file type (JSON or XML)
//	fileType, err := l.detectFileType(filePath)
//	if err != nil {
//		l.Logger.Error("Failed to detect file type", zap.String("filePath", filePath), zap.Error(err))
//		return nil, fmt.Errorf("failed to detect file type: %w", err)
//	}
//
//	var records []interface{}
//
//	switch modelName {
//	case "MistAMS":
//		// Top-level "Data" model
//		l.Logger.Info("Processing MistAMS model", zap.String("filePath", filePath))
//		var data models.Data
//		if err := l.unmarshalFile(filePath, fileType, &data); err != nil {
//			l.Logger.Error("Failed to unmarshal file for MistAMS", zap.String("filePath", filePath), zap.Error(err))
//			return nil, fmt.Errorf("failed to unmarshal file: %w", err)
//		}
//		// Convert records to []interface{} for MapReduce
//		for _, record := range data.Records {
//			records = append(records, record)
//		}
//
//	case "Record":
//		l.Logger.Info("Processing Record model", zap.String("filePath", filePath))
//		if fileType == "xml" {
//			// Parse consecutive <Record> elements (XML only)
//			rawRecords, err := l.parseXMLConsecutiveRecords(filePath)
//			if err != nil {
//				l.Logger.Error("Failed to parse consecutive XML records", zap.String("filePath", filePath), zap.Error(err))
//				return nil, fmt.Errorf("failed to parse consecutive records: %w", err)
//			}
//			for _, record := range rawRecords {
//				records = append(records, record)
//			}
//		} else if fileType == "json" {
//			// Directly parse an array of records (JSON only)
//			rawRecords, err := l.parseJSONArray(filePath)
//			if err != nil {
//				l.Logger.Error("Failed to parse JSON array", zap.String("filePath", filePath), zap.Error(err))
//				return nil, fmt.Errorf("failed to parse JSON array: %w", err)
//			}
//			for _, record := range rawRecords {
//				records = append(records, record)
//			}
//		} else {
//			l.Logger.Error("Unsupported file type for Record model", zap.String("fileType", fileType))
//			return nil, fmt.Errorf("unsupported file type for 'Record': %s", fileType)
//		}
//
//	default:
//		l.Logger.Error("Unknown model type", zap.String("modelName", modelName))
//		return nil, fmt.Errorf("unknown model type: %s", modelName)
//	}
//
//	l.Logger.Info("Successfully created model", zap.String("modelName", modelName), zap.Int("recordCount", len(records)))
//	return records, nil
//}

// unmarshalFile unmarshals the contents of a file into the provided struct.
//
// Parameters:
//   - filePath: Path to the input file.
//   - fileType: Type of the file ("json" or "xml").
//   - v: Pointer to the target struct for unmarshalling.
//
// Returns:
//   - An error if unmarshalling fails.
//func (l *LoaderFunctions) unmarshalFile(filePath, fileType string, v interface{}) error {
//	l.Logger.Info("Unmarshalling file", zap.String("filePath", filePath), zap.String("fileType", fileType))
//
//	file, err := os.Open(filePath)
//	if err != nil {
//		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
//		return fmt.Errorf("failed to open file: %w", err)
//	}
//
//	switch fileType {
//	case "json":
//		decoder := json.NewDecoder(file)
//		if err := decoder.Decode(v); err != nil {
//			l.Logger.Error("Failed to decode JSON file", zap.String("filePath", filePath), zap.Error(err))
//			return fmt.Errorf("failed to decode JSON file: %w", err)
//		}
//	case "xml":
//		decoder := xml.NewDecoder(file)
//		if err := decoder.Decode(v); err != nil {
//			l.Logger.Error("Failed to decode XML file", zap.String("filePath", filePath), zap.Error(err))
//			return fmt.Errorf("failed to decode XML file: %w", err)
//		}
//	default:
//		l.Logger.Error("Unsupported file type", zap.String("fileType", fileType))
//		return fmt.Errorf("unsupported file type: %s", fileType)
//	}
//
//	l.Logger.Info("Successfully unmarshalled file", zap.String("filePath", filePath))
//	return nil
//}

// parseXMLConsecutiveRecords parses consecutive <Record> elements from an XML file.
//
// Parameters:
//   - filePath: Path to the input XML file.
//
// Returns:
//   - A slice of parsed records, or an error if parsing fails.
//func (l *LoaderFunctions) parseXMLConsecutiveRecords(filePath string) ([]models.Record, error) {
//	l.Logger.Info("Parsing consecutive XML records", zap.String("filePath", filePath))
//
//	file, err := os.Open(filePath)
//	if err != nil {
//		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
//		return nil, fmt.Errorf("failed to open file: %w", err)
//	}
//
//	var records []models.Record
//	decoder := xml.NewDecoder(file)
//	for {
//		token, err := decoder.Token()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			l.Logger.Error("Error reading XML token", zap.String("filePath", filePath), zap.Error(err))
//			return nil, fmt.Errorf("error reading XML: %w", err)
//		}
//
//		if se, ok := token.(xml.StartElement); ok && se.Name.Local == "Record" {
//			var record models.Record
//			if err := decoder.DecodeElement(&record, &se); err != nil {
//				l.Logger.Error("Error decoding XML record", zap.Error(err))
//				return nil, fmt.Errorf("error decoding record: %w", err)
//			}
//			records = append(records, record)
//		}
//	}
//
//	l.Logger.Info("Successfully parsed XML records", zap.String("filePath", filePath), zap.Int("recordCount", len(records)))
//	return records, nil
//}

// parseJSONArray parses an array of records from a JSON file.
//
// Parameters:
//   - filePath: Path to the input JSON file.
//
// Returns:
//   - A slice of parsed records, or an error if parsing fails.
//func (l *LoaderFunctions) parseJSONArray(filePath string) ([]models.Record, error) {
//	l.Logger.Info("Parsing JSON array", zap.String("filePath", filePath))
//
//	file, err := os.Open(filePath)
//	if err != nil {
//		l.Logger.Error("Failed to open file", zap.String("filePath", filePath), zap.Error(err))
//		return nil, fmt.Errorf("failed to open file: %w", err)
//	}
//
//	var rawRecords []models.Record
//	decoder := json.NewDecoder(file)
//	if err := decoder.Decode(&rawRecords); err != nil {
//		l.Logger.Error("Error decoding JSON array", zap.String("filePath", filePath), zap.Error(err))
//		return nil, fmt.Errorf("failed to decode JSON array: %w", err)
//	}
//
//	l.Logger.Info("Successfully parsed JSON records", zap.String("filePath", filePath), zap.Int("recordCount", len(rawRecords)))
//	return rawRecords, nil
//}


// detectFileType detects whether the file is JSON or XML based on the extension or content.
func (l *LoaderFunctions) detectFileType(filePath string) (string, error) {
	if strings.HasSuffix(filePath, ".json") {
		return "json", nil
	} else if strings.HasSuffix(filePath, ".xml") {
		return "xml", nil
	}
	return "", errors.New("unsupported file format: must be .json or .xml")
}


// MoveInputFile moves a file from its current location to a specified destination folder.
// If the destination folder does not exist, it will be created.
// Parameters:
//   - inputFile: The full path to the file that needs to be moved.
//   - destinationFolder: The target directory where the file will be moved.
// Returns:
//   - error: An error if the operation fails, otherwise nil.
func (l *LoaderFunctions) MoveInputFile(inputFile, destinationFolder string) error {
	// Check if the destination folder exists. If not, create it.
	if _, err := os.Stat(destinationFolder); os.IsNotExist(err) {
		// Create all necessary directories in the destination path.
		if err := os.MkdirAll(destinationFolder, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create destination folder: %w", err)
		}
	}

	// Extract the base name of the input file (e.g., "file.txt").
	fileName := filepath.Base(inputFile)

	// Construct the full path for the destination file.
	destinationPath := filepath.Join(destinationFolder, fileName)

	// Open the source file for reading.
	sourceFile, err := os.Open(inputFile)
	if err != nil {
		// Log the error and return if the source file cannot be opened.
		l.Logger.Error("Failed to open source file",
			zap.String("inputFile", inputFile),
			zap.Error(err),
		)
		return fmt.Errorf("failed to open source file: %w", err)
	}

	// Create the destination file for writing.
	destFile, err := os.Create(destinationPath)
	if err != nil {
		// Log the error and return if the destination file cannot be created.
		l.Logger.Error("Failed to create destination file",
			zap.String("destinationPath", destinationPath),
			zap.Error(err),
		)
		return fmt.Errorf("failed to create destination file: %w", err)
	}

	// Copy the contents of the source file to the destination file.
	if _, err := io.Copy(destFile, sourceFile); err != nil {
		// Log the error and return if the copy operation fails.
		l.Logger.Error("Failed to copy file contents",
			zap.String("source", inputFile),
			zap.String("destination", destinationPath),
			zap.Error(err),
		)
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	// Remove the original source file after successfully copying its contents.
	if err := os.RemoveAll(inputFile); err != nil {
		// Log the error and return if the original file cannot be removed.
		l.Logger.Error("Failed to remove original file",
			zap.String("inputFile", inputFile),
			zap.Error(err),
		)
		return fmt.Errorf("failed to remove original file: %w", err)
	}

	// Log the successful move operation.
	l.Logger.Info("File moved successfully",
		zap.String("source", inputFile),
		zap.String("destination", destinationPath),
	)

	return nil
}
