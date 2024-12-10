package main

import (
	"data-ingestor/config"
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"go.uber.org/zap"
	"log"
	"os"
)

type App struct {
	Config    *config.Config
	Logger    *zap.Logger
	DB 		  *sql.DB
	KeyColumnMapping map[string]map[string]string // Map for key-column mappings

}

func main() {
	app, err := NewApp()
	if err != nil {
		log.Fatalf("Error initializing application: %v", err)
	}
	defer app.Close()

	// Define a command-line flag for the input file
	var inputFile string
	var modelName string
	var tableName string

	// Command-line flags
	flag.StringVar(&inputFile, "file", "", "Path to the input file ( .json or .xml )")
	flag.StringVar(&modelName, "model", "", "Target model type ( MistAMS )")
	flag.StringVar(&tableName, "table", "", "Database table name for inserts ( SFLW_RECS )")
	flag.Parse()

	if inputFile == "" || modelName == "" || tableName == "" {
		app.Logger.Fatal("Missing Fields",
			zap.Any("Error", "-file, -model, and -table flags are required"),
			zap.Any("Usage", "go run main.go -file test-loader.xml -model MistAMS -table SFLW_RECS"))
		return
	}

	keyColumnMapping, err := LoadKeyColumnMapping("data-schema.json")
	if err != nil {
		app.Logger.Fatal("Loading Key Column Mapping Failed", zap.Error(err))
		return
	}
	app.KeyColumnMapping = keyColumnMapping

	fileLoader := fileloader.LoaderFunctions{CONFIG: app.Config, Logger: app.Logger, KeyColumnMapping: keyColumnMapping}
	dbTransposer := dbtransposer.TransposerFunctions{CONFIG: app.Config, Logger: app.Logger, KeyColumnMapping: keyColumnMapping}

	// Decode the file and map records
	//records, err := fileLoader.DecodeFile(inputFile, modelName)
	//if err != nil {
	//	log.Fatalf("Error decoding input file %s - %v", inputFile, err)
	//	return
	//}
	//
	//// Run MapReduce
	//err = mapreduce.MapReduce(records, dbTransposer.InsertRecords, dbTransposer.ProcessMapResults, app.DB, tableName, app.Config.Runtime.WorkerCount)
	//if err != nil {
	//	log.Fatalf("MapReduce failed: %v", err)
	//} else {
	//	log.Printf("MapReduce completed successfully, inserted %d records", len(records))
	//}

	// Channel to stream records
	recordChan := make(chan interface{}, 1000) // Adjust the buffer size to handle more records
	//recordChan := make(chan map[string]interface{}, 1000)

	// Start streaming the file into the record channel
	go func() {
		if err := fileLoader.StreamDecodeFile(inputFile, recordChan, modelName); err != nil {
			app.Logger.Fatal("Error Streaming Input File",
				zap.Any("input_file", inputFile),
				zap.Any("model_type", modelName),
				zap.Any("table_name", tableName),
				zap.Any("worker_count", app.Config.Runtime.WorkerCount),
				zap.Error(err))
		}
		close(recordChan)
	}()

	// Run Stream MapReduce
	err = mapreduce.MapReduceStreaming(
		func(stream chan interface{}) error { // Stream function for MapReduce
			for record := range recordChan {
				stream <- record
			}
			return nil
		},
		dbTransposer.InsertRecords,
		dbTransposer.ProcessMapResults,
		app.DB,
		tableName,
		app.Config.Runtime.WorkerCount,
	)

	if err != nil {
		app.Logger.Fatal("Stream MapReduce Failed",
			zap.Any("input_file", inputFile),
			zap.Any("model_type", modelName),
			zap.Any("table_name", tableName),
			zap.Any("worker_count", app.Config.Runtime.WorkerCount),
			zap.Error(err))
		return
	} else {
		log.Println("Stream MapReduce completed successfully")
		app.Logger.Info("Stream MapReduce Succeeded",
			zap.Any("input_file", inputFile),
			zap.Any("model_type", modelName),
			zap.Any("table_name", tableName),
			zap.Any("worker_count", app.Config.Runtime.WorkerCount))
	}

}

// NewApp initializes the App with dependencies
func NewApp() (*App, error) {
	cfg := config.GetConfig()

	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("fatal error initializing logger: %w", err)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", cfg.DB.DBUser, cfg.DB.DBPassword, cfg.DB.DBHostname, cfg.DB.DBPort, cfg.DB.DBName)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("fatal error connecting to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping after connecting to database: %w", err)
	}

	db.SetMaxOpenConns(cfg.Runtime.WorkerCount)
	return &App{Config: cfg, Logger: logger, DB: db}, nil
}

func (app *App) Close() {
	app.Logger.Sync()
	app.DB.Close()
}

// LoadKeyColumnMapping loads a JSON file and parses it into a map.
func LoadKeyColumnMapping(filePath string) (map[string]map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open key-column mapping file: %w", err)
	}

	var mapping map[string]map[string]string
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&mapping); err != nil {
		return nil, fmt.Errorf("failed to decode key-column mapping JSON: %w", err)
	}

	return mapping, nil
}