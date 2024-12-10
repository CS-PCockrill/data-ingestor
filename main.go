package main

import (
	"data-ingestor/config"
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/util"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"go.uber.org/zap"
	"log"
)

type App struct {
	Config    *config.Config
	Logger    *zap.Logger
	DB 		  *sql.DB

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

	// Initialize the counter
	counter := &util.Counter{}

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

	fileLoader := fileloader.LoaderFunctions{CONFIG: app.Config, Logger: app.Logger}
	dbTransposer := dbtransposer.TransposerFunctions{CONFIG: app.Config, Logger: app.Logger}

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
	//recordChan := make(chan interface{}, 1000) // Adjust the buffer size to handle more records
	recordChan := make(chan map[string]interface{}, 1000)

	//xmlFilePath := "test-loader.xml"
	//jsonOutputPath := "output.json"
	//csvOutputPath := "output.csv"
	//excelOutputPath := "output.xlsx"

	// Parse XML and flatten
	//records, err := fileLoader.FlattenXMLToMaps(xmlFilePath)
	//if err != nil {
	//	fmt.Printf("Error flattening XML: %v\n", err)
	//	return
	//}

	// Export to JSON
	//if err := fileLoader.ExportToJSON(records, jsonOutputPath); err != nil {
	//	fmt.Printf("Error exporting to JSON: %v\n", err)
	//}

	// Export to CSV
	//if err := fileLoader.ExportToCSV(records, csvOutputPath); err != nil {
	//	fmt.Printf("Error exporting to CSV: %v\n", err)
	//}

	// Export to Excel
	//if err := fileLoader.ExportToExcel(records, excelOutputPath); err != nil {
	//	fmt.Printf("Error exporting to Excel: %v\n", err)
	//}

	// Start streaming the file into the record channel
	go func() {
		if err := fileLoader.StreamDecodeFileWithSchema(inputFile, recordChan, modelName); err != nil {
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
		func(stream chan map[string]interface{}) error { // Stream function for MapReduce
			for record := range recordChan {
				stream <- record
			}
			return nil
		},
		dbTransposer.InsertRecordsUsingSchema,
		dbTransposer.ProcessMapResults,
		app.DB,
		tableName,
		app.Config.Runtime.WorkerCount,
		counter,
	)

	if err != nil {
		app.Logger.Fatal("Stream MapReduce Failed",
			zap.Any("input_file", inputFile),
			zap.Any("model_type", modelName),
			zap.Any("table_name", tableName),
			zap.Any("worker_count", app.Config.Runtime.WorkerCount),
			zap.Error(err))
		return
	}

	log.Println("Stream MapReduce completed successfully")
	app.Logger.Info("Stream MapReduce Succeeded",
		zap.Any("input_file", inputFile),
		zap.Any("model_type", modelName),
		zap.Any("table_name", tableName),
		zap.Any("records_inserted_success", counter.GetSucceeded()),
		zap.Any("records_inserted_error", counter.GetErrors()),
		zap.Any("worker_count", app.Config.Runtime.WorkerCount))

	// Move input file (inputFile) to config runtime folder/directory destination
	err = fileLoader.MoveInputFile(inputFile, app.Config.Runtime.FileDestination)
	if err != nil {
		app.Logger.Error("Failed to Move Input File",
			zap.Any("input_file", inputFile),
			zap.Any("destination", app.Config.Runtime.FileDestination),
			zap.Any("model_type", modelName),
			zap.Any("table_name", tableName),
			zap.Any("worker_count", app.Config.Runtime.WorkerCount),
			zap.Error(err))
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