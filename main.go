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

	// Channel to stream records
	// Adjust the buffer size to handle more records
	recordChan := make(chan map[string]interface{}, 1000)

	excelInputPath := "db-template.xlsx"
	//excelOutputPath := "output.xlsx"

	// Parse XML and flatten
	//records, err := fileLoader.FlattenXMLToMaps(xmlFilePath)
	//if err != nil {
	//	fmt.Printf("Error flattening XML: %v\n", err)
	//	return
	//}

	// Export to Excel
	//if err := fileLoader.ExportToExcel(records, excelOutputPath); err != nil {
	//	fmt.Printf("Error exporting to Excel: %v\n", err)
	//}

	templateColumns, _, err := dbTransposer.ExtractSQLDataFromExcel(excelInputPath, "Sheet1", "A3:K3", 3)
	if err != nil {
		app.Logger.Fatal("Failed to Load SQL Data from Excel",
			zap.Any("excelInput", excelInputPath),
			zap.Any("sheetName", "Sheet1"),
			zap.Any("rangeSpec", "A3:K3"),
			zap.Any("line", 3),
			zap.Error(err))
	}

	// Start streaming the file into the record channel
	go func() {
		if err := fileLoader.StreamDecodeFileWithSchema(inputFile, recordChan, modelName, templateColumns); err != nil {
			app.Logger.Fatal("Error Streaming Input File",
				zap.Any("input_file", inputFile),
				zap.Any("model_type", modelName),
				zap.Any("table_name", tableName),
				zap.Any("worker_count", app.Config.Runtime.WorkerCount),
				zap.Error(err))
		}
		close(recordChan)
	}()

	// Run Stream Map-Reduce
	err = mapreduce.MapReduceStreaming(
		func(stream chan map[string]interface{}) error { // Stream function for Map-Reduce
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
		app.Logger.Fatal("Stream Map-Reduce Failed",
			zap.Any("input_file", inputFile),
			zap.Any("model_type", modelName),
			zap.Any("table_name", tableName),
			zap.Any("worker_count", app.Config.Runtime.WorkerCount),
			zap.Error(err))
		return
	}

	log.Println("Stream Map-Reduce completed successfully")
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