package main

import (
	"data-ingestor/config"
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"go.uber.org/zap"
	"log"
)

type App struct {
	Config *config.Config
	Logger    *zap.Logger
	DB *sql.DB
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
	flag.StringVar(&inputFile, "file", "", "Path to the input file (JSON or XML)")
	flag.StringVar(&modelName, "model", "", "Target model type (e.g., 'MistAMSData')")
	flag.StringVar(&tableName, "table", "", "Database table name for inserts (e.g., SFLW_RECS)")
	flag.Parse()

	if inputFile == "" || modelName == "" || tableName == "" {
		log.Fatalf("Error: -file, -model, and -table flags are required..\n\tUsage: go run main.go -file test-loader.json -model MistAMSData -table SFLW_RECS")
		return
	}

	fileLoader := fileloader.LoaderFunctions{CONFIG: app.Config}
	dbTransposer := dbtransposer.TransposerFunctions{CONFIG: app.Config}

	// Decode the file and map records
	records, err := fileLoader.DecodeFile(inputFile, modelName)
	if err != nil {
		log.Fatalf("Error decoding input file %s - %v", inputFile, err)
		return
	}

	// Run MapReduce
	err = mapreduce.MapReduce(records, dbTransposer.InsertRecords, dbTransposer.ProcessMapResults, app.DB, tableName, app.Config.Runtime.WorkerCount)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	} else {
		log.Printf("MapReduce completed successfully, inserted %d records", len(records))
	}

}

func (app *App) Close() {
	app.Logger.Sync()
	app.DB.Close()
}
