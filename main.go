package main

import (
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/pkg"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"log"
)


func main() {
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

	// Decode the file and map records
	records, err := fileloader.DecodeFile(inputFile, modelName)
	if err != nil {
		log.Fatalf("Error decoding input file %s - %v", inputFile, err)
		return
	}

	//var result models.Data
	//var records []interface{}
	//
	//// Unmarshal the file
	//if err := fileloader.UnmarshalFile(inputFile, &result); err != nil {
	//	fmt.Println("Error: ", err)
	//	return
	//}

	fmt.Printf("Processing and Mapping Records with Type: %T", records)
	// Process and map records
	//for _, record := range result.Records {
	//	records = append(records, record)
	//	// Access individual fields
	//	fmt.Printf("User: %v | Hash: %v\n", record.User, record.JsonHash)
	//}

	// PostgreSQL connection string
	dsn := "postgres://root:password@localhost:5432/testdb"

	// Connect to the database
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Failed to open a connection to the database: %v", err)
	}
	defer db.Close()

	// Test the connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	fmt.Println("Connected to PostgreSQL database successfully!")
	db.SetMaxOpenConns(pkg.WorkerCount)
	// Run MapReduce
	err = mapreduce.MapReduce(records, dbtransposer.InsertRecords, dbtransposer.ProcessMapResults, db, tableName, pkg.WorkerCount)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	} else {
		log.Printf("MapReduce completed successfully, inserted %d records", len(records))
	}

}

