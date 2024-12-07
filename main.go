package main

import (
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/models"
	"data-ingestor/pkg"
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"log"
)

func main() {
	// Example struct to hold the data
	var result models.Data
	var records []interface{}

	// Specify the input file (JSON or XML)
	inputFile := "test-loader.xml" // Change to "test-loader.json" to test JSON input

	// Unmarshal the file
	if err := fileloader.UnmarshalFile(inputFile, &result); err != nil {
		fmt.Println("Error: ", err)
		return
	}

	fmt.Printf("Unmarshalled Result: %v", result)
	fmt.Println("Processing and Mapping Records")
	// Process and map records
	for _, record := range result.Records {
		records = append(records, record)
		// Access individual fields
		fmt.Printf("User: %v| Hash: %v", record.User, record.JsonHash)
	}

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
	err = mapreduce.MapReduce(records, dbtransposer.InsertRecords, dbtransposer.ProcessMapResults, db, pkg.WorkerCount)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	} else {
		log.Printf("MapReduce completed successfully, inserted %d records", len(records))
	}

}

