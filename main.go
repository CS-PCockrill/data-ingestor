package main

import (
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/models"
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
	flag.StringVar(&inputFile, "file", "", "Path to the input file (JSON or XML)")
	flag.Parse()

	// Ensure the input file flag is provided
	if inputFile == "" {
		log.Fatalf("Error: You must specify an input file using the -file flag")
	}

	var result models.Data
	var records []interface{}

	// Unmarshal the file
	if err := fileloader.UnmarshalFile(inputFile, &result); err != nil {
		fmt.Println("Error: ", err)
		return
	}

	fmt.Println("Processing and Mapping Records")
	// Process and map records
	for _, record := range result.Records {
		records = append(records, record)
		// Access individual fields
		fmt.Printf("User: %v | Hash: %v\n", record.User, record.JsonHash)
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

