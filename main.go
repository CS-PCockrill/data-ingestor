package main

import (
	"data-ingestor/dbtransposer"
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/models"
	"data-ingestor/pkg"
	"database/sql"
	"fmt"
	_ "github.com/godror/godror"
	"log"
)

func main() {
	// Example struct to hold the data
	var result []models.Data
	var records []interface{}

	// Specify the input file (JSON or XML)
	inputFile := "test-loader.xml" // Change to "test-loader.json" to test JSON input

	// Unmarshal the file
	if err := fileloader.UnmarshalFile(inputFile, &result); err != nil {
		fmt.Println("Error: ", err)
		return
	}

	// Process and map records
	for _, record := range result {
		for _, r := range record.Records {
			// Append each Record as an interface{}
			records = append(records, r)
			// Access individual fields
			fmt.Printf("User: %v| Hash: %v", r.User, r.JsonHash)
		}
	}

	// Database connection
	dsn := fmt.Sprintf("%s/%s@%s:%s/%s", pkg.DBUser, pkg.DBPassword, pkg.DBHostname, pkg.DBPort, pkg.DBName)
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(25)

	// Run MapReduce
	err = mapreduce.MapReduce(records, dbtransposer.InsertRecords, dbtransposer.ProcessMapResults, db, pkg.WorkerCount)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	} else {
		log.Println("MapReduce completed successfully")
	}

}

