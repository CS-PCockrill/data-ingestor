package main

import (
	"data-ingestor/fileloader"
	"data-ingestor/mapreduce"
	"data-ingestor/models"
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
	inputFile := "test-loader.xml" // Change to "example.xml" to test XML input

	// Unmarshal the file
	if err := fileloader.UnmarshalFile(inputFile, &result); err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Process and map records
	for _, record := range result {
		for _, r := range record.Records {
			// Append each Record as an interface{}
			records = append(records, r)
			// Access individual fields
			fmt.Println("Security Org:", r.RecordData.SecurityOrg)
		}
	}

	// Database connection
	db, err := sql.Open("godror", "user/password@tnsname")
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(25)
	defer db.Close()

	// Define the Map function
	mapFunc := func(tx *sql.Tx, batch []interface{}) error {
		for _, item := range batch {
			record, ok := item.(models.Record)
			if !ok {
				return fmt.Errorf("invalid record type: %T", item)
			}
			_, err := tx.Exec(`INSERT INTO records (id, security_org) VALUES ($1, $2)`, record.RecordData.SecurityOrg, record.RecordData.SecurityOrg)
			if err != nil {
				return fmt.Errorf("failed to insert record %+v: %w", record, err)
			}
		}
		return nil
	}

	// Define the Reduce function
	reduceFunc := func(results []mapreduce.MapResult) error {
		// Preemptively check for errors or nil transactions
		hasError := false
		for _, result := range results {
			if result.Tx == nil {
				log.Printf("Batch %d failed to start a transaction: %v", result.BatchID, result.Err)
				hasError = true
				continue
			}

			if result.Err != nil {
				log.Printf("Batch %d failed: %v", result.BatchID, result.Err)
				hasError = true
			}
		}

		// Rollback all transactions if any errors are found
		if hasError {
			log.Println("Errors detected during the map phase. Rolling back all transactions.")
			for _, result := range results {
				if result.Tx != nil {
					if err := result.Tx.Rollback(); err != nil {
						log.Printf("Failed to rollback transaction for batch %d: %v", result.BatchID, err)
					} else {
						log.Printf("Transaction for batch %d rolled back successfully", result.BatchID)
					}
				}
			}
			return fmt.Errorf("map phase completed with errors; all transactions rolled back")
		}

		// Commit all transactions if no errors are found
		for _, result := range results {
			if result.Tx != nil {
				if err := result.Tx.Commit(); err != nil {
					log.Printf("Failed to commit transaction for batch %d: %v", result.BatchID, err)
					return fmt.Errorf("failed to commit transaction for batch %d: %w", result.BatchID, err)
				}
				log.Printf("Transaction for batch %d committed successfully", result.BatchID)
			}
		}

		log.Println("All transactions committed successfully")
		return nil
	}

	// Run MapReduce
	err = mapreduce.MapReduce(records, mapFunc, reduceFunc, db, 4)
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	} else {
		log.Println("MapReduce completed successfully")
	}

}

