package mapreduce

import (
	"database/sql"
	"fmt"
	"sync"
)

// MapResult holds the outcome of a Map task.
type MapResult struct {
	BatchID int       // Identifier for the batch
	Err     error     // Error encountered during processing (if any)
	Tx      *sql.Tx   // The transaction associated with this batch
}

// Task represents a unit of work to be processed.
type Task struct {
	Input  interface{}   // Input data for the task
	Output interface{}   // Output result after processing
	Err    error         // Any error encountered during processing
}

// MapFunc defines the function signature for the map phase.
type MapFunc func(tx *sql.Tx, tableName string, batch interface{}) error

// ReduceFunc defines the function signature for reduce phase.
type ReduceFunc func(results []MapResult) error

// worker processes tasks from the taskChan and sends results to resultChan.
func worker(taskChan <-chan interface{}, resultChan chan<- MapResult, mapFunc MapFunc, db *sql.DB, tableName string, batchID int, wg *sync.WaitGroup) {
	defer wg.Done()
	tx, err := db.Begin() // Start a transaction
	if err != nil {
		resultChan <- MapResult{BatchID: batchID, Err: err, Tx: nil}
		return
	}

	defer func() {
		resultChan <- MapResult{BatchID: batchID, Err: err, Tx: tx}
	}()

	for batch := range taskChan {
		// Execute the Map function within the transaction
		err = mapFunc(tx, tableName, batch)
	}
}

// MapReduce orchestrates the Map and Reduce phases.
func MapReduce(records []interface{}, mapFunc MapFunc, reduceFunc ReduceFunc, db *sql.DB, tableName string, workerCount int) error {
	// Channels for tasks and results
	taskChan := make(chan interface{}, workerCount)
	resultChan := make(chan MapResult, workerCount)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(taskChan, resultChan, mapFunc, db, tableName, i, &wg)
	}

	// Split records into batches and feed them to taskChan
	go func() {
		fmt.Printf("Length of Records: %d | Worker Count: %d\n", len(records), workerCount)
		batchSize := (len(records) + workerCount - 1) / workerCount
		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}
			taskChan <- records[i:end]
		}
		close(taskChan)
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var results []MapResult
	for result := range resultChan {
		results = append(results, result)
	}

	// Perform Reduce phase
	return reduceFunc(results)
}

// MapReduceStreaming orchestrates the Map and Reduce phases with streaming.
func MapReduceStreaming(
	fileLoader func(chan interface{}) error, // Function to stream records from a file
	mapFunc MapFunc,                         // Function to handle Map phase
	reduceFunc ReduceFunc,                   // Function to handle Reduce phase
	db *sql.DB,                              // Database connection
	tableName string,                        // Database table name
	workerCount int,                         // Number of workers
) error {
	// Channels for streaming records and task batches
	recordChan := make(chan interface{}, 20)
	taskChan := make(chan interface{}, 20)
	resultChan := make(chan MapResult, 20)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(taskChan, resultChan, mapFunc, db, tableName, i, &wg)
	}

	// Stream records from the file
	go func() {
		defer close(taskChan)
		for record := range recordChan {
			taskChan <- record
		}
	}()

	// Start file loading (streaming records)
	go func() {
		if err := fileLoader(recordChan); err != nil {
			close(recordChan) // Ensure recordChan is closed if there's an error
		}
		close(recordChan)
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var results []MapResult
	for result := range resultChan {
		results = append(results, result)
	}

	// Perform Reduce phase
	return reduceFunc(results)
}
