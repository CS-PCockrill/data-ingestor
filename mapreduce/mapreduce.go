package mapreduce

import (
	"database/sql"
	"fmt"
	"log"
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
	Output interface{}   // Result after processing
	Err    error         // Any error encountered during processing
}

// MapFunc defines the function signature for the map phase.
type MapFunc func(tx *sql.Tx, tableName string, batch <-chan interface{}) error

// ReduceFunc defines the function signature for the reduce phase.
type ReduceFunc func(results []MapResult) error


// worker processes streamed tasks from the taskChan and sends results to resultChan.
func worker(taskChan <-chan chan interface{}, resultChan chan<- MapResult, mapFunc func(tx *sql.Tx, tableName string, stream <-chan interface{}) error, db *sql.DB, tableName string, batchID int, wg *sync.WaitGroup) {
	defer wg.Done()
	for stream := range taskChan {
		// Start a transaction
		tx, err := db.Begin()
		if err != nil {
			resultChan <- MapResult{BatchID: batchID, Err: fmt.Errorf("failed to start transaction: %w", err), Tx: nil}
			continue
		}

		// Execute the Map function with the stream
		err = mapFunc(tx, tableName, stream)
		if err != nil {
			resultChan <- MapResult{BatchID: batchID, Err: err, Tx: tx}
			continue
		}

		// Commit the transaction if Map function succeeds
		if err != nil {
			resultChan <- MapResult{BatchID: batchID, Err: fmt.Errorf("failed to commit transaction: %w", err), Tx: tx}
			continue
		}

		// Send a successful result
		resultChan <- MapResult{BatchID: batchID, Err: nil, Tx: tx}
	}
}


// worker processes tasks from the taskChan and sends results to resultChan.
//func worker(taskChan <-chan interface{}, resultChan chan<- MapResult, mapFunc MapFunc, db *sql.DB, tableName string, batchID int, wg *sync.WaitGroup) {
//	defer wg.Done()
//	for batch := range taskChan {
//		tx, err := db.Begin() // Start a transaction
//		if err != nil {
//			resultChan <- MapResult{BatchID: batchID, Err: err, Tx: nil}
//			continue
//		}
//
//		// Execute the Map function within the transaction
//		err = mapFunc(tx, tableName, batch)
//		resultChan <- MapResult{BatchID: batchID, Err: err, Tx: tx}
//	}
//}

// MapReduce orchestrates the Map and Reduce phases.
func MapReduce(records []interface{}, mapFunc MapFunc, reduceFunc ReduceFunc, db *sql.DB, tableName string, workerCount int) error {
	// Channels for tasks and results
	taskChan := make(chan chan interface{}, workerCount)
	resultChan := make(chan  MapResult, workerCount)
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
			//taskChan <- records[i:end]
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
func MapReduceStreaming(streamFunc func(chan<- interface{}) error, mapFunc func(*sql.Tx, string, <-chan interface{}) error, reduceFunc ReduceFunc, db *sql.DB, tableName string, workerCount int) error {
	// Channels for tasks and results
	taskChan := make(chan chan interface{}, workerCount)
	resultChan := make(chan MapResult, workerCount)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(taskChan, resultChan, mapFunc, db, tableName, i, &wg)
	}

	// Stream records to worker task channels
	go func() {
		recordStream := make(chan interface{}, workerCount)
		defer close(recordStream)

		if err := streamFunc(recordStream); err != nil {
			log.Printf("Error during stream: %v", err)
			close(taskChan) // Close tasks channel to stop workers
			return
		}

		taskChan <- recordStream
		close(taskChan)
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and apply Reduce
	var results []MapResult
	for result := range resultChan {
		results = append(results, result)
	}

	return reduceFunc(results)
}

