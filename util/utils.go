package util

import (
	"data-ingestor/models"
	"encoding/xml"
	"fmt"
	"sync"
)

// GenerateSampleRecords Generate sample records (replace with actual data source)
func GenerateSampleRecords(count int) []models.Record {
	records := make([]models.Record, count)
	for i := 0; i < count; i++ {
		records[i] = models.Record{
			XMLName:     xml.Name{},
			MistAMSData: models.MistAMSData{
				User:          fmt.Sprintf("User%d", i+1),
				DateCreated:   1698412800 + int64(i),
				DateSubmitted: 1698499200 + int64(i),
				AssetName:     stringPointer(fmt.Sprintf("Asset%d", i+1)),
				Location:      "HQ",
				Status:        "Pending",
				JsonHash:      fmt.Sprintf("hash%d", i+1),
				LocalID:       stringPointer(fmt.Sprintf("local%d", i+1)),
				FileName:      fmt.Sprintf("file%d.txt", i+1),
				FNumber:       fmt.Sprintf("FN%03d", i+1),
				ScanTime:      "2024-12-05T08:00:00Z",
			},
		}
	}
	return records
}

// Helper function to create a string pointer
func stringPointer(s string) *string {
	return &s
}

type Counter struct {
	mu    sync.Mutex
	total int
}

// Increment safely increments the total count by the given value.
func (c *Counter) Increment(count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total += count
}

// Get safely retrieves the total count.
func (c *Counter) Get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

