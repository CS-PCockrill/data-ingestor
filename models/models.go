package models

import "encoding/xml"

// MistAMSData contains the data fields for each record
type MistAMSData struct {
	User          string      `json:"user" xml:"user" db:"user"`
	DateCreated   int64       `json:"dateCreated" xml:"dateCreated" db:"dt_created"`
	DateSubmitted int64       `json:"dateSubmitted" xml:"dateSubmitted" db:"dt_submitted"`
	AssetName     *string     `json:"assetName" xml:"assetName" db:"ast_name"`
	Location      string      `json:"location" xml:"location" db:"location"`
	Status        string      `json:"status" xml:"status" db:"status"`
	JsonHash      string      `json:"jsonHash" xml:"jsonHash" db:"json_hash"`
	LocalID       *string     `json:"localId" xml:"localId" db:"local_id"`
	FileName      string      `json:"fileName" xml:"fileName" db:"filename"`
	FNumber  string `json:"fNumber" xml:"fNumber" db:"fnumber"`
	ScanTime string `json:"scanTime" xml:"scanTime" db:"scan_time"`
	FNumbers      []FNumbers  `json:"fnumbers" xml:"fnumbers"` // Not directly mapped to the database
}

// FNumbers represents the fNumber and scanTime fields
type FNumbers struct {
	FNumber  string `json:"fNumber" xml:"fNumber"`
	ScanTime string `json:"scanTime" xml:"scanTime"`
}

// Data is the top-level tag in the input XML file
type Data struct {
	XMLName xml.Name `xml:"Data"`
	Records []Record `json:"Records" xml:"Record"` // Correctly maps repeated <Record> elements
}

// Record represents a single record in the XML
type Record struct {
	XMLName     xml.Name    `xml:"Record"`
	MistAMSData `xml:",inline"` // Inline fields from MistAMSData into the <Record> element
}
