package models

import "encoding/xml"

type FNumbers struct {
	FNumber string `json:"fNumber" xml:"fNumber"`
	ScanTime string `json:"scanTime" xml:"scanTime"`
}

type MistAMSData struct {
	User string `json:"user" xml:"user"`
	DateCreated int64 `json:"dateCreated" xml:"dateCreated"`
	DateSubmitted int64 `json:"dateSubmitted" xml:"dateSubmitted"`
	AssetName	  *string `json:"assetName" xml:"assetName"`
	Location	  string  `json:"location" xml:"location"`
	Status		  string  `json:"status" xml:"status"`
	JsonHash	  string  `json:"jsonHash" xml:"jsonHash"`
	LocalID		  *string `json:"localId" xml:"localId"`
	FileName	  string  `json:"fileName" xml:"fileName"`
	FNumbers	  []FNumbers `json:"fnumbers" xml:"fnumbers"`
}

// Data is the top-level tag in the input XML file
type Data struct {
	XMLName xml.Name `xml:"Data"`
	Records []Record `xml:"Record"`
}

// Record is a single row of a database record in the input XML file
type Record struct {
	XMLName      xml.Name     `xml:"Record"`
	RecordData RecordData `xml:"RecordData"`
}

// RecordData is the column names of a database Record in the input XML file
type RecordData struct {
	XMLName xml.Name `xml:"RecordData"`
	SecurityOrg string   `xml:"security_org"`
}
