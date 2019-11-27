package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

var (
	writer *csv.Writer
)

func main() {

	fileNames := GetFileNames()
	readers := OpenReaders(fileNames)
	allHeaders := GatherAllHeaders(readers, fileNames)
	joinColumns := IdentifyJoinColumns(allHeaders)
	outputColumns := IdentifyOutputColumns(allHeaders)

	allKeys, allData := ReadAllInputSources(readers, allHeaders, joinColumns)

	writer = csv.NewWriter(os.Stdout)

	err := writer.Write(outputColumns)
	if err != nil {
		log.Fatalf("failed to write CSV output: %v", err)
	}

	for _, key := range allKeys {
		WriteCSVs(key, outputColumns, allData)
	}

	writer.Flush()
}

// WriteCSVs writes out the full join of records across all the data collections
// for a single key.
func WriteCSVs(key string, outputColumns []string, allData []DataCollection) {

	prt := func(recs []Record) {

		row := []string{}

		for _, col := range outputColumns {
			got := false
			for _, rec := range recs {
				v, ok := rec[col]
				if ok {
					row = append(row, v)
					got = true
					break
				}
			}
			if !got {
				row = append(row, "")
			}
		}

		err := writer.Write(row)
		if err != nil {
			log.Fatalf("failed to write CSV output: %v", err)
		}
	}

	recurse(key, []Record{}, allData, prt)
}

// Printer is a function that prints a record from a slice of Records.
type Printer func([]Record)

// recurse is a recurser to iterate over all the combinations of Records for a
// particular key.
func recurse(key string, recs []Record, remain []DataCollection, prt Printer) {

	if len(remain) == 0 {
		prt(recs)
		return
	}

	this := remain[0]
	thisRecords := this.data[key]

	if len(thisRecords) == 0 {
		recurse(key, recs, remain[1:], prt)
		return
	}

	for _, rec := range thisRecords {
		recurse(key, append(recs, rec), remain[1:], prt)
	}
}

// ReadAllInputSources reads all the readers, loading all data into
// DataCollections. Returns a list of distinct keys (across all inputs), and a
// list of all the DataCollections.
func ReadAllInputSources(readers []*csv.Reader, allHeaders [][]string, joinColumns []string) ([]string, []DataCollection) {

	keyMap := map[string]bool{}
	allData := []DataCollection{}

	for i, r := range readers {

		data := ReadData(r, allHeaders[i], joinColumns)

		for k := range data.data {
			keyMap[k] = true
		}

		allData = append(allData, data)
	}

	keys := []string{}
	for k := range keyMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys, allData
}

// ReadData reads a CSV input source collecting all the input into a DataCollection.
func ReadData(reader *csv.Reader, headers []string, joinColumns []string) DataCollection {

	recordOf := func(row []string) Record {

		r := Record{}

		for i, v := range row {
			n := headers[i]
			r[n] = v
		}

		return r
	}

	keyOf := func(rec Record) string {

		sb := strings.Builder{}

		for i, c := range joinColumns {
			if i > 0 {
				sb.WriteString("++")
			}
			sb.WriteString(rec[c])
		}

		return sb.String()
	}

	data := NewDataCollection()

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("failed to read/parse CSV input: %v", err)
		}

		rec := recordOf(row)
		key := keyOf(rec)

		data.Add(key, rec)
	}

	return data
}

// GetFileNames gets the list of file names from command line arguments. If no
// files named, prints usage message and aborts program.
func GetFileNames() []string {

	fileNames := os.Args[1:]

	if len(fileNames) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s f1.csv f2.csv ...\n", os.Args[0])
		os.Exit(1)
	}

	return fileNames
}

// OpenReaders opens all the named files and creates a CSV reader for each input
// source.
func OpenReaders(fileNames []string) []*csv.Reader {

	readers := []*csv.Reader{}

	for _, fName := range fileNames {

		r, err := os.Open(fName)
		if err != nil {
			log.Fatalf("cannot read CSV file %s: %v", fName, err)
		}

		readers = append(readers, csv.NewReader(r))
	}

	return readers
}

// GatherAllHeaders reads the firest line of each CSV reader, and returns the
// list of all header lists.
func GatherAllHeaders(readers []*csv.Reader, fileNames []string) [][]string {

	allHeaders := [][]string{}

	for i, r := range readers {

		header, err := r.Read()
		if err == io.EOF {
			log.Fatalf("CSV file %s has no headers. cannot process.", fileNames[i])
		}

		allHeaders = append(allHeaders, header)
	}

	return allHeaders
}

// IdentifyJoinColumns looks over all the headers of all the inputs and
// identifies which columns are in all the input sources.
func IdentifyJoinColumns(allHeaders [][]string) []string {

	headerCounts := map[string]int{}

	for _, header := range allHeaders {
		for _, col := range header {
			headerCounts[col]++
		}
	}

	joinColumns := []string{}
	for col, count := range headerCounts {
		if count == len(allHeaders) {
			joinColumns = append(joinColumns, col)
		}
	}

	if len(joinColumns) == 0 {
		log.Fatalf("cannot identify columns common to all input files to join")
	}

	return joinColumns
}

// IdentifyOutputColumns returns the unique columns across all the input
// sources.
func IdentifyOutputColumns(allHeaders [][]string) []string {

	outputFields := UniqueSlice{}
	for _, header := range allHeaders {
		for _, col := range header {
			outputFields.Append(col)
		}
	}

	return outputFields.GetSlice()
}

// DataCollection is a collection of records, mapped by key.
type DataCollection struct {
	data map[string][]Record
}

// NewDataCollection sets up a new DataCollection
func NewDataCollection() DataCollection {

	dc := DataCollection{}
	dc.data = map[string][]Record{}

	return dc
}

// Record is a set of data, mapped by column name.
type Record map[string]string

// Add appends another record to the data collection.
func (dc *DataCollection) Add(key string, rec Record) {

	cur := dc.data[key]
	dc.data[key] = append(cur, rec)
}

// UniqueSlice contains a slice of distinct strings.
type UniqueSlice struct {
	slice []string
}

// Append adds the string to the slice, only if not already present.
func (u *UniqueSlice) Append(s string) {
	for _, x := range u.slice {
		if x == s {
			return
		}
	}

	u.slice = append(u.slice, s)
}

// GetSlice returns the slice containing the unique values.
func (u *UniqueSlice) GetSlice() []string {
	return u.slice
}
