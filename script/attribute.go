package main

import (
	"experiment"
	"flag"
	"log"
	"path/filepath"
	"webtable"
)

var (
	tableListFile string
	outFile       string
	numThread     int
)

func init() {
	flag.StringVar(&tableListFile, "i", "LIST",
		"List of webtable files as a line-separated file")
	flag.StringVar(&outFile, "o", "ATTRIBUTE.csv",
		"Result output as a CSV file <table_name>, <column>, <cardinality>")
	flag.IntVar(&numThread, "t", 32, "Number of threads")
}

func tableParser(file string) ([]experiment.Attribute, error) {
	table, err := webtable.LoadTable(file)
	if err != nil {
		return nil, err
	}
	tableName := filepath.Base(file)
	return table.AttributesExactCardinalityNoSignature(tableName), nil
}

func main() {
	flag.Parse()
	fs, err := experiment.LineChannel(tableListFile)
	if err != nil {
		log.Panic(err.Error())
	}
	experiment.CreateAttributeCsv(fs, tableParser, numThread, outFile)
}
