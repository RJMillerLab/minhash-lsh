package main

import (
	"experiment"
	"flag"
	"log"
	"path/filepath"
	"webtable"
)

const (
	minhashSeed = 1
)

var (
	tableListFile string
	outDir        string
	numThread     int
	sigSize       int
)

func init() {
	flag.StringVar(&tableListFile, "i", "LIST",
		"List of webtable files as a line-separated file")
	flag.StringVar(&outDir, "o", "MINHASH",
		"Output directory for the Minhash signatures")
	flag.IntVar(&numThread, "t", 32, "Number of threads")
	flag.IntVar(&sigSize, "s", 256, "Size of signature")
}

func hash(file string) ([]experiment.Attribute, error) {
	table, err := webtable.LoadTable(file)
	if err != nil {
		return nil, err
	}
	tableName := filepath.Base(file)
	return table.AttributesEstimatedCardinality(tableName, minhashSeed, sigSize), nil
}

func main() {
	flag.Parse()
	fs, err := experiment.LineChannel(tableListFile)
	if err != nil {
		log.Panic(err.Error())
	}
	experiment.CreateMinhashSignatures(fs, hash, numThread, outDir)
}
