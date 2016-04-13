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
)

func init() {
	flag.StringVar(&tableListFile, "i", "LIST",
		"List of webtable files as a line-separated file")
	flag.StringVar(&outDir, "o", "DOMAIN",
		"Output directory for the domains")
	flag.IntVar(&numThread, "t", 64, "Number of threads")
}

func parse(file string) ([]experiment.Attribute, error) {
	table, err := webtable.LoadTable(file)
	if err != nil {
		return nil, err
	}
	tableName := filepath.Base(file)
	return table.AttributesDomainNoSignature(tableName), nil
}

func main() {
	flag.Parse()
	fs, err := experiment.LineChannel(tableListFile)
	if err != nil {
		log.Panic(err.Error())
	}
	experiment.CreateAttributeDomains(fs, parse, numThread, outDir)
}
