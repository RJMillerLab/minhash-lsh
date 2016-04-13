package main

import (
	"experiment"
	"flag"
)

const (
	randomSeed = 12
)

var (
	attributeCsvFile string
	outFile          string
	subsetSize       int
)

func init() {
	flag.StringVar(&attributeCsvFile, "i", "ATTRIBUTE.csv",
		"List of items as a line-separated file")
	flag.StringVar(&outFile, "o", "SUBSET.csv",
		"List of selected as a line-separated file")
	flag.IntVar(&subsetSize, "s", 1000, "Size of selection")
}

func main() {
	flag.Parse()
	experiment.CreateAttributeSubset(attributeCsvFile, outFile, subsetSize, randomSeed)
}
