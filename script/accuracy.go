package main

import (
	"experiment"
	"flag"
)

var (
	groundTruthFile string
	annFile         string
	outFile         string
)

func init() {
	flag.StringVar(&groundTruthFile, "g", "GROUNDTRUTH.json",
		"The query results of ground truth")
	flag.StringVar(&annFile, "a", "LSH.json",
		"The query results of LSH Ensemble")
	flag.StringVar(&outFile, "o", "ACCURACY.json",
		"Output file for accuracy analysis result")
}

func main() {
	flag.Parse()
	experiment.AccuracyVsKAnalysis(annFile, groundTruthFile, outFile)
}
