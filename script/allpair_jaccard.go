package main

import (
	"experiment"
	"flag"
	"log"
)

var (
	domainDir    string
	indexAttrCsv string
	queryAttrCsv string
	nWorker      int
	outFile      string
)

func init() {
	flag.StringVar(&domainDir, "d", "DOMAIN",
		"Directory of domains")
	flag.StringVar(&indexAttrCsv, "i", "ATTRIBUTE.csv",
		"CSV file of index attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&outFile, "o", "ALL_PAIR_JACCARD.json",
		"Output file of the all pair Jaccard distances")
	nWorker = 200
}

type AllPairJaccard [][]float64

func main() {
	flag.Parse()
	// Load attributes
	log.Print("Loading the index attributes")
	attributes, err := experiment.ReadAttributeDomains(indexAttrCsv, domainDir)
	if err != nil {
		log.Panic(err.Error())
	}
	// Initialize result
	result := make(AllPairJaccard, len(attributes))
	for i, q := range attributes {
		dists := make([]float64, len(attributes))
		for j, a := range attributes {
			_, jaccard := experiment.ExactResemblance(&q, &a)
			dists[j] = 1.0 - jaccard
		}
		result[i] = dists
	}
	err = experiment.DumpJson(outFile, &result)
	log.Printf("Result output to %s", outFile)
}
