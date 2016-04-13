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
	ks           []int
)

func init() {
	flag.StringVar(&domainDir, "d", "DOMAIN",
		"Directory of domains")
	flag.StringVar(&indexAttrCsv, "i", "ATTRIBUTE.csv",
		"CSV file of index attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&queryAttrCsv, "q", "ATTRIBUTE.csv",
		"CSV file of query attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&outFile, "o", "GROUNDTRUTH.json",
		"Output file of the groud truth result")
	nWorker = 200
	ks = []int{10, 20, 30, 40, 50}
}

func main() {
	flag.Parse()
	// Load attributes
	log.Print("Loading the index attributes")
	attributes, err := experiment.ReadAttributeDomains(indexAttrCsv, domainDir)
	if err != nil {
		log.Panic(err.Error())
	}
	// Initialize result
	result := experiment.NewAccuracyVsK(ks)
	linearscan := experiment.NewLinearscan(attributes)
	for i, k := range ks {
		queryFunc := func(q experiment.Attribute) experiment.QueryResult {
			candidates := linearscan.TopK(&q, k, 0.0, experiment.ExactResemblance)
			neighbours := make([]int, len(candidates))
			for i := range candidates {
				neighbours[i] = candidates[i].Id
			}
			return experiment.QueryResult{
				Id:         q.Id,
				Neighbours: neighbours,
			}
		}
		queries := experiment.AttributeDomainChannel(queryAttrCsv, domainDir)
		result.Results[i] = experiment.ParallelQuery(queries, queryFunc, nWorker)
	}
	err = experiment.DumpJson(outFile, result)
	log.Printf("Result output to %s", outFile)
}
