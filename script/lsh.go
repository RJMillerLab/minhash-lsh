package main

import (
	"experiment"
	"flag"
	"log"
	"time"
)

var (
	sigDir       string
	indexAttrCsv string
	queryAttrCsv string
	nWorker      int
	outFile      string
	ks           []int
	l            int
	k            int
)

func init() {
	flag.StringVar(&sigDir, "s", "MINHASH",
		"Directory of minhash signatures")
	flag.StringVar(&indexAttrCsv, "i", "ATTRIBUTE.csv",
		"CSV file of index attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&queryAttrCsv, "q", "ATTRIBUTE.csv",
		"CSV file of query attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&outFile, "o", "LSH.json",
		"Output file of the lsh result")
	flag.IntVar(&l, "l", 256, "Number of hash tables for LSH")
	flag.IntVar(&k, "k", 2, "Size of hash key for LSH")
	nWorker = 200
	ks = []int{10}
}

func main() {
	flag.Parse()
	// Load attributes
	log.Print("Loading the index attributes")
	attributes, err := experiment.ReadAttributeSignatures(indexAttrCsv, sigDir)
	if err != nil {
		log.Panic(err.Error())
	}
	// Initialize result
	result := experiment.NewAccuracyVsK(ks)
	index, dur, err := experiment.Lsh(attributes, l, k)
	if err != nil {
		log.Panic(err.Error())
	}
	log.Printf("Indexing finished in %.4f seconds", dur)
	for i, _ := range ks {
		queryFunc := func(q experiment.Attribute) experiment.QueryResult {
			r := make(chan int)
			start := time.Now().UnixNano()
			go func() {
				index.Query(q.Signature, l, r)
				close(r)
			}()
			end := time.Now().UnixNano()
			diff := end - start
			neighbours := make([]int, 0)
			for i := range r {
				neighbours = append(neighbours, i)
			}
			return experiment.QueryResult{
				Time: float64(diff),
			}
		}
		queries := experiment.AttributeSignatureChannel(queryAttrCsv, sigDir)
		result.Results[i] = experiment.ParallelQuery(queries, queryFunc, nWorker)
	}
	err = experiment.DumpJson(outFile, result)
	log.Printf("Result output to %s", outFile)
}
