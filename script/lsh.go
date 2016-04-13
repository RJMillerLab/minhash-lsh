package main

import (
	"experiment"
	"flag"
	"log"
)

var (
	sigDir       string
	domainDir    string
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
	flag.StringVar(&domainDir, "d", "DOMAIN",
		"Directory of domains")
	flag.StringVar(&indexAttrCsv, "i", "ATTRIBUTE.csv",
		"CSV file of index attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&queryAttrCsv, "q", "ATTRIBUTE.csv",
		"CSV file of query attributes with format <table_name>, <column>, <cardinality>")
	flag.StringVar(&outFile, "o", "LSH.json",
		"Output file of the lsh result")
	flag.IntVar(&l, "l", 128, "Number of hash tables for LSH")
	flag.IntVar(&k, "k", 2, "Size of hash key for LSH")
	nWorker = 200
	ks = []int{10, 20, 30, 40, 50}
}

func main() {
	flag.Parse()
	// Load attributes
	log.Print("Loading the index attributes")
	attributes, err := experiment.ReadAttributeSignatures(indexAttrCsv, sigDir)
	if err != nil {
		log.Panic(err.Error())
	}
	indexDomains, err := experiment.ReadAttributeDomains(indexAttrCsv, domainDir)
	if err != nil {
		log.Panic(err.Error())
	}
	queryDomains, err := experiment.ReadAttributeDomains(queryAttrCsv, domainDir)
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
	for i, k := range ks {
		queryFunc := func(q experiment.Attribute) experiment.QueryResult {
			r := make(chan int)
			go func() {
				index.Query(q.Signature, l, r)
				close(r)
			}()
			neighbours := make([]int, 0)
			for i := range r {
				neighbours = append(neighbours, i)
			}
			candidates := make([]experiment.Attribute, len(neighbours))
			for i, id := range neighbours {
				candidates[i] = indexDomains[id]
			}
			queryDomain := queryDomains[q.Id]
			scan := experiment.NewLinearscan(candidates)
			topk := scan.TopK(&queryDomain, k, 0.0, experiment.ExactResemblance)
			topkNeighbours := make([]int, len(topk))
			for i := range topk {
				topkNeighbours[i] = topk[i].Id
			}
			return experiment.QueryResult{
				Id:         q.Id,
				Neighbours: topkNeighbours,
			}
		}
		queries := experiment.AttributeSignatureChannel(queryAttrCsv, sigDir)
		result.Results[i] = experiment.ParallelQuery(queries, queryFunc, nWorker)
	}
	err = experiment.DumpJson(outFile, result)
	log.Printf("Result output to %s", outFile)
}
