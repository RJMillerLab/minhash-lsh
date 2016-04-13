package experiment

import "sync"

type QueryFunc func(Attribute) QueryResult

func ParallelQuery(queries chan Attribute, queryFunc QueryFunc, nWorker int) QueryResults {
	var wg sync.WaitGroup
	// Worker threads will write results to this channel
	// before they exit
	queryResults := make(chan QueryResult)
	wg.Add(nWorker)
	for i := 0; i < nWorker; i++ {
		go func(workerId int) {
			for q := range queries {
				r := queryFunc(q)
				queryResults <- r
			}
			wg.Done()
		}(i)
	}
	// Waiting thread for the workers, close the output channel when
	// all workers exit
	go func() {
		wg.Wait()
		close(queryResults)
	}()
	// Merge all query results from workers
	completeQueryResults := make(QueryResults, 0)
	for r := range queryResults {
		completeQueryResults = append(completeQueryResults, r)
	}
	return completeQueryResults
}
