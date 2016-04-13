package experiment

import "log"

type AnalysisResult struct {
	Recalls    []float64 `json:"recalls"`
	Precisions []float64 `json:"precisions"`
}

type AccuracyVsKAnalysisResult struct {
	Ks      []int            `json:"ks"`
	Results []AnalysisResult `json:"results"`
}

func RecallPrecision(ann, groundTruth QueryResult) (recall, precision float64) {
	if len(groundTruth.Neighbours) == 0 {
		return 1.0, 1.0
	}
	if len(ann.Neighbours) == 0 {
		return 0.0, 0.0
	}
	truth := make(map[int]bool)
	for _, n := range groundTruth.Neighbours {
		truth[n] = true
	}
	overlap := 0
	for _, n := range ann.Neighbours {
		if _, found := truth[n]; found {
			overlap += 1
		}
	}
	recall = float64(overlap) / float64(len(groundTruth.Neighbours))
	precision = float64(overlap) / float64(len(ann.Neighbours))
	return
}

func Analysis(qr, gt QueryResults) (ar AnalysisResult) {
	gtmap := make(map[int]*QueryResult)
	for i := range gt {
		gtmap[gt[i].Id] = &gt[i]
	}
	ar = AnalysisResult{
		Recalls:    make([]float64, 0, len(qr)),
		Precisions: make([]float64, 0, len(qr)),
	}
	for i := range qr {
		groundTruth, exist := gtmap[qr[i].Id]
		if !exist {
			log.Panicf("Ground Truth does not have query with Id %d", qr[i].Id)
		}
		r, p := RecallPrecision(qr[i], *groundTruth)
		if len(qr[i].Neighbours) != 0 {
			ar.Precisions = append(ar.Precisions, p)
		}
		ar.Recalls = append(ar.Recalls, r)
	}
	return ar
}

func AccuracyVsKAnalysis(qFile, gFile, outFile string) {
	var annResults AccuracyVsK
	var groundTruthResults AccuracyVsK
	err := LoadJson(qFile, &annResults)
	if err != nil {
		log.Panic(err.Error())
	}
	err = LoadJson(gFile, &groundTruthResults)
	if err != nil {
		log.Panic(err.Error())
	}
	if len(groundTruthResults.Ks) != len(annResults.Ks) {
		log.Panic("Groud truth result number of thresholds mismatch")
	}
	o := AccuracyVsKAnalysisResult{
		Ks:      annResults.Ks,
		Results: make([]AnalysisResult, len(annResults.Results)),
	}
	for i, r := range annResults.Results {
		o.Results[i] = Analysis(r, groundTruthResults.Results[i])
	}
	err = DumpJson(outFile, &o)
	if err != nil {
		log.Panic(err.Error())
	}
}
