package experiment

type QueryResult struct {
	Id          int     `json:"id"`
	TableName   string  `json:"table_name"`
	Column      int     `json:"column"`
	Cardinality int     `json:"cardinality"`
	Neighbours  []int   `json:"neighbours"`
	Time        float64 `json:"time"`
}

type QueryResults []QueryResult

func (r QueryResults) Len() int           { return len(r) }
func (r QueryResults) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r QueryResults) Less(i, j int) bool { return r[i].Id < r[j].Id }

type AccuracyVsK struct {
	Ks      []int          `json:"ks"`
	Results []QueryResults `json:"results"`
}

func NewAccuracyVsK(ks []int) *AccuracyVsK {
	return &AccuracyVsK{
		Ks:      ks,
		Results: make([]QueryResults, len(ks)),
	}
}
