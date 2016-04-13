package webtable

import (
	"experiment"
	"minhash"
)

type Column []string

type Table struct {
	Relation                         []Column `json:"relation"`
	PageTitle                        string   `json:"pageTitle"`
	Title                            string   `json:"title"`
	Url                              string   `json:"url"`
	HasHeader                        bool     `json:"hashHeader"`
	HeaderPosition                   string   `json:"headerPosition"`
	TableType                        string   `json:"tableType"`
	TableNum                         int      `json:"tableNum"`
	S3Link                           string   `json:"s3Link"`
	RecordEndOffset                  int      `json:"recordEndOffset"`
	RecordOffset                     int      `json:"recordOffset"`
	TableOrientation                 string   `json:"tableOrientation"`
	TableContextTimeStampBeforeTable string   `json:"TableContextTimeStampBeforeTable"`
	TableContextTimeStampAfterTable  string   `json:"TableContextTimeStampAfterTable"`
	LastModified                     string   `json:"lastModified"`
	TextBeforeTable                  string   `json:"textBeforeTable"`
	TextAfterTable                   string   `json:"textAfterTable"`
	HasKeyColumn                     bool     `json:"hasKeyColumn"`
	KeyColumnIndex                   int      `json:"keyColumnIndex"`
	HeaderRowIndex                   int      `json:"headerRowIndex"`
}

func (c Column) Cardinality() int {
	s := make(map[string]bool)
	for _, v := range c {
		s[v] = true
	}
	return len(s)
}

func (t Table) AttributesEstimatedCardinality(tableName string, minhashSeed, sigSize int) []experiment.Attribute {
	attrs := make([]experiment.Attribute, len(t.Relation))
	for i, col := range t.Relation {
		m := minhash.NewMinhash(minhashSeed, sigSize)
		for _, v := range col {
			m.Push([]byte(v))
		}
		sig := m.Signature()
		attrs[i] = experiment.Attribute{
			Column:      i,
			TableName:   tableName,
			Cardinality: m.Cardinality(),
			Signature:   sig,
		}
	}
	return attrs
}

func (t Table) AttributesExactCardinalityNoSignature(tableName string) []experiment.Attribute {
	attrs := make([]experiment.Attribute, len(t.Relation))
	for i, col := range t.Relation {
		card := col.Cardinality()
		attrs[i] = experiment.Attribute{
			Column:      i,
			TableName:   tableName,
			Cardinality: card,
			Signature:   nil,
		}
	}
	return attrs
}

func (t Table) AttributesDomainNoSignature(tableName string) []experiment.Attribute {
	attrs := make([]experiment.Attribute, len(t.Relation))
	for i, col := range t.Relation {
		d := make(map[string]bool)
		for _, v := range col {
			d[v] = true
		}
		attrs[i] = experiment.Attribute{
			Column:      i,
			TableName:   tableName,
			Cardinality: len(d),
			Domain:      d,
		}
	}
	return attrs
}
