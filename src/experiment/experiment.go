package experiment

import (
	"errors"
	"minhash"
	"sort"
)

type Attribute struct {
	Id          int
	TableName   string
	Column      int
	Cardinality int
	Signature   minhash.Signature
	Domain      map[string]bool
}

type Attributes []Attribute

func (r Attributes) Len() int           { return len(r) }
func (r Attributes) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Attributes) Less(i, j int) bool { return r[i].Id < r[j].Id }

type AttributesByCardinality []Attribute

func (r AttributesByCardinality) Len() int           { return len(r) }
func (r AttributesByCardinality) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r AttributesByCardinality) Less(i, j int) bool { return r[i].Cardinality < r[j].Cardinality }

func (r AttributesByCardinality) Subset(lower, upper int) ([]Attribute, error) {
	if !sort.IsSorted(r) {
		return nil, errors.New("Must be sorted by cardinality first")
	}
	start, end := -1, -1
	for i := range r {
		if start == -1 && r[i].Cardinality >= lower {
			start = i
		}
		if end == -1 && (r[i].Cardinality > upper || i == len(r)-1) {
			end = i
			break
		}
	}
	if start == -1 || end == -1 {
		return nil, errors.New("Cannot find such cardinality range")
	}
	if end == len(r)-1 {
		end++
	}
	return []Attribute(r[start:end]), nil
}
