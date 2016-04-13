package experiment

import (
	"errors"
	"lsh"
	"time"
)

func Lsh(attributes []Attribute, l, k int) (index *lsh.Lsh, dur float64, err error) {
	if len(attributes) == 0 {
		return nil, dur, errors.New("Attribute slice is empty")
	}
	start := time.Now()
	index = lsh.NewLsh(l, k)
	for _, a := range attributes {
		err = index.Insert(a.Id, a.Signature)
		if err != nil {
			return nil, dur, err
		}
	}
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	return index, dur, nil
}
