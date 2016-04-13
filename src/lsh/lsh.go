package lsh

import (
	"errors"
	"fmt"
	"minhash"
)

type Lsh struct {
	l          int // number of hash tables
	k          int // length of combined hash
	hashtables [](map[string]([]int))
}

func NewLsh(l, k int) *Lsh {
	hashtables := make([](map[string]([]int)), l)
	for i := range hashtables {
		hashtables[i] = make(map[string]([]int))
	}
	return &Lsh{
		l:          l,
		k:          k,
		hashtables: hashtables,
	}
}

// Insert adds a new key to the LSH
func (lsh *Lsh) Insert(key int, sig minhash.Signature) error {
	if len(sig) < lsh.l*lsh.k {
		return errors.New("Signature size too small!")
	}
	for i := 0; i < lsh.l; i++ {
		H := toString(sig[i*lsh.k : (i+1)*lsh.k])
		if _, exist := lsh.hashtables[i][H]; exist {
			lsh.hashtables[i][H] = append(lsh.hashtables[i][H], key)
		} else {
			lsh.hashtables[i][H] = make([]int, 1)
			lsh.hashtables[i][H][0] = key
		}
	}
	return nil
}

func (lsh *Lsh) Query(sig minhash.Signature, numTables int, out chan int) error {
	if numTables > lsh.l {
		return errors.New("Number of hash tables to query exceed total number of hash tables")
	}
	if len(sig) < numTables*lsh.k {
		return errors.New("Signature size too small!")
	}
	// Keep track of keys seen
	seens := make(map[int]bool)
	for i := 0; i < numTables; i++ {
		H := toString(sig[i*lsh.k : (i+1)*lsh.k])
		if candidates, exist := lsh.hashtables[i][H]; exist {
			for _, key := range candidates {
				if _, seen := seens[key]; seen {
					continue
				}
				seens[key] = true
				out <- key
			}
		}
	}
	return nil
}

func toString(sig minhash.Signature) string {
	s := ""
	for _, v := range sig {
		s += fmt.Sprintf("%.16x", v)
	}
	return s
}
