package experiment

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
)

// CreateAttributeSubset creats a random subset of the attributes
// and output the subset as a new attribute CSV file.
func CreateAttributeSubset(attributeCsvFile, outFile string, subsetSize, randomSeed int) {
	numAttrs, err := CountLine(attributeCsvFile)
	if err != nil {
		log.Panic(err.Error())
	}
	if subsetSize > numAttrs {
		log.Panicf("Selection size %d is less than number of tables %d",
			subsetSize, numAttrs)
	}
	r := rand.New(rand.NewSource(int64(randomSeed)))
	inds := r.Perm(numAttrs)[:subsetSize]
	sort.Ints(inds)

	in, err := os.Open(attributeCsvFile)
	if err != nil {
		log.Panic(err.Error())
	}
	out, err := os.Create(outFile)
	if err != nil {
		log.Panic(err.Error())
	}

	inScanner := bufio.NewScanner(in)
	outWriter := bufio.NewWriter(out)
	i := 0
	j := 0
	for inScanner.Scan() {
		if inds[j] == i {
			_, err = outWriter.Write(inScanner.Bytes())
			if err != nil {
				log.Panic(err.Error())
			}
			outWriter.WriteString("\n")
			j++
			if j%10 == 0 {
				fmt.Printf("\rCurrently selected %d lines", j)
			}
			if j >= len(inds) {
				fmt.Printf("\n")
				break
			}
		}
		i++
	}
	err = outWriter.Flush()
	if err != nil {
		log.Panic(err.Error())
	}
	err = out.Close()
	if err != nil {
		log.Panic(err.Error())
	}
	err = in.Close()
	if err != nil {
		log.Panic(err.Error())
	}
}

type AttributeFilter func(*Attribute) bool

// CreateAttributeFilteredSubset creates a subset of attributes based on
// some filter condititon,
// and output the subset as a new attribute CSV file.
func CreateAttributeFilteredSubset(attributeCsvFile, outFile string, filterFunc AttributeFilter) {
	in, err := os.Open(attributeCsvFile)
	if err != nil {
		log.Panic(err.Error())
	}
	out, err := os.Create(outFile)
	if err != nil {
		log.Panic(err.Error())
	}

	inReader := csv.NewReader(in)
	outWriter := csv.NewWriter(out)
	for {
		line, err := inReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err.Error())
		}
		tableName := line[0]
		column, err := strconv.Atoi(line[1])
		if err != nil {
			log.Panic(err.Error())
		}
		card, err := strconv.Atoi(line[2])
		if err != nil {
			log.Panic(err.Error())
		}
		attr := Attribute{
			Column:      column,
			TableName:   tableName,
			Cardinality: card,
		}
		if !filterFunc(&attr) {
			continue
		}
		err = outWriter.Write(line)
		if err != nil {
			log.Panic(err.Error())
		}
	}
	outWriter.Flush()
	err = out.Close()
	if err != nil {
		log.Panic(err.Error())
	}
	err = in.Close()
	if err != nil {
		log.Panic(err.Error())
	}
}
