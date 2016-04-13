package experiment

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"minhash"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ParseTable takes a table file and parse it into
// a list of Attribute
type ParseTable func(file string) ([]Attribute, error)

// CreateAttributeCsv takes a chanel of table files,
// create a CSV file of attributes
func CreateAttributeCsv(files chan string, parser ParseTable, numThread int, outFile string) {
	attrs := make(chan Attribute)
	var wg sync.WaitGroup
	wg.Add(numThread)
	for i := 0; i < numThread; i++ {
		go func() {
			for f := range files {
				attributes, err := parser(f)
				if err != nil {
					log.Printf("Error parsing table %s: %s", f, err.Error())
					continue
				}
				for _, a := range attributes {
					attrs <- a
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(attrs)
	}()

	out, err := os.Create(outFile)
	if err != nil {
		log.Panic(err.Error())
	}
	csvWriter := csv.NewWriter(out)
	var count int
	for a := range attrs {
		count++
		err = csvWriter.Write([]string{a.TableName,
			strconv.Itoa(a.Column),
			strconv.Itoa(a.Cardinality)})
		if err != nil {
			log.Panic(err.Error())
		}
		if count%10 == 0 {
			fmt.Printf("\rCurrently parsed %d attributes", count)
		}
	}
	fmt.Print("\n")
	csvWriter.Flush()
	err = out.Close()
	if err != nil {
		log.Panic(err.Error())
	}
}

// CreateAttributeDomains takes a channel of data files and
// create domain files
func CreateAttributeDomains(files chan string, parser ParseTable, numThread int, outDir string) {
	err := os.Mkdir(outDir, 0777)
	if err != nil {
		log.Panic(err.Error())
	}
	var wg sync.WaitGroup
	signals := make(chan bool)
	wg.Add(numThread)
	for i := 0; i < numThread; i++ {
		go func() {
			for f := range files {
				attributes, err := parser(f)
				if err != nil {
					log.Printf("Error parsing table %s: %s", f, err.Error())
					continue
				}
				tableDir := filepath.Join(outDir, filepath.Base(f))
				err = os.Mkdir(tableDir, 0777)
				if err != nil {
					log.Panicf("Cannot create table directory %s",
						tableDir)
				}
				for _, a := range attributes {
					outFile := filepath.Join(tableDir,
						strconv.Itoa(a.Column))
					// Write domain to file
					out, err := os.Create(outFile)
					if err != nil {
						log.Panic(err.Error())
					}
					writer := bufio.NewWriter(out)
					for v := range a.Domain {
						_, err = writer.WriteString(v + "\n")
						if err != nil {
							log.Panic(err.Error())
						}
					}
					writer.Flush()
					err = out.Close()
					if err != nil {
						log.Panic(err.Error())
					}
					signals <- true
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(signals)
	}()
	var count int
	for _ = range signals {
		count += 1
		fmt.Printf("\rCurrently created %d attribute domains", count)
	}
	fmt.Print("\n")
}

// CreateMinhashSignatures takes a channel of data files,
// create minhash signatures of attributes.
func CreateMinhashSignatures(files chan string, parser ParseTable, numThread int, outDir string) {
	err := os.Mkdir(outDir, 0777)
	if err != nil {
		log.Panic(err.Error())
	}
	var wg sync.WaitGroup
	signals := make(chan bool)
	wg.Add(numThread)
	for i := 0; i < numThread; i++ {
		go func() {
			for f := range files {
				attributes, err := parser(f)
				if err != nil {
					log.Printf("Error parsing table %s: %s", f, err.Error())
					continue
				}
				tableDir := filepath.Join(outDir, filepath.Base(f))
				err = os.Mkdir(tableDir, 0777)
				if err != nil {
					log.Panicf("Cannot create table directory %s",
						tableDir)
				}
				for _, a := range attributes {
					outFile := filepath.Join(tableDir,
						strconv.Itoa(a.Column))
					buf := minhash.SerializeSignature(a.Signature)
					err := ioutil.WriteFile(outFile, buf, 0777)
					if err != nil {
						panic(err.Error())
					}
					signals <- true
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(signals)
	}()
	var count int
	for _ = range signals {
		count += 1
		fmt.Printf("\rCurrently hashed %d attributes", count)
	}
	fmt.Print("\n")
}
