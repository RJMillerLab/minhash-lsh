package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

var (
	dataRoot string
	outFile  string
)

func init() {
	flag.StringVar(&dataRoot, "r", "WEBTABLE2015",
		"Parent directory of webtables")
	flag.StringVar(&outFile, "o", "LIST",
		"Output the list of webtables as a line-separated file")
}

func getDataDirs(dataRoot string) (dataDirs []string) {
	dataDirs = make([]string, 0)
	for i := 0; i <= 50; i++ {
		dataDirs = append(dataDirs, filepath.Join(dataRoot, strconv.Itoa(i)))
	}
	return
}

func tableFiles(dataDirs []string) (files chan string) {
	files = make(chan string, 100)
	go func() {
		for _, dataDir := range dataDirs {
			fs, err := ioutil.ReadDir(dataDir)
			if err != nil {
				log.Panic(err.Error())
			}
			for _, f := range fs {
				if f.IsDir() {
					continue
				}
				files <- filepath.Join(dataDir, f.Name())
			}
		}
		close(files)
	}()
	return files
}

func main() {
	flag.Parse()
	dataDirs := getDataDirs(dataRoot)
	fs := tableFiles(dataDirs)
	out, err := os.Create(outFile)
	if err != nil {
		log.Panic(err.Error())
	}
	w := bufio.NewWriter(out)
	var count int
	for f := range fs {
		count += 1
		_, err = w.WriteString(f + "\n")
		if err != nil {
			log.Panic(err.Error())
		}
		if count%10 == 0 {
			fmt.Printf("\rCurrently collected %d tables", count)
		}
	}
	w.Flush()
	out.Close()
}
