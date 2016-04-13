package experiment

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"minhash"
	"os"
	"path/filepath"
	"strconv"
)

type AttributeGenerator struct {
	sigDir    string
	domainDir string
}

func NewAttributeGenerator(sigDir, domainDir string) *AttributeGenerator {
	return &AttributeGenerator{
		sigDir:    sigDir,
		domainDir: domainDir,
	}
}

func (ag *AttributeGenerator) LoadSignature(a *Attribute) error {
	if ag.sigDir == "" {
		return errors.New("Signature directory is not initialized")
	}
	sigFile := filepath.Join(ag.sigDir, a.TableName,
		strconv.Itoa(a.Column))
	b, err := ioutil.ReadFile(sigFile)
	if err != nil {
		return err
	}
	a.Signature = minhash.DeserializeSignature(b)
	return nil
}

func (ag *AttributeGenerator) LoadDomain(a *Attribute) error {
	if ag.domainDir == "" {
		return errors.New("Domain directory is not initialized")
	}
	domainFile := filepath.Join(ag.domainDir, a.TableName,
		strconv.Itoa(a.Column))
	a.Domain = make(map[string]bool)
	in, err := os.Open(domainFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		v := scanner.Text()
		err = scanner.Err()
		if err != nil {
			return err
		}
		a.Domain[v] = true
	}
	err = in.Close()
	if err != nil {
		return err
	}
	return nil
}

func (ag *AttributeGenerator) NewAttribute(id int, tableName string, column int) (Attribute, error) {
	a := Attribute{
		Id:        id,
		TableName: tableName,
		Column:    column,
	}
	var err error
	if ag.domainDir != "" {
		err = ag.LoadDomain(&a)
		if err != nil {
			return a, err
		}
		a.Cardinality = len(a.Domain)
	}
	if ag.sigDir != "" {
		err = ag.LoadSignature(&a)
		if err != nil {
			return a, err
		}
		if a.Cardinality == 0 {
			a.Cardinality = a.Signature.Cardinality()
		}
	}
	return a, nil
}

func getCols(tableDir string) ([]int, error) {
	fs, err := ioutil.ReadDir(tableDir)
	if err != nil {
		return nil, err
	}
	cols := make([]int, 0, len(fs))
	for _, f := range fs {
		col, err := strconv.Atoi(f.Name())
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}
func (ag *AttributeGenerator) NewAttributes(tableName string) ([]Attribute, error) {
	var tableDir string
	if ag.sigDir != "" {
		tableDir = filepath.Join(ag.sigDir, tableName)
	} else if ag.domainDir != "" {
		tableDir = filepath.Join(ag.domainDir, tableName)
	} else {
		return nil, errors.New("No table directory set.")
	}
	cols, err := getCols(tableDir)
	if err != nil {
		return nil, err
	}
	attrs := make([]Attribute, 0, len(cols))
	for _, col := range cols {
		a, err := ag.NewAttribute(0, tableName, col)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, a)
	}
	return attrs, nil
}

func ReadAttributeCsvRecord(line []string) (tableName string, column, card int, err error) {
	tableName = line[0]
	column, err = strconv.Atoi(line[1])
	if err != nil {
		return
	}
	card, err = strconv.Atoi(line[2])
	if err != nil {
		return
	}
	return
}

// AttributeChannel takes a attribute CSV file as input and produce
// a channel of Attribute with Signature and Domain field empty.
func AttributeChannel(csvFile string) (attrs chan Attribute) {
	attrs = make(chan Attribute)
	go func() {
		in, err := os.Open(csvFile)
		if err != nil {
			log.Panic(err.Error())
		}
		// Read CSV file
		csvReader := csv.NewReader(in)
		id := 0
		for {
			line, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Panic(err.Error())
			}
			tableName, column, card, err := ReadAttributeCsvRecord(line)
			if err != nil {
				log.Panic(err.Error())
			}
			attrs <- Attribute{
				Id:          id,
				Column:      column,
				TableName:   tableName,
				Cardinality: card,
			}
			id++
		}
		err = in.Close()
		if err != nil {
			log.Panic(err.Error())
		}
		close(attrs)
	}()
	return attrs
}

// AttributeSignatureChannel creates a channel of Attribute with non-nil
// signatures by scanning the attribute CSV file and the signature directory.
func AttributeSignatureChannel(csvFile, sigDir string) (attrs chan Attribute) {
	attrs = make(chan Attribute)
	go func() {
		in, err := os.Open(csvFile)
		if err != nil {
			log.Panic(err.Error())
		}
		// Read CSV file
		csvReader := csv.NewReader(in)
		id := 0
		ag := &AttributeGenerator{
			sigDir: sigDir,
		}
		for {
			line, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Panic(err.Error())
			}
			tableName, column, card, err := ReadAttributeCsvRecord(line)
			if err != nil {
				log.Panic(err.Error())
			}
			a, err := ag.NewAttribute(id, tableName, column)
			a.Cardinality = card
			if err != nil {
				log.Panic(err.Error())
			}
			attrs <- a
			id++
		}
		err = in.Close()
		if err != nil {
			log.Panic(err.Error())
		}
		close(attrs)
	}()
	return attrs
}

// AttributeDomainChannel creates a channel of Attribute with non-nil Domain.
func AttributeDomainChannel(csvFile, domainDir string) (attrs chan Attribute) {
	attrs = make(chan Attribute)
	go func() {
		in, err := os.Open(csvFile)
		if err != nil {
			log.Panic(err.Error())
		}
		// Read CSV file
		csvReader := csv.NewReader(in)
		id := 0
		ag := &AttributeGenerator{
			domainDir: domainDir,
		}
		for {
			line, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Panic(err.Error())
			}
			tableName, column, _, err := ReadAttributeCsvRecord(line)
			if err != nil {
				log.Panic(err.Error())
			}
			a, err := ag.NewAttribute(id, tableName, column)
			if err != nil {
				log.Panic(err.Error())
			}
			attrs <- a
			id++
		}
		err = in.Close()
		if err != nil {
			log.Panic(err.Error())
		}
		close(attrs)
	}()
	return attrs
}

// ReadAttributes loads attributes into memory,
// and returns a slice of Attribute structs with nil Signature and Domain.
func ReadAttributes(attrCsv string) ([]Attribute, error) {
	size, err := CountLine(attrCsv)
	if err != nil {
		return nil, err
	}
	attrs := make([]Attribute, 0, size)
	ac := AttributeChannel(attrCsv)
	for a := range ac {
		attrs = append(attrs, a)
	}
	return attrs, nil
}

// ReadAttributeSignatures loads attribute signatures from the disk into memory,
// and returns a slice of Attribute structs with non-nil Signature.
func ReadAttributeSignatures(attrCsv string, sigDir string) ([]Attribute, error) {
	size, err := CountLine(attrCsv)
	if err != nil {
		return nil, err
	}
	attrs := make([]Attribute, 0, size)
	ac := AttributeSignatureChannel(attrCsv, sigDir)
	for a := range ac {
		attrs = append(attrs, a)
	}
	return attrs, nil
}

// ReadAttributeDomains loads attribute signatures from the disk into memory,
// and returns a slice of Attribute structs with non-nil Domain.
func ReadAttributeDomains(attrCsv string, domainDir string) ([]Attribute, error) {
	size, err := CountLine(attrCsv)
	if err != nil {
		return nil, err
	}
	attrs := make([]Attribute, 0, size)
	ac := AttributeDomainChannel(attrCsv, domainDir)
	for a := range ac {
		attrs = append(attrs, a)
	}
	return attrs, nil
}
