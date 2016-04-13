package webtable

import (
	"encoding/json"
	"io/ioutil"
)

func LoadTable(file string) (table *Table, err error) {
	table = &Table{}
	err = loadJson(file, table)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func loadJson(file string, v interface{}) (err error) {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buffer, v)
	if err != nil {
		return err
	}
	return nil
}
