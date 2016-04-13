package experiment

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"os"
)

func LoadJson(file string, v interface{}) error {
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

func DumpJson(file string, v interface{}) error {
	buffer, err := json.Marshal(v)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(file, buffer, 0664)
	if err != nil {
		return err
	}
	return nil
}

func CountLine(file string) (count int, err error) {
	f, err := os.Open(file)
	if err != nil {
		return
	}
	fileScanner := bufio.NewScanner(f)
	for fileScanner.Scan() {
		count++
	}
	err = f.Close()
	return
}

func LineChannel(file string) (chan string, error) {
	in, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(in)
	fs := make(chan string)
	go func() {
		for s.Scan() {
			fs <- s.Text()
		}
		if s.Err() != nil {
			panic(s.Err().Error())
		}
		close(fs)
		in.Close()
	}()
	return fs, nil
}
