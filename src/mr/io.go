package mr

import (
	"encoding/json"
	"errors"
	"io"
	"os"
)

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return content, err
}

func writeKV(writer io.Writer, intermediate []KeyValue) error {
	enc := json.NewEncoder(writer)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func readKV(file *os.File, kva *[]KeyValue) error {
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		*kva = append(*kva, kv)
	}
	return nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
