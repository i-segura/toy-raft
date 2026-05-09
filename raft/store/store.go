package store

import (
	"encoding/json"
	"os"
)

type Store struct {
	path string

	data Data
}

func New(path string) (*Store, error) {
	p := &Store{
		path: path,
		data: Data{},
	}

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		raw = []byte("{}")
	}
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(raw, &p.data)
	if err != nil {
		return nil, err
	}

	return p, nil
}
