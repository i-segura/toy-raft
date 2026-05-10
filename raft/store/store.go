package store

import (
	"encoding/json"
	"fmt"
	"os"
)

type Store struct {
	path string

	Data Data
}

func New(path string) (*Store, error) {
	p := &Store{
		path: path,
		Data: Data{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         []LogEntry{},
		},
	}

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		raw = []byte("{}")
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(raw, &p.Data)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (s *Store) WriteTerm(term int) error {
	s.Data.CurrentTerm = term
	s.Data.VotedFor = ""

	return persist(s.path, &s.Data)
}

func (s *Store) WriteVotedFor(term int, candidate string) error {
	s.Data.CurrentTerm = term
	s.Data.VotedFor = candidate

	return persist(s.path, &s.Data)
}

// Write entries into the log.
//
// If an entry conflicts with an existig, the log is overwritten from that point onward.
func (s *Store) WriteEntries(idx int, entries ...LogEntry) error {
	if len(s.Data.Log) < idx {
		return fmt.Errorf("index is greater than current log")
	}

	if len(entries) == 0 {
		return nil
	}

	i := 0
	for ; idx+i < len(s.Data.Log) && i < len(entries); i++ {
		if s.Data.Log[idx+i].Term != entries[i].Term {
			break
		}
	}
	if i < len(entries) {
		s.Data.Log = append(s.Data.Log[:idx+i], entries[i:]...)
	}

	return persist(s.path, &s.Data)
}

func persist(path string, data *Data) error {
	fil, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fil.Close()

	return json.NewEncoder(fil).Encode(data)
}
