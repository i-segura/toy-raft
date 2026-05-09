package store

import (
	"encoding/json"
	"fmt"
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

func (s *Store) Snapshot() *Data {
	return &s.data
}

func (s *Store) WriteTerm(term int) error {
	s.data.CurrentTerm = term
	s.data.VotedFor = ""

	return persist(s.path, &s.data)
}

func (s *Store) WriteVotedFor(candidate string) error {
	s.data.VotedFor = candidate

	return persist(s.path, &s.data)
}

// Write entries into the log.
//
// If an entry conflicts with an existig, the log is overwritten from that point onward.
func (s *Store) WriteEntries(idx int, entries ...LogEntry) error {
	if len(s.data.Log) < idx {
		return fmt.Errorf("index is greater than current log")
	}

	if len(entries) == 0 {
		return nil
	}

	if len(s.data.Log) == idx {
		s.data.Log = append(s.data.Log, entries...)
		return persist(s.path, &s.data)
	}

	for replayIdx, replay := range s.data.Log[idx:] {
		if replay.Term == entries[replayIdx].Term {
			continue
		}

		s.data.Log = append(s.data.Log[:idx+replayIdx], entries[replayIdx:]...)
		return persist(s.path, &s.data)
	}

	return nil
}

func persist(path string, data *Data) error {
	fil, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fil.Close()

	return json.NewEncoder(fil).Encode(data)
}
