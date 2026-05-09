package protocol

import (
	"encoding/json"
	"errors"
)

type Error struct {
	Cause string `json:"string"`
}

func (r *Error) Serialize() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append([]byte{ErrorMagicNumber}, buf...), nil
}

func (r *Error) Deserialize(data []byte) error {
	if len(data) < 1 {
		return errors.New("invalid error")
	}
	if data[0] != ErrorMagicNumber {
		return errors.New("invalid error")
	}
	return json.Unmarshal(data[1:], r)
}
