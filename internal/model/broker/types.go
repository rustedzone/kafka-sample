package broker

import (
	"encoding/json"
)

type (
	// Message contain message contract
	Message struct {
		Header Header          `json:"header"`
		Body   json.RawMessage `json:"body"`
	}

	// Header message header
	Header struct {
		Action string `json:"action"`
		Target string `json:"target"`
		Key    string `json:"key"`
	}
)
