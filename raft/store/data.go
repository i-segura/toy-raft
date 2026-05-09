package store

type Data struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"`

	Log []LogEntry `json:"log"`
}

type LogEntry struct {
	Term    int `json:"term"`
	Command any `json:"command"`
}
