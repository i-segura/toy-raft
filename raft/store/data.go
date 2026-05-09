package store

type Data struct {
	CurrentTerm int    `json:"current_term"` // Latest term server has seen.
	VotedFor    string `json:"voted_for"`    // Who received vote in curent term. Can be empty if none.

	Log []LogEntry `json:"log"`
}

type LogEntry struct {
	Term    int `json:"term"`
	Command any `json:"command"`
}
