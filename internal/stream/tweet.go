package stream

type Tweet struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
	Text      string `json:"text"`
}
