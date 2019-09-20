package splits

type Config struct {
	Source       string `json:"source"`
	Destination  string `json:"destination"`
	DirectorySep string `json:"directorysep"`
	Client       string `json:"client"`
}

type OrderData struct {
	Status string
	Nimi   int64
	Data   [][]string
}
