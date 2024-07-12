package repository

type TDocument struct {
	Url            string `gorm:"primaryKey"`
	PubDate        uint64
	FetchTime      uint64
	Text           string
	FirstFetchTime uint64
}

type Repository interface {
	GetDocument(url string) (*TDocument, error)
	SaveDocument(doc *TDocument) error
}
