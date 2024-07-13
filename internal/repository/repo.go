package repository

// DocumentRepository - интерфейс для операций с документами в БД
type DocumentRepository interface {
	Save(document *TDocument) error
	FindAll() ([]TDocument, error)
}

func (r *DatabaseRepository) Save(document *TDocument) error {
	return r.db.Create(document).Error
}

func (r *DatabaseRepository) FindAll() ([]TDocument, error) {
	var documents []TDocument
	err := r.db.Find(&documents).Error
	return documents, err
}

// TDocument - структура документа
type TDocument struct {
	ID        uint   `gorm:"primaryKey"`
	Url       string `gorm:"not null"`
	PubDate   int64  `gorm:"not null"`
	FetchTime int64  `gorm:"not null"`
	Text      string `gorm:"not null"`
}
