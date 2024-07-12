package repository

import (
	"gorm.io/gorm"
)

type DatabaseRepository struct {
	db *gorm.DB
}

func NewDatabaseRepository(db *gorm.DB) *DatabaseRepository {
	return &DatabaseRepository{db: db}
}

func (r *DatabaseRepository) GetDocument(url string) (*TDocument, error) {
	var doc TDocument
	if err := r.db.First(&doc, "url = ?", url).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &doc, nil
}

func (r *DatabaseRepository) SaveDocument(doc *TDocument) error {
	return r.db.Save(doc).Error
}
