package repository

import (
	"errors"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// DatabaseRepository provides an interface for operations on documents in the database
type DatabaseRepository struct {
	db *gorm.DB
}

// NewDatabaseRepository creates a new DatabaseRepository with the given database connection
func NewDatabaseRepository(db *gorm.DB) *DatabaseRepository {
	return &DatabaseRepository{db: db}
}

// GetDocument retrieves a document from the database by its URL
func (r *DatabaseRepository) GetDocument(url string) (*TDocument, error) {
	var doc TDocument
	err := r.db.Where("url = ?", url).First(&doc).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &doc, nil
}

// SaveDocument saves the given document to the database
func (r *DatabaseRepository) SaveDocument(doc *TDocument) error {
	return r.db.Save(doc).Error
}

// SetupDatabase initializes the SQLite database connection and migrates the schema
func SetupDatabase(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Automigrate the schema
	if err := db.AutoMigrate(&TDocument{}); err != nil {
		return nil, err
	}

	return db, nil
}

// TDocument represents the structure of a document in the database
type TDocument struct {
	ID             uint   `gorm:"primaryKey"`
	Url            string `gorm:"not null"`
	PubDate        int64  `gorm:"not null"`
	FetchTime      int64  `gorm:"not null"`
	Text           string `gorm:"not null"`
	FirstFetchTime int64  `gorm:"not null"`
}
