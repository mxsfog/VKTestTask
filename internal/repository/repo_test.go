package repository

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"testing"
)

func setupTestDB(t *testing.T) (*gorm.DB, func()) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	if err := db.AutoMigrate(&TDocument{}); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}
	return db, func() { db.Exec("DROP TABLE TDocuments") }
}

func TestSave(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewDatabaseRepository(db)

	doc := &TDocument{
		Url:       "http://example.com",
		PubDate:   100,
		FetchTime: 200,
		Text:      "Some text",
	}

	if err := repo.Save(doc); err != nil {
		t.Fatalf("Failed to save document: %v", err)
	}

	var count int64
	db.Model(&TDocument{}).Count(&count)
	if count != 1 {
		t.Fatalf("Expected 1 document, got %d", count)
	}
}

func TestFindByUrl(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := NewDatabaseRepository(db)

	doc := &TDocument{
		Url:       "http://example.com",
		PubDate:   100,
		FetchTime: 200,
		Text:      "Some text",
	}

	repo.Save(doc)

	docs, err := repo.FindByUrl("http://example.com")
	if err != nil {
		t.Fatalf("Failed to find documents by URL: %v", err)
	}

	if len(docs) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(docs))
	}

	if docs[0].Url != "http://example.com" {
		t.Fatalf("Expected URL to be 'http://example.com', got '%s'", docs[0].Url)
	}
}
