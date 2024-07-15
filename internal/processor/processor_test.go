package processor

import (
	"VKTestTask/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"testing"
)

func setupTestDB(t *testing.T) (*gorm.DB, func()) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	if err := db.AutoMigrate(&repository.TDocument{}); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}
	return db, func() { db.Exec("DROP TABLE TDocuments") }
}

func TestProcess(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := repository.NewDatabaseRepository(db)
	proc := NewDocumentProcessor(repo)

	doc := &repository.TDocument{
		Url:       "http://example.com",
		PubDate:   100,
		FetchTime: 200,
		Text:      "Some text",
	}

	processedDoc, err := proc.Process(doc)
	if err != nil {
		t.Fatalf("Failed to process document: %v", err)
	}

	if processedDoc.Url != doc.Url {
		t.Fatalf("Expected URL to be '%s', got '%s'", doc.Url, processedDoc.Url)
	}

	if processedDoc.FirstFetchTime != doc.FetchTime {
		t.Fatalf("Expected FirstFetchTime to be %d, got %d", doc.FetchTime, processedDoc.FirstFetchTime)
	}

	// Add more test cases as needed
}
