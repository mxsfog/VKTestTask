package processor

import (
	"errors"
	"github.com/mxsfog/VKTestTask/internal/repository"
	"log"
	"sync"
)

type Processor interface {
	Process(d *repository.TDocument) (*repository.TDocument, error)
}

type DocumentProcessor struct {
	mu   sync.Mutex
	repo repository.Repository
}

func NewDocumentProcessor(repo repository.Repository) *DocumentProcessor {
	return &DocumentProcessor{repo: repo}
}

func (dp *DocumentProcessor) Process(d *repository.TDocument) (*repository.TDocument, error) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if d == nil {
		log.Println("Received nil document")
		return nil, errors.New("document is nil")
	}

	existingDoc, err := dp.repo.GetDocument(d.Url)
	if err != nil {
		log.Printf("Failed to get document from repository: %v", err)
		return nil, err
	}

	if existingDoc == nil {
		d.FirstFetchTime = d.FetchTime
		if err := dp.repo.SaveDocument(d); err != nil {
			log.Printf("Failed to save new document: %v", err)
			return nil, err
		}
		log.Printf("New document added with URL: %s", d.Url)
		return d, nil
	}

	// Update PubDate to the smallest FetchTime
	if d.FetchTime < existingDoc.FirstFetchTime {
		existingDoc.PubDate = d.PubDate
		existingDoc.FirstFetchTime = d.FetchTime
		log.Printf("Updated PubDate and FirstFetchTime for URL: %s", d.Url)
	}

	// Update Text and FetchTime to the latest values
	if d.FetchTime > existingDoc.FetchTime {
		existingDoc.Text = d.Text
		existingDoc.FetchTime = d.FetchTime
		log.Printf("Updated Text and FetchTime for URL: %s", d.Url)
	}

	if err := dp.repo.SaveDocument(existingDoc); err != nil {
		log.Printf("Failed to save updated document: %v", err)
		return nil, err
	}

	return existingDoc, nil
}
