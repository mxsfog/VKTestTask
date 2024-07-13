package processor

import (
	"VKTestTask/internal/repository"
	"sync"
)

type DocumentProcessor struct {
	repo repository.DocumentRepository
	mu   sync.Mutex
}

func NewDocumentProcessor(repo repository.DocumentRepository) *DocumentProcessor {
	return &DocumentProcessor{repo: repo}
}

func (p *DocumentProcessor) Process(document *repository.TDocument) (*repository.TDocument, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.repo.Save(document)
	if err != nil {
		return nil, err
	}

	return document, nil
}

func (p *DocumentProcessor) AddSampleData() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	doc := &repository.TDocument{
		Url:       "http://example.com/doc1",
		PubDate:   100,
		FetchTime: 200,
		Text:      "First version",
	}
	return p.repo.Save(doc)
}
