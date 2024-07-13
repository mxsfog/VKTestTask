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

func (p *DocumentProcessor) Process(d *repository.TDocument) (*repository.TDocument, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Найдем все документы с совпадающим полем Url
	existingDocs, err := p.repo.FindByUrl(d.Url)
	if err != nil {
		return nil, err
	}

	if len(existingDocs) == 0 {
		// Если это первый документ с данным Url, сохраняем его
		if err := p.repo.Save(d); err != nil {
			return nil, err
		}
		d.FirstFetchTime = d.FetchTime
		return d, nil
	}

	// Ищем документ с наибольшим и наименьшим FetchTime
	var latestDoc, earliestDoc *repository.TDocument
	for i, doc := range existingDocs {
		if i == 0 || doc.FetchTime > latestDoc.FetchTime {
			latestDoc = &doc
		}
		if i == 0 || doc.FetchTime < earliestDoc.FetchTime {
			earliestDoc = &doc
		}
	}

	// Обновляем поля текущего документа по правилам
	d.Text = latestDoc.Text
	d.FetchTime = latestDoc.FetchTime
	d.PubDate = earliestDoc.PubDate
	d.FirstFetchTime = earliestDoc.FetchTime

	// Сохраняем обновленный документ
	if err := p.repo.Save(d); err != nil {
		return nil, err
	}

	return d, nil
}
