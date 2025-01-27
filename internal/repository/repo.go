package repository

// DocumentRepository - интерфейс для операций с документами в БД
type DocumentRepository interface {
	Save(document *TDocument) error
	FindAll() ([]TDocument, error)
	FindByUrl(url string) ([]TDocument, error)
}

func (r *DatabaseRepository) Save(document *TDocument) error {
	return r.db.Save(document).Error
}

func (r *DatabaseRepository) FindAll() ([]TDocument, error) {
	var documents []TDocument
	err := r.db.Find(&documents).Error
	return documents, err
}

func (r *DatabaseRepository) FindByUrl(url string) ([]TDocument, error) {
	var documents []TDocument
	err := r.db.Where("url = ?", url).Find(&documents).Error
	return documents, err
}
