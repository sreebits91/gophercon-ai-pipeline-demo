package loader

import (
	"encoding/json"
	"os"

	"ai_cuurency_with_go/internal/models"
)

func LoadDocuments(path string) ([]models.Document, error) {

	file, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	var documents []models.Document

	decoder := json.NewDecoder(file)

	err = decoder.Decode(&documents)

	if err != nil {
		return nil, err
	}

	return documents, nil
}
