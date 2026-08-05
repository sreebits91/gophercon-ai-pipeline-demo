package loader

import (
	"testing"
)

func TestLoadDocuments(t *testing.T) {

	documents, err := LoadDocuments(
		"../../../dataset/sample_documents.json",
	)

	if err != nil {
		t.Fatalf(
			"failed loading documents: %v",
			err,
		)
	}

	if len(documents) == 0 {
		t.Fatal(
			"expected documents but got zero",
		)
	}

	if documents[0].Content == "" {
		t.Fatal(
			"document content empty",
		)
	}
}
