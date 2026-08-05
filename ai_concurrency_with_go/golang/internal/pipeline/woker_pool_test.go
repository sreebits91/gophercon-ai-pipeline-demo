package pipeline

import (
	"testing"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/models"
)

func TestWorkerPool(t *testing.T) {

	documents := []models.Document{

		{
			ID:      1,
			Content: "test document one",
		},

		{
			ID:      2,
			Content: "test document two",
		},
	}

	simulator := embedding.NewSimulator(
		embedding.Config{
			Dimension:   768,
			LatencyMs:   1,
			FailureRate: 0,
			Seed:        42,
		},
	)

	result := RunWorkerPool(
		documents,
		simulator,
		WorkerPoolConfig{
			Workers: 2,
		},
	)

	if result.Successful != 2 {

		t.Fatalf(
			"expected 2 successful, got %d",
			result.Successful,
		)
	}

	if result.Failed != 0 {

		t.Fatalf(
			"expected 0 failures, got %d",
			result.Failed,
		)
	}
}
