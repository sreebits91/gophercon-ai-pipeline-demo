package pipeline

import (
	"testing"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/models"
)

func TestFanOut(t *testing.T) {

	documents := []models.Document{
		{
			ID:      1,
			Content: "Document One",
		},
		{
			ID:      2,
			Content: "Document Two",
		},
		{
			ID:      3,
			Content: "Document Three",
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

	result := RunFanOut(
		documents,
		simulator,
		FanOutConfig{
			Workers: 2,
		},
	)

	if result.Successful != 3 {
		t.Fatalf(
			"expected 3 successful, got %d",
			result.Successful,
		)
	}

	if result.Failed != 0 {
		t.Fatalf(
			"expected 0 failed, got %d",
			result.Failed,
		)
	}
}
