package pipeline

import (
	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/models"
	"testing"
)

func TestBackpressure(t *testing.T) {

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
		{
			ID:      4,
			Content: "Document Four",
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

	result := RunBackpressure(
		documents,
		simulator,
		BackpressureConfig{
			Workers:     2,
			ChannelSize: 2,
		},
	)

	if result.Successful != 4 {
		t.Fatalf(
			"expected 4 successful, got %d",
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
