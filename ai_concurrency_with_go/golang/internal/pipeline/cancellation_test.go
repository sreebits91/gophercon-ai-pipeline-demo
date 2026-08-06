package pipeline

import (
	"context"
	"testing"
	"time"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/models"
)

func TestCancellation(t *testing.T) {

	var docs []models.Document

	for i := 0; i < 100; i++ {

		docs = append(docs, models.Document{
			ID:      i,
			Content: "Sample document",
		})
	}

	simulator := embedding.NewSimulator(
		embedding.Config{
			Dimension:   768,
			LatencyMs:   10,
			FailureRate: 0,
			Seed:        42,
		},
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)

	defer cancel()

	result := RunCancellation(
		ctx,
		docs,
		simulator,
		CancellationConfig{
			Workers: 4,
		},
	)

	if result.Documents > len(docs) {

		t.Fatal("processed more documents than expected")
	}
}
