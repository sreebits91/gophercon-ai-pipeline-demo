package pipeline

import (
	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
)

func RunSequential(
	documents []models.Document,
	simulator *embedding.Simulator,
) metrics.Result {

	tracker := metrics.NewTracker(
		len(documents),
	)

	tracker.Start()

	for _, document := range documents {

		_, err := simulator.GenerateEmbedding(
			document.Content,
		)

		if err != nil {

			tracker.Failure()

			continue
		}

		tracker.Success()
	}

	return tracker.Finish()
}
