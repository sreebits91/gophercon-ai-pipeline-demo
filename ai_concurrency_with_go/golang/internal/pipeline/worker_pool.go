package pipeline

import (
	"sync"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
)

type WorkerPoolConfig struct {
	Workers int
}

func RunWorkerPool(
	documents []models.Document,
	simulator *embedding.Simulator,
	config WorkerPoolConfig,
) metrics.Result {

	tracker := metrics.NewTracker(
		len(documents),
	)

	tracker.Start()

	jobs := make(
		chan models.Document,
		len(documents),
	)

	var wg sync.WaitGroup

	for i := 0; i < config.Workers; i++ {

		wg.Add(1)

		go func() {

			defer wg.Done()

			for document := range jobs {

				_, err := simulator.GenerateEmbedding(
					document.Content,
				)

				if err != nil {

					tracker.Failure()

					continue
				}

				tracker.Success()
			}

		}()

	}

	for _, document := range documents {

		jobs <- document
	}

	close(jobs)

	wg.Wait()

	return tracker.Finish()
}
