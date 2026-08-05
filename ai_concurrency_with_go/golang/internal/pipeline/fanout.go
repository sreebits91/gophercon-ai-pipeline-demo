package pipeline

import (
	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
	"sync"
)

type FanOutConfig struct {
	Workers int
}

type workerResult struct {
	Success bool
}

func RunFanOut(
	documents []models.Document,
	simulator *embedding.Simulator,
	config FanOutConfig,
) metrics.Result {

	tracker := metrics.NewTracker(len(documents))
	tracker.Start()

	jobs := make(chan models.Document, len(documents))
	results := make(chan workerResult, len(documents))

	var wg sync.WaitGroup

	for i := 0; i < config.Workers; i++ {

		wg.Add(1)

		go func() {

			defer wg.Done()

			for document := range jobs {

				_, err := simulator.GenerateEmbedding(document.Content)

				if err != nil {
					results <- workerResult{
						Success: false,
					}
					continue
				}

				results <- workerResult{
					Success: true,
				}
			}

		}()
	}

	go func() {

		wg.Wait()

		close(results)

	}()

	for _, document := range documents {

		jobs <- document

	}

	close(jobs)

	for result := range results {

		if result.Success {

			tracker.Success()

		} else {

			tracker.Failure()

		}
	}

	return tracker.Finish()
}
