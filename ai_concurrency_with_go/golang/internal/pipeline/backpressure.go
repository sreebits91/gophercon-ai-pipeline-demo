package pipeline

import (
	"sync"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
)

type BackpressureConfig struct {
	Workers     int
	ChannelSize int
}

type backpressureResult struct {
	Success bool
}

func RunBackpressure(
	documents []models.Document,
	simulator *embedding.Simulator,
	config BackpressureConfig,
) metrics.Result {

	tracker := metrics.NewTracker(len(documents))
	tracker.Start()

	jobs := make(chan models.Document, config.ChannelSize)
	results := make(chan backpressureResult, config.ChannelSize)

	var wg sync.WaitGroup

	for i := 0; i < config.Workers; i++ {

		wg.Add(1)

		go func() {

			defer wg.Done()

			for document := range jobs {

				_, err := simulator.GenerateEmbedding(document.Content)

				if err != nil {

					results <- backpressureResult{
						Success: false,
					}

					continue
				}

				results <- backpressureResult{
					Success: true,
				}
			}

		}()
	}

	go func() {

		for _, document := range documents {

			jobs <- document

		}

		close(jobs)

	}()

	go func() {

		wg.Wait()

		close(results)

	}()

	for result := range results {

		if result.Success {

			tracker.Success()

		} else {

			tracker.Failure()

		}
	}

	return tracker.Finish()
}
