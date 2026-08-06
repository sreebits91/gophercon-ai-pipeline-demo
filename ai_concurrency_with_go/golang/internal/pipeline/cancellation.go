package pipeline

import (
	"context"
	"sync"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
)

type CancellationConfig struct {
	Workers int
}

type cancellationResult struct {
	Success bool
}

func RunCancellation(
	ctx context.Context,
	documents []models.Document,
	simulator *embedding.Simulator,
	config CancellationConfig,
) metrics.Result {

	tracker := metrics.NewTracker(len(documents))
	tracker.Start()

	jobs := make(chan models.Document, config.Workers*2)
	results := make(chan cancellationResult, config.Workers*2)

	var wg sync.WaitGroup

	// Workers
	for i := 0; i < config.Workers; i++ {

		wg.Add(1)

		go func() {

			defer wg.Done()

			for {

				select {

				case <-ctx.Done():
					return

				case document, ok := <-jobs:

					if !ok {
						return
					}

					_, err := simulator.GenerateEmbedding(
						document.Content,
					)

					if err != nil {

						select {

						case results <- cancellationResult{Success: false}:

						case <-ctx.Done():
							return
						}

						continue
					}

					select {

					case results <- cancellationResult{Success: true}:

					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Producer
	go func() {

		defer close(jobs)

		for _, document := range documents {

			select {

			case <-ctx.Done():
				return

			case jobs <- document:
			}
		}

	}()

	// Close results once all workers finish
	go func() {

		wg.Wait()

		close(results)

	}()

	// Collector
	for result := range results {

		if result.Success {

			tracker.Success()

		} else {

			tracker.Failure()

		}
	}

	return tracker.Finish()
}
