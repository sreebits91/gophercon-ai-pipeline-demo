package benchmark

import (
	"context"
	"time"

	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/models"
	"ai_cuurency_with_go/internal/pipeline"
)

type Config struct {
	Mode string

	Workers int

	Timeout time.Duration
}

func Run(
	cfg Config,
	documents []models.Document,
	simulator *embedding.Simulator,
) metrics.Result {

	switch cfg.Mode {

	case "sequential":

		return pipeline.RunSequential(
			documents,
			simulator,
		)

	case "workerpool":

		return pipeline.RunWorkerPool(
			documents,
			simulator,
			pipeline.WorkerPoolConfig{
				Workers: cfg.Workers,
			},
		)

	case "fanout":

		return pipeline.RunFanOut(
			documents,
			simulator,
			pipeline.FanOutConfig{
				Workers: cfg.Workers,
			},
		)

	case "backpressure":

		return pipeline.RunBackpressure(
			documents,
			simulator,
			pipeline.BackpressureConfig{
				Workers:     cfg.Workers,
				ChannelSize: 100,
			},
		)

	case "cancellation":

		ctx, cancel := context.WithTimeout(
			context.Background(),
			cfg.Timeout,
		)

		defer cancel()

		return pipeline.RunCancellation(
			ctx,
			documents,
			simulator,
			pipeline.CancellationConfig{
				Workers: cfg.Workers,
			},
		)
	}

	return metrics.Result{}
}
