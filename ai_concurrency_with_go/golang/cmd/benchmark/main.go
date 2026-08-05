package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"context"

	"ai_cuurency_with_go/internal/benchmark"
	"ai_cuurency_with_go/internal/embedding"
	"ai_cuurency_with_go/internal/loader"
	"ai_cuurency_with_go/internal/metrics"
	"ai_cuurency_with_go/internal/pipeline"
)

func main() {

	mode := flag.String(
		"mode",
		"sequential",
		"benchmark mode: sequential or workerpool",
	)

	workers := flag.Int(
		"workers",
		4,
		"number of workers for worker pool",
	)

	dataset := flag.String(
		"dataset",
		"../dataset/sample_documents.json",
		"dataset path",
	)

	timeout := flag.Duration(
	"timeout",
	30*time.Second,
	"benchmark timeout",
)

	channelSize := flag.Int(
		"channel-size",
		100,
		"buffer size for backpressure pipeline",
	)

	flag.Parse()

	documents, err := loader.LoadDocuments(
		*dataset,
	)

	if err != nil {
		log.Fatalf(
			"failed loading dataset: %v",
			err,
		)
	}

	fmt.Println(
		"Loaded documents:",
		len(documents),
	)

	simulator := embedding.NewSimulator(
		embedding.Config{
			Dimension:   768,
			LatencyMs:   50,
			FailureRate: 0.01,
			Seed:        42,
		},
	)

	start := time.Now()

	var report metrics.Report

	switch *mode {

	case "sequential":

		result :=
			pipeline.RunSequential(
				documents,
				simulator,
			)

		report =
			result.ToReport(
				"sequential",
				1,
			)

	case "workerpool":

		result :=
			pipeline.RunWorkerPool(
				documents,
				simulator,
				pipeline.WorkerPoolConfig{
					Workers: *workers,
				},
			)

		report =
			result.ToReport(
				"workerpool",
				*workers,
			)

	case "fanout":

		result := pipeline.RunFanOut(
			documents,
			simulator,
			pipeline.FanOutConfig{
				Workers: *workers,
			},
		)

		report = result.ToReport(
			"fanout",
			*workers,
		)

	case "backpressure":

		result := pipeline.RunBackpressure(
			documents,
			simulator,
			pipeline.BackpressureConfig{
				Workers:     *workers,
				ChannelSize: *channelSize,
			},
		)

		report = result.ToReport(
			"backpressure",
			*workers,
		)

		case "cancellation":

	ctx, cancel := context.WithTimeout(
		context.Background(),
		*timeout,
	)

	defer cancel()

	result := pipeline.RunCancellation(
		ctx,
		documents,
		simulator,
		pipeline.CancellationConfig{
			Workers: *workers,
		},
	)

	report = result.ToReport(
		"cancellation",
		*workers,
	)

	default:

		log.Fatalf(
			"unknown mode %s",
			*mode,
		)
	}

	duration := time.Since(start)

	fmt.Println()
	fmt.Println("========== Benchmark Result ==========")

	fmt.Println(
		"Mode:",
		*mode,
	)

	fmt.Println(
		"Workers:",
		*workers,
	)

	fmt.Println(
		"Duration:",
		duration,
	)

	fmt.Println(
		"======================================",
	)

	err =
		benchmark.SaveReport(report)

	if err != nil {

		log.Println(
			"failed saving report:",
			err,
		)
	}

}

func saveResult(result interface{}) {

	file, err := os.Create(
		"benchmark_result.json",
	)

	if err != nil {

		log.Println(err)

		return
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	encoder.SetIndent(
		"",
		"  ",
	)

	encoder.Encode(result)
}
