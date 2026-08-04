package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/gophercon-india-2026/gophercon-ai-pipeline-demo/pkg/pipeline"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("🚀 GopherCon India 2026 Demo: High-Throughput AI Data Pipeline", "workers", 50, "rps_limit", 1000)

	docs := make([]pipeline.Document, 5000)
	for i := 0; i < 5000; i++ {
		docs[i] = pipeline.Document{
			ID:      fmt.Sprintf("doc-%05d", i+1),
			Content: "High performance Go concurrency pipeline example for vector embeddings.",
		}
	}

	p := pipeline.NewPipeline(50, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		startTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				indexed := atomic.LoadUint64(&p.Metrics.Indexed)
				opsPerSec := float64(indexed) / elapsed

				fmt.Printf("\r\033[K[LIVE METRICS] Elapsed: %.1fs | Ingested: %d | Embedded: %d | Indexed: %d | Throughput: %.0f ops/sec",
					elapsed,
					atomic.LoadUint64(&p.Metrics.Ingested),
					atomic.LoadUint64(&p.Metrics.Embedded),
					indexed,
					opsPerSec)
			}
		}
	}()

	start := time.Now()
	err := p.Run(ctx, docs)
	duration := time.Since(start)

	fmt.Println("\n")
	if err != nil {
		slog.Error("❌ Pipeline error encountered", "err", err)
		os.Exit(1)
	}

	slog.Info("✅ PIPELINE EXECUTION SUCCESSFUL",
		"total_indexed", p.Metrics.Indexed,
		"duration_ms", duration.Milliseconds(),
		"avg_ops_sec", fmt.Sprintf("%.0f ops/sec", float64(p.Metrics.Indexed)/duration.Seconds()))
}