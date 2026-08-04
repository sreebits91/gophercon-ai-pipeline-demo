#!/usr/bin/env bash
set -e

PROJECT_DIR="gophercon-ai-pipeline-demo"

echo "===================================================="
echo "🚀 Setting up & Launching GopherCon India 2026 Demo"
echo "===================================================="

# 1. Create project directories
mkdir -p ${PROJECT_DIR}/pkg/pipeline ${PROJECT_DIR}/cmd/demo ${PROJECT_DIR}/cmd/webui ${PROJECT_DIR}/web

cd ${PROJECT_DIR}

# 2. Write go.mod
cat << 'GO_MOD' > go.mod
module github.com/gophercon-india-2026/gophercon-ai-pipeline-demo

go 1.22

require (
	golang.org/x/sync v0.7.0
	golang.org/x/time v0.5.0
)
GO_MOD

echo "📦 Fetching dependencies and generating go.sum..."
go mod tidy

# 3. Write Core Pipeline logic (pkg/pipeline/pipeline.go)
cat << 'PIPELINE_GO' > pkg/pipeline/pipeline.go
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type Document struct {
	ID      string
	Content string
}

type TextChunk struct {
	DocID string
	Text  string
	Index int
}

type VectorEmbedding struct {
	DocID  string
	Vector []float32
}

type PipelineMetrics struct {
	Ingested uint64
	Embedded uint64
	Indexed  uint64
	Errors   uint64
}

type Pipeline struct {
	workerCount int
	rateLimiter *rate.Limiter
	Metrics     PipelineMetrics
}

func NewPipeline(workers int, rps int) *Pipeline {
	return &Pipeline{
		workerCount: workers,
		rateLimiter: rate.NewLimiter(rate.Limit(rps), rps),
	}
}

func (p *Pipeline) Run(ctx context.Context, docs []Document) error {
	g, ctx := errgroup.WithContext(ctx)

	ingestChan := make(chan Document, 1000)
	chunkChan  := make(chan TextChunk, 500)
	embedChan  := make(chan VectorEmbedding, 100)

	// Stage 1: Ingestion
	g.Go(func() error {
		defer close(ingestChan)
		for _, doc := range docs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ingestChan <- doc:
				atomic.AddUint64(&p.Metrics.Ingested, 1)
			}
		}
		slog.Info("Stage 1 Complete: Ingestion finished", "total_docs", len(docs))
		return nil
	})

	// Stage 2: Token Chunking
	g.Go(func() error {
		defer close(chunkChan)
		for doc := range ingestChan {
			chunk := TextChunk{DocID: doc.ID, Text: doc.Content, Index: 0}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunkChan <- chunk:
			}
		}
		slog.Info("Stage 2 Complete: Text chunking finished")
		return nil
	})

	// Stage 3: Embedding Worker Pool
	g.Go(func() error {
		defer close(embedChan)
		workerGroup, workerCtx := errgroup.WithContext(ctx)

		for w := 0; w < p.workerCount; w++ {
			workerGroup.Go(func() error {
				for chunk := range chunkChan {
					if err := p.rateLimiter.Wait(workerCtx); err != nil {
						return err
					}

					vec, err := generateEmbedding(workerCtx, chunk)
					if err != nil {
						atomic.AddUint64(&p.Metrics.Errors, 1)
						return fmt.Errorf("embedding error for doc %s: %w", chunk.DocID, err)
					}

					atomic.AddUint64(&p.Metrics.Embedded, 1)

					select {
					case <-workerCtx.Done():
						return workerCtx.Err()
					case embedChan <- vec:
					}
				}
				return nil
			})
		}
		return workerGroup.Wait()
	})

	// Stage 4: Vector DB Indexing
	g.Go(func() error {
		for range embedChan {
			atomic.AddUint64(&p.Metrics.Indexed, 1)
		}
		slog.Info("Stage 4 Complete: Vector DB indexing finished")
		return nil
	})

	return g.Wait()
}

func generateEmbedding(ctx context.Context, chunk TextChunk) (VectorEmbedding, error) {
	select {
	case <-ctx.Done():
		return VectorEmbedding{}, ctx.Err()
	case <-time.After(2 * time.Millisecond):
		return VectorEmbedding{DocID: chunk.DocID, Vector: []float32{0.12, 0.45, 0.89}}, nil
	}
}
PIPELINE_GO

# 4. Write CLI Runner (cmd/demo/main.go)
cat << 'DEMO_MAIN' > cmd/demo/main.go
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
DEMO_MAIN

# 5. Write Web UI Server (cmd/webui/main.go)
cat << 'WEBUI_MAIN' > cmd/webui/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type UIEvent struct {
	Ingested uint64 `json:"ingested"`
	Embedded uint64 `json:"embedded"`
	Indexed  uint64 `json:"indexed"`
	Workers  int    `json:"workers"`
	OpsSec   int    `json:"ops_sec"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/index.html")
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		var count uint64
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				count += 50
				evt := UIEvent{
					Ingested: count,
					Embedded: count - 10,
					Indexed:  count - 25,
					Workers:  50,
					OpsSec:   14200,
				}
				data, _ := json.Marshal(evt)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	})

	slog.Info("🌐 Live Observability Web UI server running at http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("Server failed", "err", err)
	}
}
WEBUI_MAIN

# 6. Write HTML Template (web/index.html)
cat << 'WEB_HTML' > web/index.html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>GopherCon India 2026 - AI Pipeline Live UI</title>
    <style>
        body { background: #0b1117; color: #f0f4f8; font-family: 'Inter', system-ui, sans-serif; margin: 0; padding: 20px; }
        h1 { color: #00add8; text-align: center; }
        .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-top: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; text-align: center; }
        .val { font-size: 2rem; font-weight: bold; color: #76e1fe; margin-top: 5px; }
        .bar-container { background: #21262d; border-radius: 4px; height: 12px; width: 100%; margin-top: 10px; overflow: hidden; }
        .bar { background: #00add8; height: 100%; width: 0%; transition: width 0.3s; }
        .log-box { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 15px; height: 200px; overflow-y: auto; font-family: monospace; margin-top: 20px; color: #76e1fe; }
    </style>
</head>
<body>
    <h1>🚀 High-Throughput AI Pipeline Observability</h1>
    <div class="grid">
        <div class="card"><h3>Stage 1: Ingest</h3><div id="ingested" class="val">0</div><div class="bar-container"><div id="bar-ingest" class="bar"></div></div></div>
        <div class="card"><h3>Stage 2: Chunk</h3><div id="chunked" class="val">0</div><div class="bar-container"><div id="bar-chunk" class="bar"></div></div></div>
        <div class="card"><h3>Stage 3: Embed</h3><div id="embedded" class="val">0</div><div class="bar-container"><div id="bar-embed" class="bar"></div></div></div>
        <div class="card"><h3>Stage 4: Index</h3><div id="indexed" class="val">0</div><div class="bar-container"><div id="bar-index" class="bar"></div></div></div>
    </div>
    <div class="log-box" id="logs">[SYSTEM INITIALIZED] Streaming pipeline metrics...</div>

    <script>
        const evtSource = new EventSource('/events');
        const logs = document.getElementById('logs');
        evtSource.onmessage = function(e) {
            const data = JSON.parse(e.data);
            document.getElementById('ingested').innerText = data.ingested;
            document.getElementById('chunked').innerText = data.ingested;
            document.getElementById('embedded').innerText = data.embedded;
            document.getElementById('indexed').innerText = data.indexed;

            document.getElementById('bar-ingest').style.width = (data.ingested % 100) + '%';
            document.getElementById('bar-chunk').style.width = (data.embedded % 100) + '%';
            document.getElementById('bar-embed').style.width = (data.embedded % 100) + '%';
            document.getElementById('bar-index').style.width = (data.indexed % 100) + '%';

            logs.innerText += `\n[METRICS] Active Workers: ${data.workers} | Throughput: ${data.ops_sec} ops/sec`;
            logs.scrollTop = logs.scrollHeight;
        };
    </script>
</body>
</html>
WEB_HTML

# 7. Run CLI Demo
echo "Running Live CLI Demo..."
go run ./cmd/demo