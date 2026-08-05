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

	// Stage 1: Ingest
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

	// Stage 2: Chunk
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

	// Stage 4: Indexing
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