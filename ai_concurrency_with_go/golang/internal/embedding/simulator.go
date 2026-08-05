package embedding

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand"
	"time"
)

// Config defines the behaviour of the simulated embedding service.
type Config struct {
	Dimension   int
	LatencyMs   int
	FailureRate float64
	Seed        int64
}

// Result represents the generated embedding.
type Result struct {
	Vector           []float64
	Dimension        int
	ProcessingTimeMs float64
}

// Simulator simulates an embedding model.
type Simulator struct {
	config Config
	random *rand.Rand
}

// NewSimulator creates a new embedding simulator.
func NewSimulator(config Config) *Simulator {
	return &Simulator{
		config: config,
		random: rand.New(rand.NewSource(config.Seed)),
	}
}

// GenerateEmbedding simulates embedding generation.
func (s *Simulator) GenerateEmbedding(text string) (*Result, error) {
	start := time.Now()

	// Simulate inference latency.
	time.Sleep(time.Duration(s.config.LatencyMs) * time.Millisecond)

	// Simulate failures.
	if s.random.Float64() < s.config.FailureRate {
		return nil, errors.New("embedding generation failed")
	}

	vector := generateVector(text, s.config.Dimension)

	return &Result{
		Vector:           vector,
		Dimension:        s.config.Dimension,
		ProcessingTimeMs: float64(time.Since(start).Microseconds()) / 1000.0,
	}, nil
}

// generateVector creates a deterministic pseudo-random embedding.
// The same input text always produces the same vector.
func generateVector(text string, dimension int) []float64 {
	hash := sha256.Sum256([]byte(text))

	seed := int64(binary.LittleEndian.Uint64(hash[:8]))

	r := rand.New(rand.NewSource(seed))

	vector := make([]float64, dimension)

	for i := range vector {
		vector[i] = r.Float64()
	}

	return vector
}
