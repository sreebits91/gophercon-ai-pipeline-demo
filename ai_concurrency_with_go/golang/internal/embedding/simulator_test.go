package embedding

import (
	"testing"
)

func TestGenerateEmbedding(t *testing.T) {

	config := Config{

		Dimension: 768,

		LatencyMs: 10,

		FailureRate: 0,

		Seed: 42,
	}

	simulator := NewSimulator(
		config,
	)

	result, err :=
		simulator.GenerateEmbedding(
			"Artificial intelligence pipelines",
		)

	if err != nil {

		t.Fatalf(
			"unexpected error: %v",
			err,
		)
	}

	if result.Dimension != 768 {

		t.Fatalf(
			"expected dimension 768 got %d",
			result.Dimension,
		)
	}

	if len(result.Vector) != 768 {

		t.Fatalf(
			"expected vector size 768 got %d",
			len(result.Vector),
		)
	}

	if result.ProcessingTimeMs <= 0 {

		t.Fatalf(
			"processing time should be positive",
		)
	}
}
