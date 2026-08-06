package metrics

import (
	"time"
)

type Result struct {
	Documents int `json:"documents"`

	Successful int `json:"successful"`

	Failed int `json:"failed"`

	Duration time.Duration `json:"duration"`

	Throughput float64 `json:"throughput"`

	Runtime RuntimeStats `json:"runtime"`
}

type Tracker struct {
	total int

	successful int

	failed int

	start time.Time

	startRuntime RuntimeStats
}

func NewTracker(total int) *Tracker {

	return &Tracker{
		total: total,
	}
}

func (t *Tracker) Start() {

	t.start = time.Now()

	t.startRuntime = CaptureRuntimeStats()
}

func (t *Tracker) Success() {

	t.successful++
}

func (t *Tracker) Failure() {

	t.failed++
}

func (t *Tracker) Finish() Result {

	duration := time.Since(t.start)

	endRuntime := CaptureRuntimeStats()

	var throughput float64

	if duration.Seconds() > 0 {

		throughput =
			float64(t.successful) /
				duration.Seconds()
	}

	return Result{

		Documents: t.total,

		Successful: t.successful,

		Failed: t.failed,

		Duration: duration,

		Throughput: throughput,

		Runtime: RuntimeStats{

			Goroutines: endRuntime.Goroutines,

			MemoryAllocated: endRuntime.MemoryAllocated -
				t.startRuntime.MemoryAllocated,

			TotalAllocated: endRuntime.TotalAllocated -
				t.startRuntime.TotalAllocated,

			GCCycles: endRuntime.GCCycles -
				t.startRuntime.GCCycles,
		},
	}
}
