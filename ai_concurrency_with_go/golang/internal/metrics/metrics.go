package metrics

import (
	"time"
)

type Result struct {
	DocumentsProcessed int

	Successful int

	Failed int

	Duration time.Duration

	Throughput float64
}

type Tracker struct {
	start time.Time

	total int

	success int

	failed int
}

func NewTracker(total int) *Tracker {

	return &Tracker{
		total: total,
	}
}

func (t *Tracker) Start() {

	t.start = time.Now()

}

func (t *Tracker) Success() {

	t.success++

}

func (t *Tracker) Failure() {

	t.failed++

}

func (t *Tracker) Finish() Result {

	duration :=
		time.Since(t.start)

	return Result{

		DocumentsProcessed: t.total,

		Successful: t.success,

		Failed: t.failed,

		Duration: duration,

		Throughput: float64(t.success) /
			duration.Seconds(),
	}
}

func (r Result) ToReport(mode string, workers int) Report {

	return Report{
		Mode: mode,

		Workers: workers,

		DocumentsProcessed: r.DocumentsProcessed,

		Successful: r.Successful,

		Failed: r.Failed,

		DurationSeconds: r.Duration.Seconds(),

		Throughput: r.Throughput,
	}
}
