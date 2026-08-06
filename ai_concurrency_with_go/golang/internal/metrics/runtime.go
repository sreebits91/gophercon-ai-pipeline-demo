package metrics

import "runtime"

type RuntimeStats struct {
	Goroutines int `json:"goroutines"`

	MemoryAllocated uint64 `json:"memory_allocated_bytes"`

	TotalAllocated uint64 `json:"total_allocated_bytes"`

	GCCycles uint32 `json:"gc_cycles"`
}

func CaptureRuntimeStats() RuntimeStats {

	var mem runtime.MemStats

	runtime.ReadMemStats(&mem)

	return RuntimeStats{

		Goroutines: runtime.NumGoroutine(),

		MemoryAllocated: mem.Alloc,

		TotalAllocated: mem.TotalAlloc,

		GCCycles: mem.NumGC,
	}
}
