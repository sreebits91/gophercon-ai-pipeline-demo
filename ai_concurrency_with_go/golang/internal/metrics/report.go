package metrics

type Report struct {
	Mode               string  `json:"mode"`
	Workers            int     `json:"workers"`
	DocumentsProcessed int     `json:"documents_processed"`
	Successful         int     `json:"successful"`
	Failed             int     `json:"failed"`
	DurationSeconds    float64 `json:"duration_seconds"`
	Throughput         float64 `json:"throughput_docs_per_sec"`
}
