package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"ai_cuurency_with_go/internal/metrics"
)

func SaveReport(report metrics.Report) error {

	timestamp :=
		time.Now().
			Format("20060102_150405")

	filename :=
		fmt.Sprintf(
			"../../../benchmark_results/go/%s_%s_%d.json",
			timestamp,
			report.Mode,
			report.Workers,
		)

	file, err :=
		os.Create(filename)

	if err != nil {
		return err
	}

	defer file.Close()

	encoder :=
		json.NewEncoder(file)

	encoder.SetIndent(
		"",
		"  ",
	)

	return encoder.Encode(report)
}
