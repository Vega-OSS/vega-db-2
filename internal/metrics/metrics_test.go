package metrics_test

import (
	"testing"

	"vegadb/internal/metrics"
)

func TestMetricsSnapshot(t *testing.T) {
	m := metrics.New()
	m.Puts.Add(10)
	m.Gets.Add(5)
	m.GetMisses.Add(2)
	m.Deletes.Add(1)

	snap := m.Snapshot()
	if snap["puts"] != 10 {
		t.Fatalf("expected puts 10, got %d", snap["puts"])
	}
	if snap["gets"] != 5 {
		t.Fatalf("expected gets 5, got %d", snap["gets"])
	}
	if snap["get_misses"] != 2 {
		t.Fatalf("expected get_misses 2, got %d", snap["get_misses"])
	}
	if snap["deletes"] != 1 {
		t.Fatalf("expected deletes 1, got %d", snap["deletes"])
	}
}
