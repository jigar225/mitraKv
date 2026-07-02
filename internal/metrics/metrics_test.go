package metrics

import (
	"testing"
	"time"
)

func TestRecorderCollectsMetrics(t *testing.T) {
	recorder, err := NewRecorder()
	if err != nil {
		t.Fatalf("NewRecorder returned error: %v", err)
	}

	recorder.ObserveRequest("GET", 10*time.Millisecond)
	recorder.ObserveRequest("SET", 25*time.Millisecond)

	families, err := recorder.registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	found := map[string]bool{
		"mitrakv_requests_total":           false,
		"mitrakv_request_duration_seconds": false,
	}

	for _, family := range families {
		if _, ok := found[family.GetName()]; ok {
			found[family.GetName()] = true
		}
	}

	for name, ok := range found {
		if !ok {
			t.Fatalf("missing metric family %q", name)
		}
	}
}
