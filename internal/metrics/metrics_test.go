package metrics

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// 验证 New() 注册 HTTP / Go runtime / process 三类。
// 用 GatherAndCount 间接断言 collector 数 > 0。
func TestNew_RegistersBaseCollectors(t *testing.T) {
	r := New()
	count, err := testutil.GatherAndCount(r.Registry)
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if count == 0 {
		t.Errorf("expected base collectors registered, got 0 metric families")
	}
}

func TestBindLocalCache(t *testing.T) {
	r := New()
	var hits, misses uint64
	r.BindLocalCache(
		func() float64 { return float64(atomic.LoadUint64(&hits)) },
		func() float64 { return float64(atomic.LoadUint64(&misses)) },
	)

	atomic.StoreUint64(&hits, 1000)
	atomic.StoreUint64(&misses, 3)

	gathered, err := r.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := map[string]float64{}
	for _, mf := range gathered {
		switch *mf.Name {
		case "slink_l1_hits_total":
			got["hits"] = *mf.Metric[0].Counter.Value
		case "slink_l1_misses_total":
			got["misses"] = *mf.Metric[0].Counter.Value
		}
	}

	if got["hits"] != 1000 {
		t.Errorf("hits_total = %v, want 1000", got["hits"])
	}
	if got["misses"] != 3 {
		t.Errorf("misses_total = %v, want 3", got["misses"])
	}
}

func TestBindEventBuffer(t *testing.T) {
	r := New()
	r.BindEventBuffer(EventBufferGetters{
		Enqueued: func() float64 { return 100 },
		Dropped:  func() float64 { return 5 },
		Flushed:  func() float64 { return 95 },
		FlushErr: func() float64 { return 0 },
		Used:     func() float64 { return 42 },
		Capacity: func() float64 { return 50000 },
	})

	gathered, err := r.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	want := map[string]float64{
		"slink_event_buffer_enqueued_total":  100,
		"slink_event_buffer_dropped_total":   5,
		"slink_event_buffer_flushed_total":   95,
		"slink_event_buffer_flush_err_total": 0,
		"slink_event_buffer_used":            42,
		"slink_event_buffer_capacity":        50000,
	}
	for _, mf := range gathered {
		if !strings.HasPrefix(*mf.Name, "slink_event_buffer_") {
			continue
		}
		exp, ok := want[*mf.Name]
		if !ok {
			continue
		}
		var v float64
		if mf.Metric[0].Counter != nil {
			v = *mf.Metric[0].Counter.Value
		} else if mf.Metric[0].Gauge != nil {
			v = *mf.Metric[0].Gauge.Value
		}
		if v != exp {
			t.Errorf("%s = %v, want %v", *mf.Name, v, exp)
		}
	}
}

func TestBindIDGenerator(t *testing.T) {
	r := New()
	r.BindIDGenerator(func() float64 { return 0.42 })

	gathered, err := r.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range gathered {
		if *mf.Name == "slink_id_segment_usage" {
			if v := *mf.Metric[0].Gauge.Value; v != 0.42 {
				t.Errorf("id_segment_usage = %v, want 0.42", v)
			}
			return
		}
	}
	t.Errorf("slink_id_segment_usage not registered")
}

func TestHTTPMetrics_RecordRequest(t *testing.T) {
	r := New()
	// 模拟一次请求记录
	r.HTTP.Requests.WithLabelValues("/:code", "GET", "302").Inc()
	r.HTTP.Duration.WithLabelValues("/:code").Observe(0.005)

	if got := testutil.ToFloat64(r.HTTP.Requests.WithLabelValues("/:code", "GET", "302")); got != 1 {
		t.Errorf("requests_total = %v, want 1", got)
	}
	// histogram 不能直接 ToFloat64；通过 Gather 找 bucket 数
	gathered, _ := r.Registry.Gather()
	for _, mf := range gathered {
		if *mf.Name == "slink_http_request_duration_seconds" {
			h := mf.Metric[0].Histogram
			if *h.SampleCount != 1 {
				t.Errorf("histogram count = %v, want 1", *h.SampleCount)
			}
			return
		}
	}
	t.Errorf("histogram not gathered")
}

func TestNormalizePath(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/", "/"},
		{"/healthz", "/healthz"},
		{"/readyz", "/readyz"},
		{"/api/links", "/api/links"},
		{"/api/users", "/api/*"},
		{"/api/foo/bar", "/api/*"},
		{"/abc123", "/:code"},
		{"/", "/"},
		{"/x/y/z", "unknown"},
	}
	for _, c := range cases {
		if got := normalizePath(c.in); got != c.want {
			t.Errorf("normalizePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
