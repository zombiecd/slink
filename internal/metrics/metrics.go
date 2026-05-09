// Package metrics 把 slink 各组件的运行时指标暴露为 Prometheus 格式。
//
// 设计：
//   - HTTP 指标（counter + histogram）由 fasthttp middleware 现写
//   - L1 / event buffer / id 等"被动"指标用 GaugeFunc / CounterFunc 直接绑现有 Stats() 接口，
//     免一个后台 goroutine 周期 sync
//   - 所有指标都在 Prometheus.NewRegistry() 上注册，main 把这个 registry 给 promhttp.Handler 用
//
// 命名遵循 Prometheus 官方 conventions：
//   - <namespace>_<subsystem>_<unit>_<suffix>
//   - namespace: slink
//   - counter 用 _total 后缀，histogram 用 _seconds 后缀
package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/valyala/fasthttp"
)

// Namespace 是所有 slink 指标的命名前缀。
const Namespace = "slink"

// HTTPMetrics 是请求级 instrumentation。
//
// labels 选择：
//   - path: 路由模板（"/:code" 而不是真实 code，避免基数爆炸）
//   - method: GET/POST
//   - status: HTTP status code（200/302/4xx/5xx）
//
// histogram buckets 覆盖 1ms ~ 1s（slink 跳转 P99 < 50ms）。
type HTTPMetrics struct {
	Requests *prometheus.CounterVec
	Duration *prometheus.HistogramVec
}

func newHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{
		Requests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: Namespace,
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total HTTP requests handled, partitioned by route, method, and status.",
			},
			[]string{"path", "method", "status"},
		),
		Duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: Namespace,
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "HTTP request latency distribution, partitioned by route.",
				Buckets:   []float64{0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0},
			},
			[]string{"path"},
		),
	}
}

// 设计取舍：用闭包 (func() float64) 而不是 interface 注入业务对象的 stats getter。
//
// 好处：
//   - metrics 包零依赖业务包（cache / event / id），不会循环 import
//   - 业务包不需要为 metrics 加 9 个 wrapper 方法（LocalStatsHits / StatsEnqueued / ...）
//   - main.go 装配时一行 lambda 完事，类型转换 (uint64 → float64) 也在装配点显式

// Registry 持有 slink 全部 metrics 注册表 + 各 collector 句柄。
//
// 用法：
//
//	r := metrics.New()
//	r.BindLocalCache(
//	    func() float64 { return float64(linkCache.LocalStats().Hits) },
//	    func() float64 { return float64(linkCache.LocalStats().Misses) },
//	)
//	// ...
//	http.Handle("/metrics", promhttp.HandlerFor(r.Registry, ...))
type Registry struct {
	Registry *prometheus.Registry
	HTTP     *HTTPMetrics
}

// New 构造 Registry，并注册 HTTP + Go runtime + process collector。
//
// 已注册：
//   - HTTP requests_total / request_duration_seconds
//   - go_* (goroutines, GC, mem)：collectors.NewGoCollector()
//   - process_* (CPU, FDs)：collectors.NewProcessCollector()
//
// L1 / event / id 在 BindXxx() 时再注册（GaugeFunc 直接绑业务对象的 Stats 接口）。
func New() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	httpM := newHTTPMetrics()
	reg.MustRegister(httpM.Requests, httpM.Duration)

	return &Registry{
		Registry: reg,
		HTTP:     httpM,
	}
}

// BindLocalCache 绑定 L1 hits / misses 为 CounterFunc。
//
// 用 CounterFunc 而非 Counter：避免每次 cache hit 都走一次 metrics 包，
// Prometheus 拉取时再现读 atomic 计数器（O(1) 无锁）。
func (r *Registry) BindLocalCache(getHits, getMisses func() float64) {
	r.Registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "l1",
			Name:      "hits_total",
			Help:      "Total L1 (in-process LRU) cache hits.",
		},
		getHits,
	))
	r.Registry.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "l1",
			Name:      "misses_total",
			Help:      "Total L1 (in-process LRU) cache misses.",
		},
		getMisses,
	))
}

// EventBufferGetters 把 event.Buffer 的各 stats getter 打包，避免参数列表过长。
type EventBufferGetters struct {
	Enqueued func() float64
	Dropped  func() float64
	Flushed  func() float64
	FlushErr func() float64
	Used     func() float64
	Capacity func() float64
}

// BindEventBuffer 绑定 event buffer 全套指标（4 counter + 2 gauge）。
func (r *Registry) BindEventBuffer(g EventBufferGetters) {
	for _, m := range []struct {
		name string
		help string
		fn   func() float64
	}{
		{"enqueued_total", "Total events successfully enqueued.", g.Enqueued},
		{"dropped_total", "Total events dropped due to buffer full or stop.", g.Dropped},
		{"flushed_total", "Total events flushed to sink (PG batch insert).", g.Flushed},
		{"flush_err_total", "Total flush failures.", g.FlushErr},
	} {
		r.Registry.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{
				Namespace: Namespace,
				Subsystem: "event_buffer",
				Name:      m.name,
				Help:      m.help,
			},
			m.fn,
		))
	}

	r.Registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "event_buffer",
			Name:      "used",
			Help:      "Current number of events in buffer (channel length).",
		},
		g.Used,
	))
	r.Registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "event_buffer",
			Name:      "capacity",
			Help:      "Configured buffer capacity (channel cap).",
		},
		g.Capacity,
	))
}

// KafkaProducerGetters 把 event.KafkaProducer 的 stats getter 打包。
//
// 4 个 counter 对应 KafkaStats 字段（决策稿 §6.4）：
//   - sent_total：已交给 client buffer
//   - acked_total：broker 已确认
//   - dropped_total：100ms timeout drop（broker 不可达 / buffer 满）
//   - errors_total：broker 错误（非超时）
type KafkaProducerGetters struct {
	Sent    func() float64
	Acked   func() float64
	Dropped func() float64
	Errors  func() float64
}

// BindKafkaProducer 绑定 Kafka producer 全套 counter（4 个）。
//
// metric 命名遵循 Prometheus convention：
//
//	slink_kafka_producer_<name>_total
func (r *Registry) BindKafkaProducer(g KafkaProducerGetters) {
	for _, m := range []struct {
		name string
		help string
		fn   func() float64
	}{
		{"sent_total", "Total events handed off to kgo client buffer.", g.Sent},
		{"acked_total", "Total events acknowledged by broker.", g.Acked},
		{"dropped_total", "Total events dropped (send timeout or shutdown).", g.Dropped},
		{"errors_total", "Total non-timeout broker errors.", g.Errors},
	} {
		r.Registry.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{
				Namespace: Namespace,
				Subsystem: "kafka_producer",
				Name:      m.name,
				Help:      m.help,
			},
			m.fn,
		))
	}
}

// KafkaConsumerGetters 把 event.ClickEventConsumer 的 stats getter 打包。
//
// 5 个 counter 对应 ConsumerStats 字段（决策稿 §6.4 略调，spec 列了 processed_total
// + errors_total + lag_seconds，本实现细化为 5 个：polled / decoded / inserted /
// decode_errors / insert_errors，更利于排查 decode-vs-insert 哪段出问题。
// lag_seconds 留 P5 故障演练后看是否值得加 — 当前用 inserted/sent 比例代偿）。
type KafkaConsumerGetters struct {
	Polled       func() float64
	Decoded      func() float64
	Inserted     func() float64
	DecodeErrors func() float64
	InsertErrors func() float64
}

// BindKafkaConsumer 绑定 Kafka consumer 全套 counter（5 个）。
//
// metric 命名遵循 Prometheus convention：
//
//	slink_kafka_consumer_<name>_total
func (r *Registry) BindKafkaConsumer(g KafkaConsumerGetters) {
	for _, m := range []struct {
		name string
		help string
		fn   func() float64
	}{
		{"polled_total", "Total records polled from Kafka topic.", g.Polled},
		{"decoded_total", "Total records successfully JSON-decoded.", g.Decoded},
		{"inserted_total", "Total events written to PG via COPY FROM.", g.Inserted},
		{"decode_errors_total", "Total JSON decode failures (poison records skipped).", g.DecodeErrors},
		{"insert_errors_total", "Total BatchInsert failures (offset NOT committed).", g.InsertErrors},
	} {
		r.Registry.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{
				Namespace: Namespace,
				Subsystem: "kafka_consumer",
				Name:      m.name,
				Help:      m.help,
			},
			m.fn,
		))
	}
}

// BindIDGenerator 绑定 ID 号段使用率。
func (r *Registry) BindIDGenerator(getUsage func() float64) {
	r.Registry.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "id_segment",
			Name:      "usage",
			Help:      "Current segment usage ratio [0,1]; >0.9 triggers async refill.",
		},
		getUsage,
	))
}

// FastHTTPMiddleware 包一层 fasthttp.RequestHandler，记录请求 counter + 延迟 histogram。
//
// path label 用 normalizePath 控制基数：
//   - /healthz, /readyz, /api/links 原样保留
//   - 单段路径视为短码 → "/:code"
//   - 其他视为 "unknown"
//
// 这样 prometheus 的 path label 基数稳定在 5-6 种，不会因 ?code= 变化爆炸。
func (r *Registry) FastHTTPMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		start := time.Now()
		next(ctx)
		elapsed := time.Since(start).Seconds()

		path := normalizePath(string(ctx.Path()))
		method := string(ctx.Method())
		status := strconv.Itoa(ctx.Response.StatusCode())

		r.HTTP.Requests.WithLabelValues(path, method, status).Inc()
		r.HTTP.Duration.WithLabelValues(path).Observe(elapsed)
	}
}

// normalizePath 把真实 URL 收敛到固定的几个路由模板，控制 prometheus label 基数。
//
//	/healthz                  → /healthz
//	/readyz                   → /readyz
//	/api/links                → /api/links
//	/abc123 / /xyz999         → /:code（slink 跳转主路径）
//	其他多段 / 未知            → unknown
func normalizePath(p string) string {
	switch p {
	case "/", "/healthz", "/readyz", "/api/links":
		return p
	}
	if strings.HasPrefix(p, "/api/") {
		return "/api/*" // 收敛未来可能的其他 /api/* 路由
	}
	// 单段视为短码（/abcdef）
	if strings.Count(p, "/") == 1 {
		return "/:code"
	}
	return "unknown"
}
