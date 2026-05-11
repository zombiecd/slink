package otel

import (
	"context"
	"strconv"

	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// fasthttpHeaderCarrier 让 propagation.TextMapPropagator 能读 fasthttp request header。
//
// W3C traceparent header 名固定 "traceparent" / "tracestate"，扫常见 propagation 字段即可。
type fasthttpHeaderCarrier struct {
	h *fasthttp.RequestHeader
}

func (c fasthttpHeaderCarrier) Get(key string) string {
	return string(c.h.Peek(key))
}

func (c fasthttpHeaderCarrier) Set(key, value string) {
	c.h.Set(key, value)
}

func (c fasthttpHeaderCarrier) Keys() []string {
	keys := make([]string, 0, c.h.Len())
	c.h.VisitAll(func(k, _ []byte) {
		keys = append(keys, string(k))
	})
	return keys
}

var _ propagation.TextMapCarrier = fasthttpHeaderCarrier{}

// FasthttpMiddleware 创建 server-kind span 包裹 next handler。
//
// 行为：
//  1. 从 request header 提取 W3C traceparent（client 已设置时延续 trace）
//  2. 创建 server span 名 "<METHOD> <route>"，例 "GET /:code"
//  3. 写入 http.* / net.* attribute（semconv 标准）
//  4. 业务 handler 执行
//  5. 根据 status code 写 span Status + http.status_code attribute
//
// 当 OTel 未初始化（noop tracer），中间件仍跑但 span 是 noop，0 开销。
func FasthttpMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	tracer := Tracer()
	propagator := newPropagator()

	return func(ctx *fasthttp.RequestCtx) {
		method := string(ctx.Method())
		path := string(ctx.Path())

		// 提取 client 端 trace context
		parentCtx := propagator.Extract(
			context.Background(),
			fasthttpHeaderCarrier{h: &ctx.Request.Header},
		)

		spanCtx, span := tracer.Start(
			parentCtx,
			method+" "+normalizePath(path),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(method),
				semconv.URLPath(path),
				semconv.NetworkPeerAddress(ctx.RemoteIP().String()),
			),
		)
		defer span.End()

		// 把 trace context 挂到 fasthttp ctx，下游 handler 可取
		ctx.SetUserValue(traceCtxKey, spanCtx)

		next(ctx)

		statusCode := ctx.Response.StatusCode()
		span.SetAttributes(attribute.Int("http.status_code", statusCode))
		if statusCode >= 500 {
			span.SetStatus(codes.Error, strconv.Itoa(statusCode))
		}
	}
}

// CtxFromFasthttp 从 fasthttp ctx 取出 OTel context（已含 server span）。
//
// 业务 handler 需要在下游创建 child span 时用：
//
//	otelCtx := otel.CtxFromFasthttp(ctx)
//	_, span := otel.Tracer().Start(otelCtx, "kafka.publish")
//
// 未启用 OTel / 未挂 span 时返回 context.Background()。
func CtxFromFasthttp(ctx *fasthttp.RequestCtx) context.Context {
	if v := ctx.UserValue(traceCtxKey); v != nil {
		if c, ok := v.(context.Context); ok {
			return c
		}
	}
	return context.Background()
}

// InjectKafkaHeaders 把当前 ctx 的 trace context 写入 Kafka record headers（W3C format）。
//
// KafkaProducer 在 produce 前调用让 consumer 能 extract context 续 trace。
func InjectKafkaHeaders(ctx context.Context, set func(key, value string)) {
	carrier := setterCarrier{set: set}
	newPropagator().Inject(ctx, carrier)
}

// 实现细节

type traceCtxKeyType struct{}

var traceCtxKey = traceCtxKeyType{}

type setterCarrier struct {
	set func(key, value string)
}

func (s setterCarrier) Get(string) string  { return "" }
func (s setterCarrier) Set(k, v string)    { s.set(k, v) }
func (s setterCarrier) Keys() []string     { return nil }

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// normalizePath 把 /:code 这类参数化路径折成静态形式，避免 trace span name 基数爆炸。
//
// fasthttp/router 已 path 解析过，但 ctx.Path() 是原始字符串，所以仍需 normalize。
// 简化版：6-8 字符 alnum 段（slink code 长度区间）替换成 "/:code"。
func normalizePath(p string) string {
	// 静态路径直接返回
	if p == "/" || p == "/healthz" || p == "/api/links" {
		return p
	}
	if len(p) > len("/api/stats/") && p[:len("/api/stats/")] == "/api/stats/" {
		return p
	}
	// 单段疑似 short code（/abc12X）
	if len(p) >= 4 && len(p) <= 12 && p[0] == '/' && isAlnum(p[1:]) {
		return "/:code"
	}
	return p
}

func isAlnum(s string) bool {
	for _, c := range s {
		ok := (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9')
		if !ok {
			return false
		}
	}
	return true
}
