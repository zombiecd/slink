// Package otel 是 slink v0.6 Phase 4.1 OTel trace 接入。
//
// 设计取舍（决策稿 docs/architecture/v0.6-phase4.md §7）：
//   - code-level 注入（不用 auto-instrumentation agent）
//   - OTLP gRPC exporter（不用 stdout/HTTP）
//   - 独立 collector Deployment（Pod 内 SDK 直发 collector）
//
// 启用条件：环境变量 SLINK_OTEL_ENDPOINT 非空。空时 InitTracer 返回 noop tracer，
// 任何 Tracer().Start(...) 调用都是无副作用空操作（v0.6 hardening 同款 graceful 路径）。
package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/zombiecd/slink"
)

// Config 是 OTel 初始化所需最小配置。
type Config struct {
	Endpoint    string // SLINK_OTEL_ENDPOINT，例 "otel-collector:4317" / "" = 禁用
	ServiceName string // 例 "slink-server" / "slink-consumer"
	ServiceVer  string // 例 "v0.6"
	Env         string // 例 "dev" / "prod"
}

// Shutdown 在 main 退出时调用，保证 in-flight span flush。
type Shutdown func(context.Context) error

// InitTracer 按 Config 初始化全局 Tracer + Propagator。
//
// 返回 shutdown 函数。当 Endpoint 为空时返回 noop shutdown（不初始化 SDK）。
func InitTracer(ctx context.Context, cfg Config) (Shutdown, error) {
	if cfg.Endpoint == "" {
		// 禁用模式：不安装 sdktrace.TracerProvider，全局 otel.Tracer() 返回 noop
		return func(context.Context) error { return nil }, nil
	}

	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVer),
			semconv.DeploymentEnvironment(cfg.Env),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: resource: %w", err)
	}

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(3*time.Second),
	))
	if err != nil {
		return nil, fmt.Errorf("otel: exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer 返回 slink 全局 tracer。空 endpoint 模式下返回 noop tracer。
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// StringAttr 是常用 attribute helper（避免散落 attribute.String 调用）。
func StringAttr(k, v string) attribute.KeyValue { return attribute.String(k, v) }

// IntAttr 同上。
func IntAttr(k string, v int) attribute.KeyValue { return attribute.Int(k, v) }
