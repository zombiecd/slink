// Package api 的健康检查 handler（Day 11 从 cmd/server/main.go 抽出）。
//
// 设计参考 K8s probe 语义：
//
//	/healthz —— liveness probe
//	  仅检查"进程是否还活着"，绝不依赖外部组件。
//	  Pod 卡死时 K8s 据此重启它。
//
//	/readyz  —— readiness probe
//	  并行 ping 所有外部依赖（PG / Redis），任一失败返回 503。
//	  依赖暂时不可达时 LB 摘流量，但 Pod 不重启。
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

// PGPinger 是 readiness 检查 PostgreSQL 可达性的最小接口。
//
// 这里不直接吃 *pgxpool.Pool 是为了：
//   - api 层不反向依赖 store 包的具体实现
//   - 单测可用 stub 替换
type PGPinger interface {
	Ping(context.Context) error
}

// RedisPinger 同上，PG/Redis 用对称接口便于后续扩展更多依赖。
type RedisPinger interface {
	Ping(context.Context) error
}

type readyResp struct {
	Status   string            `json:"status"`
	Version  string            `json:"version"`
	Backends map[string]string `json:"backends"`
}

// Liveness 返回 fasthttp 形式的 liveness probe handler。
// version 由 main 包传入（build 信息归属 main，api 不持有）。
func Liveness(version string) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Content-Type", "application/json")
		_ = json.NewEncoder(ctx).Encode(map[string]string{
			"status":  "ok",
			"version": version,
		})
	}
}

// Readiness 并行 ping 所有依赖，全部成功才返回 200，任一失败返回 503。
//
// probe 超时 2s 是为了配合 K8s 默认 timeoutSeconds=1：
// 留 1s 给网络栈，防 K8s 直接判 timeout 把 Pod 摘掉。
func Readiness(version string, pg PGPinger, rd RedisPinger) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		var (
			wg  sync.WaitGroup
			mu  sync.Mutex
			ok  = true
			res = make(map[string]string, 2)
		)
		check := func(name string, fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				mu.Lock()
				ok = false
				res[name] = "fail: " + err.Error()
				mu.Unlock()
				return
			}
			mu.Lock()
			res[name] = "ok"
			mu.Unlock()
		}

		wg.Add(2)
		go check("postgres", func() error { return pg.Ping(probeCtx) })
		go check("redis", func() error { return rd.Ping(probeCtx) })
		wg.Wait()

		ctx.Response.Header.Set("Content-Type", "application/json")
		if !ok {
			ctx.SetStatusCode(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(ctx).Encode(readyResp{
			Status:   statusFor(ok),
			Version:  version,
			Backends: res,
		})
	}
}

func statusFor(ok bool) string {
	if ok {
		return "ok"
	}
	return "degraded"
}
