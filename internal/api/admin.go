// Package api 的 admin 观测 handler（Day 11 从 cmd/server/main.go 抽出）。
//
// /debug/stats 一站式观测：L1 命中率 + event buffer 状态 + ID 号段进度 + uptime。
//
// 仅挂 admin 端口（默认 127.0.0.1:6060），不暴露给外网。
//
// 用途：
//  1. bench 后核对 L1 hit rate（不再靠 profile 间接估）
//  2. 监控 event buffer Used/Capacity，提前预警 dropped
//  3. 简历讲故事时有现成数字（"L1 命中 99.7% / dropped 0 / uptime 2 小时"）
//
// 同端口 /metrics 由 prometheus client 提供，本文件只负责手写 JSON 视角。
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/id"
)

type localCacheStatsView struct {
	Hits    uint64  `json:"hits"`
	Misses  uint64  `json:"misses"`
	HitRate float64 `json:"hit_rate"` // 计算字段，便于一眼看出
}

type linkCacheStats struct {
	L1 localCacheStatsView `json:"l1"`
}

type statsResp struct {
	Version       string         `json:"version"`
	UptimeSeconds int64          `json:"uptime_seconds"`
	LinkCache     linkCacheStats `json:"link_cache"`
	EventBuffer   event.Stats    `json:"event_buffer"`
	IDGenerator   id.BufferStat  `json:"id_generator"`
}

// Stats 返回 admin /debug/stats 的 net/http handler。
//
// 用 net/http 而不是 fasthttp 是因为它挂在 admin :6060，
// 该端口同时托管 net/http/pprof（标准库）和 prometheus client_golang
// 的 promhttp.Handler，后两者都是 net/http 接口，统一栈更省事。
func Stats(
	version string,
	lc *cache.LinkCache,
	eb *event.Buffer,
	gen *id.Generator,
	startTime time.Time,
) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		l1 := lc.LocalStats()
		hitRate := 0.0
		if total := l1.Hits + l1.Misses; total > 0 {
			hitRate = float64(l1.Hits) / float64(total)
		}

		resp := statsResp{
			Version:       version,
			UptimeSeconds: int64(time.Since(startTime).Seconds()),
			LinkCache: linkCacheStats{
				L1: localCacheStatsView{
					Hits:    l1.Hits,
					Misses:  l1.Misses,
					HitRate: hitRate,
				},
			},
			EventBuffer: eb.Stats(),
			IDGenerator: gen.Stat(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
