// Package api 的 admin 观测 handler（Day 11 从 cmd/server/main.go 抽出）。
//
// /debug/stats 一站式观测：L1 命中率 + Kafka producer 状态 + ID 号段进度 + uptime。
//
// 仅挂 admin 端口（默认 127.0.0.1:6060），不暴露给外网。
//
// 用途：
//  1. bench 后核对 L1 hit rate（不再靠 profile 间接估）
//  2. 监控 Kafka producer dropped/errors，提前预警
//  3. 一站式拉取关键运行指标，方便排障/回顾
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
	Version       string            `json:"version"`
	UptimeSeconds int64             `json:"uptime_seconds"`
	LinkCache     linkCacheStats    `json:"link_cache"`
	KafkaProducer *event.KafkaStats `json:"kafka_producer,omitempty"`
	IDGenerator   id.BufferStat     `json:"id_generator"`
}

// Stats 返回 admin /debug/stats 的 net/http handler。
//
// 用 net/http 而不是 fasthttp 是因为它挂在 admin :6060，
// 该端口同时托管 net/http/pprof（标准库）和 prometheus client_golang
// 的 promhttp.Handler，后两者都是 net/http 接口，统一栈更省事。
//
// v0.4 Day 16 切流后只有 KafkaProducer 一种 backend（buffer/dual 删除，
// 代码考古见 git tag v0.3-buffer-final）。kp 为 nil 是配置 bug，render 跳过该字段。
func Stats(
	version string,
	lc *cache.LinkCache,
	kp *event.KafkaProducer,
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
			IDGenerator: gen.Stat(),
		}
		if kp != nil {
			s := kp.Stats()
			resp.KafkaProducer = &s
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
