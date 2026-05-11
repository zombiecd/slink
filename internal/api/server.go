package api

import (
	"net/netip"
	"strings"

	"github.com/fasthttp/router"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/store"
)

// Server 是 slink HTTP 接口层的依赖容器 + 路由装配点。
//
// v0.2 起接口层底层改用 valyala/fasthttp + fasthttp/router：
//
//	srv := api.NewServer(api.Config{...}, generator, linkRepo, linkCache, eventer)
//	r   := srv.Routes()
//	server := &fasthttp.Server{Handler: r.Handler}
//
// 切换原因见 docs/bench/day-07-fasthttp.md（Day 6 profile 显示 net/http 标准库
// 单进程在 21k RPS 处被 syscall + netpoll 框死，要破必须换底）。
type Server struct {
	cfg       Config
	generator *id.Generator
	links     *store.LinkRepo
	linkCache *cache.LinkCache
	events    event.Eventer
	// stats 是 v0.5 新增 ClickHouse 分析查询数据源。
	// 可为 nil（v0.1-v0.4 + 测试场景）：handler 会返回 503 不影响主路径。
	stats statsRepo
}

// SetStats 在 NewServer 之后注入 ClickHouse 分析数据源（v0.5 +）。
//
// 单独 setter 避免 NewServer 签名爆炸（已有 5 参）。也允许 server 启动后
// 异步注入（如 CH 尚未 healthy 时先返回 503，healthy 后再 SetStats）。
func (s *Server) SetStats(r statsRepo) { s.stats = r }

// Config 是 api 层需要的最小子集（不直接吃 *config.Config，避免反向依赖）。
type Config struct {
	BaseURL string // 用于生成 short_url 字段（如 https://o.cn）

	// TrustedProxies 是反向代理白名单（CIDR）。
	// 仅当 RemoteAddr 命中其中一条时，才信任 X-Forwarded-For / X-Real-IP；
	// 否则把 RemoteAddr 当真实 IP（v0.3 H6 hardening）。
	// nil = 不信任 XFF（最安全的默认）。
	TrustedProxies []netip.Prefix
}

// NewServer 构造 Server。
//
// linkCache / events 可为 nil（v0.1 早期 / 测试场景）：
//   - linkCache 为 nil 时，跳转每次都打 DB（仅供测试）
//   - events 为 nil 时，跳转不投递事件
//
// 生产装配应同时传入两者。
func NewServer(
	cfg Config,
	gen *id.Generator,
	links *store.LinkRepo,
	linkCache *cache.LinkCache,
	events event.Eventer,
) *Server {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Server{
		cfg:       cfg,
		generator: gen,
		links:     links,
		linkCache: linkCache,
		events:    events,
	}
}

// Routes 返回 fasthttp 路由器。
//
// 路由表：
//
//	POST /api/links          创建短链（Day 4）
//	GET  /{code}             跳转（Day 5）
//
// fasthttp/router 用 /{name} 语法（与 Go 1.22+ ServeMux 同），
// 通过 ctx.UserValue("code").(string) 取路径参数。
//
// 路由优先级：静态路径（/api/links）优先于带参数路径（/{code}），
// 所以 /api/* 不会被 /{code} 误吞。
func (s *Server) Routes() *router.Router {
	r := router.New()
	r.POST("/api/links", s.handleCreateLink)
	// v0.5 分析查询（仅当 stats 注入时实际工作；未注入时 handler 返回 503）
	r.GET("/api/stats/uv", s.handleStatsUV)
	r.GET("/api/stats/topk", s.handleStatsTopK)
	r.GET("/api/stats/timeseries", s.handleStatsTimeseries)
	r.GET("/{code}", s.handleRedirect)
	return r
}
