package api

import (
	"net/http"
	"strings"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/store"
)

// Server 是 slink HTTP 接口层的依赖容器 + 路由装配点。
//
// 由 main.go 在启动期构造一次：
//
//	srv := api.NewServer(api.Config{...}, generator, linkRepo, linkCache, eventer)
//	mux := srv.Routes()
//	http.Server{Handler: mux}
type Server struct {
	cfg       Config
	generator *id.Generator
	links     *store.LinkRepo
	linkCache *cache.LinkCache
	events    event.Eventer
}

// Config 是 api 层需要的最小子集（不直接吃 *config.Config，避免反向依赖）。
type Config struct {
	BaseURL string // 用于生成 short_url 字段（如 https://o.cn）
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

// Routes 返回包含所有 HTTP 路由的 mux。
//
// 路由表：
//
//	POST /api/links          创建短链（Day 4）
//	GET  /api/links/{code}   v0.5+，读元数据（暂不实现）
//	GET  /{code}             跳转（Day 5）
//
// Go 1.22+ ServeMux 支持 method matching，单 mux 即可。
//
// 注意路由优先级：ServeMux 规则越具体优先级越高，
// "/api/" 前缀比 "/{code}" 更具体，所以 /api/* 不会被 /{code} 匹配走。
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/links", s.handleCreateLink)
	mux.HandleFunc("GET /{code}", s.handleRedirect)
	return mux
}
