package api

import (
	"net/http"
	"strings"

	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/store"
)

// Server 是 slink HTTP 接口层的依赖容器 + 路由装配点。
//
// 由 main.go 在启动期构造一次：
//
//	srv := api.NewServer(api.Config{...}, generator, linkRepo)
//	mux := srv.Routes()
//	http.Server{Handler: mux}
type Server struct {
	cfg       Config
	generator *id.Generator
	links     *store.LinkRepo
}

// Config 是 api 层需要的最小子集（不直接吃 *config.Config，避免反向依赖）。
type Config struct {
	BaseURL string // 用于生成 short_url 字段（如 https://o.cn）
}

// NewServer 构造 Server。
// 不验证 generator / links 非 nil — 调用方保证（main.go 的装配责任）。
func NewServer(cfg Config, gen *id.Generator, links *store.LinkRepo) *Server {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Server{
		cfg:       cfg,
		generator: gen,
		links:     links,
	}
}

// Routes 返回包含所有 API 路由的 mux。
//
// /api/links             POST  创建短链
// /api/links/{code}      GET   v0.5+，读元数据（暂不实现）
// /:code                 GET   跳转（Day 5 实现）
//
// Go 1.22+ ServeMux 支持 method matching，单 mux 即可。
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/links", s.handleCreateLink)
	return mux
}
