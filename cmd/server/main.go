// Package main is the slink server entrypoint.
//
// v0.1 占位实现：仅启动一个最小 HTTP 服务确认骨架可跑。
// 真实实现从 Day 2 起逐步填充：
//   - Day 2: config 加载、PG/Redis 客户端、健康检查
//   - Day 3: 号段发号器、Base62
//   - Day 4: POST /api/links
//   - Day 5: GET /:code 跳转
//   - Day 6: 异步事件入库
//   - Day 7: Docker 镜像 + wrk 压测
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := os.Getenv("SLINK_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","version":"v0.1-day1"}`))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("slink listening on %s (skeleton, not functional yet)", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("bye")
}
