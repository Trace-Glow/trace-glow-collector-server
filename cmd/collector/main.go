package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Trace-Glow/trace-glow-collector-server/internal/config"
	"github.com/Trace-Glow/trace-glow-collector-server/internal/queue"
	httptransport "github.com/Trace-Glow/trace-glow-collector-server/internal/transport/http"
)

// main 负责依赖组装、HTTP 服务启动和优雅停机，不承载业务逻辑。
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.Load()
	if err != nil { slog.Error("invalid configuration", "error", err); os.Exit(1) }
	publisher := queue.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer publisher.Close()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httptransport.NewRouter(cfg, publisher)}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { slog.Error("http server failed", "error", err); os.Exit(1) }
	}()
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
