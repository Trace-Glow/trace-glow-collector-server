// Package httptransport 提供 Collector 的 HTTP 接入层。
package httptransport

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Trace-Glow/trace-glow-collector-server/internal/auth"
	"github.com/Trace-Glow/trace-glow-collector-server/internal/config"
	"github.com/Trace-Glow/trace-glow-collector-server/internal/protocol"
	"github.com/gin-gonic/gin"
)

// Publisher 是 HTTP 接入层所需的最小持久化接口。
type Publisher interface {
	Publish(context.Context, []protocol.TelemetryEvent) error
}

var errRequestBodyTooLarge = errors.New("request body too large")

// NewRouter 创建健康检查和事件接收路由。
func NewRouter(cfg config.Config, publisher Publisher) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	router.GET("/readyz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ready", "clickhouse": cfg.ClickHouseAddr != ""}) })
	router.POST("/v1/events", EventsHandler(cfg, publisher))
	return router
}

// DecodeBody 解压请求并限制解压后大小，防止压缩炸弹耗尽内存。
func DecodeBody(c *gin.Context, limit int64) ([]byte, error) {
	var reader io.Reader = io.LimitReader(c.Request.Body, limit+1)
	encoding := strings.TrimSpace(strings.ToLower(c.GetHeader("Content-Encoding")))
	if encoding != "" && encoding != "gzip" {
		return nil, errors.New("unsupported content encoding")
	}
	if encoding == "gzip" {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, errors.New("invalid gzip body")
		}
		defer gz.Close()
		reader = io.LimitReader(gz, limit+1)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errRequestBodyTooLarge
	}
	return body, nil
}

// Authenticate 验证标准请求头或 Beacon body 凭证，禁止两种凭证同时出现。
func Authenticate(c *gin.Context, batch protocol.Batch, expected string, beacon bool) bool {
	headerKey := c.GetHeader("X-Trace-Glow-Key")
	if beacon {
		return headerKey == "" && auth.ConstantTimeEqual(batch.APIKey, expected)
	}
	return batch.APIKey == "" && auth.ConstantTimeEqual(headerKey, expected)
}

// EventsHandler 完成协议分支选择、解码、鉴权、资源限制和消息发布。
func EventsHandler(cfg config.Config, publisher Publisher) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
			c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": "content type must be application/json"})
			return
		}
		body, err := DecodeBody(c, cfg.MaxBodyBytes)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errRequestBodyTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			c.JSON(status, gin.H{"error": err.Error()})
			return
		}
		beacon := c.GetHeader("X-Trace-Glow-Key") == ""
		batch, err := protocol.DecodeBatch(body, beacon)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request contract"})
			return
		}
		if !Authenticate(c, batch, cfg.WriteKey, beacon) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
			return
		}
		if err := protocol.ValidateBatchLimits(batch, cfg.MaxEvents); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid event count"})
			return
		}
		if err := publisher.Publish(c.Request.Context(), batch.Events); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event persistence unavailable"})
			return
		}
		c.Status(http.StatusAccepted)
	}
}
