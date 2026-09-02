// Package config 提供服务启动配置的读取和校验。
package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// Config 集中管理 HTTP、Kafka 和 ClickHouse 的运行参数。
type Config struct {
	HTTPAddr       string
	KafkaBrokers   []string
	KafkaTopic     string
	ClickHouseAddr string
	MaxBodyBytes   int64
	MaxEvents      int
	WriteKey       string
}

// Load 从环境变量读取配置，并在启动阶段拒绝不完整配置。
func Load() (Config, error) {
	c := Config{
		HTTPAddr:       os.Getenv("HTTP_ADDR"),
		KafkaTopic:     os.Getenv("KAFKA_TOPIC"),
		ClickHouseAddr: os.Getenv("CLICKHOUSE_ADDR"),
		WriteKey:       os.Getenv("TRACE_GLOW_WRITE_KEY"),
		MaxBodyBytes:   envInt64("MAX_BODY_BYTES", 1<<20),
		MaxEvents:      int(envInt64("MAX_EVENTS", 100)),
	}
	if c.HTTPAddr == "" { c.HTTPAddr = ":8080" }
	if c.KafkaTopic == "" { c.KafkaTopic = "trace-glow-events" }
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" { c.KafkaBrokers = append(c.KafkaBrokers, broker) }
	}
	if len(c.KafkaBrokers) == 0 { return c, errors.New("KAFKA_BROKERS is required") }
	if c.WriteKey == "" { return c, errors.New("TRACE_GLOW_WRITE_KEY is required") }
	if c.MaxBodyBytes <= 0 || c.MaxEvents <= 0 { return c, errors.New("MAX_BODY_BYTES and MAX_EVENTS must be positive") }
	return c, nil
}

// envInt64 读取整数环境变量，无法解析时使用默认值。
func envInt64(name string, fallback int64) int64 {
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil { return fallback }
	return v
}
