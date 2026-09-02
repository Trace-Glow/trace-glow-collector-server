// Package queue 封装可靠消息队列发布能力。
package queue

import (
	"context"
	"encoding/json"

	"github.com/Trace-Glow/trace-glow-collector-server/internal/protocol"
	"github.com/segmentio/kafka-go"
)

// Publisher 是 HTTP 接入层依赖的最小发布接口，便于单元测试替换。
type Publisher interface { Publish(context.Context, []protocol.TelemetryEvent) error }

// KafkaPublisher 将事件写入 Kafka，并等待 broker 确认。
type KafkaPublisher struct { writer *kafka.Writer }

// NewKafkaPublisher 创建启用全部副本确认的 Kafka publisher。
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}}
}

// Publish 发布一批事件，使用 projectId 作为消息 key 保持项目内顺序。
func (p *KafkaPublisher) Publish(ctx context.Context, events []protocol.TelemetryEvent) error {
	msgs := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil { return err }
		msgs = append(msgs, kafka.Message{Key: []byte(event.ProjectId), Value: value})
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

// Close 释放 Kafka writer 连接。
func (p *KafkaPublisher) Close() error { return p.writer.Close() }
