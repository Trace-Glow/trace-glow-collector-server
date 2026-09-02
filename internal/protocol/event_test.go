package protocol

import "testing"

// TestDecodeBatch 接受最新 contracts 中的 trace 事件和可选 span 字段。
func TestDecodeBatch(t *testing.T) {
	body := []byte(`{"sentAt":"2026-01-01T00:00:00Z","events":[{"schemaVersion":1,"id":"evt-1","timestamp":"2026-01-01T00:00:00Z","type":"trace","name":"span","level":"info","projectId":"project-1","sdk":{"name":"trace-glow","version":"1.0.0"},"payload":{},"spanId":"0123456789abcdef","spanKind":"server","spanStatus":"ok","durationMs":12.5}]}`)
	batch, err := DecodeBatch(body, false)
	if err != nil {
		t.Fatalf("valid contracts request rejected: %v", err)
	}
	if len(batch.Events) != 1 || batch.Events[0].ProjectId != "project-1" {
		t.Fatalf("unexpected decoded batch: %+v", batch)
	}
}

// TestDecodeBatchRejectsUnknownField 确保 Collector 严格执行 additionalProperties=false。
func TestDecodeBatchRejectsUnknownField(t *testing.T) {
	body := []byte(`{"sentAt":"2026-01-01T00:00:00Z","events":[],"unexpected":true}`)
	if _, err := DecodeBatch(body, false); err == nil {
		t.Fatal("request with unknown field was accepted")
	}
}

// TestDecodeBeaconBatch 验证 Beacon body apiKey 和标准批次字段均来自生成协议类型。
func TestDecodeBeaconBatch(t *testing.T) {
	body := []byte(`{"apiKey":"write-key","sentAt":"2026-01-01T00:00:00Z","events":[{"schemaVersion":1,"id":"evt-1","timestamp":"2026-01-01T00:00:00Z","type":"log","name":"message","level":"debug","projectId":"project-1","sdk":{"name":"trace-glow","version":"1.0.0"},"payload":{"message":"ok"}}]}`)
	batch, err := DecodeBatch(body, true)
	if err != nil {
		t.Fatalf("valid beacon request rejected: %v", err)
	}
	if batch.APIKey != "write-key" {
		t.Fatalf("unexpected beacon key: %q", batch.APIKey)
	}
}

// TestValidateBatchLimits 确保资源限制在进入可靠队列前生效。
func TestValidateBatchLimits(t *testing.T) {
	if err := ValidateBatchLimits(Batch{}, 1); err == nil {
		t.Fatal("empty batch was accepted")
	}
}
