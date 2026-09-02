// Package protocol 提供基于 trace-glow-contracts 的协议解码和校验。
package protocol

import (
	_ "embed"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	contracts "github.com/Trace-Glow/trace-glow-contracts/generated/go"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// 该 schema 从 pinned contracts 提交复制并随二进制发布，避免生产运行时访问外部仓库。
//go:embed contracts.schema.json
var contractsSchema []byte

var (
	envelopeSchema = compileSchema("https://schemas.trace-glow.dev/v1/contracts.schema.json#/$defs/Envelope")
	beaconSchema   = compileSchema("https://schemas.trace-glow.dev/v1/contracts.schema.json#/$defs/BeaconRequest")
)

// Envelope 和 BeaconRequest 直接复用 contracts 生成类型，防止 Collector 自行维护字段定义。
type Envelope = contracts.Envelope
type BeaconRequest = contracts.BeaconRequest
type TelemetryEvent = contracts.TelemetryEvent

// Batch 是 HTTP 层发布所需的统一批次视图，隐藏两种请求的鉴权字段差异。
type Batch struct {
	Events []TelemetryEvent
	SentAt time.Time
	APIKey string
}

// compileSchema 在进程启动时编译固定版本 schema；代码内 schema 损坏属于部署错误，应立即失败。
func compileSchema(ref string) *jsonschema.Schema {
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("https://schemas.trace-glow.dev/v1/contracts.schema.json", bytes.NewReader(contractsSchema)); err != nil {
		panic(fmt.Sprintf("compile contracts schema resource: %v", err))
	}
	schema, err := compiler.Compile(ref)
	if err != nil {
		panic(fmt.Sprintf("compile contracts schema %s: %v", ref, err))
	}
	return schema
}

// DecodeBatch 严格校验 JSON Schema 后再解码为 contracts 生成类型。
func DecodeBatch(body []byte, beacon bool) (Batch, error) {
	var document interface{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&document); err != nil {
		return Batch{}, errors.New("invalid json")
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Batch{}, errors.New("multiple json values")
	}
	schema := envelopeSchema
	if beacon {
		schema = beaconSchema
	}
	if err := schema.Validate(document); err != nil {
		return Batch{}, fmt.Errorf("contract validation failed: %w", err)
	}
	if beacon {
		var request BeaconRequest
		if err := json.Unmarshal(body, &request); err != nil {
			return Batch{}, fmt.Errorf("decode beacon request: %w", err)
		}
		return Batch{Events: request.Events, SentAt: request.SentAt, APIKey: request.ApiKey}, nil
	}
	var request Envelope
	if err := json.Unmarshal(body, &request); err != nil {
		return Batch{}, fmt.Errorf("decode envelope: %w", err)
	}
	return Batch{Events: request.Events, SentAt: request.SentAt}, nil
}

// ValidateBatchLimits 检查 schema 未表达但 Collector 必须执行的资源边界。
func ValidateBatchLimits(batch Batch, maxEvents int) error {
	if len(batch.Events) == 0 {
		return errors.New("batch must contain at least one event")
	}
	if len(batch.Events) > maxEvents {
		return fmt.Errorf("batch contains %d events, maximum is %d", len(batch.Events), maxEvents)
	}
	return nil
}
