// Package auth 提供请求凭证校验。
package auth

import "crypto/subtle"

// ConstantTimeEqual 使用恒定时间比较，避免 write key 比较产生时序泄露。
func ConstantTimeEqual(provided, expected string) bool {
	return provided != "" && len(provided) == len(expected) && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}
