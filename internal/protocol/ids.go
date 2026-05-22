package protocol

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// NewID 生成房间、连接、消息等非敏感标识。
// ID 只需要全局难猜和低碰撞，不承担鉴权；真正的加入权限由 roomKey HMAC 决定。
func NewID(prefix string) string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// rand.Read 失败极少见；返回可识别 fallback，便于日志暴露运行环境问题。
		return prefix + "-fallback"
	}
	return prefix + "-" + hex.EncodeToString(buf[:])
}

// NewSecret 生成 roomKey 这类敏感密钥，使用 32 字节随机数并用 URL 安全 base64 编码。
// roomKey 会进入邀请链接 fragment，因此不能包含需要额外转义的普通 base64 字符。
func NewSecret(prefix string) string {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return prefix + "-fallback"
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(buf[:])
}

// CleanID 清洗客户端上报的本地 ID；为空或过长时直接生成服务端 ID。
// 服务端还会在 Room.uniqueClientIDLocked 中处理房间内冲突。
func CleanID(value string, fallbackPrefix string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return NewID(fallbackPrefix)
	}
	return value
}

// CleanText 用于用户名、颜色、拒绝原因等短文本，避免异常客户端提交超长字符串撑大日志和内存。
func CleanText(value string, fallback string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if len(value) > max {
		return value[:max]
	}
	return value
}
