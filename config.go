package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const defaultConfigPath = "config.json"

// AppConfig 是服务端运行配置。
// JSON 文件是主配置来源；环境变量只作为部署平台注入 secret 或临时覆盖的兜底方式。
type AppConfig struct {
	// Addr 同时用于 HTTP/WS TCP 监听和内置 STUN UDP 监听；部署时需要同时放行 TCP/UDP。
	Addr string `json:"addr"`
	// DBPath 保存房间密钥哈希、Yjs compact_state 和增量 update。
	DBPath string `json:"dbPath"`
	// LogPath/LogMaxBytes 控制单一主日志文件，避免诊断时同一事件散落到多个长期目录。
	LogPath     string `json:"logPath"`
	LogMaxBytes int64  `json:"logMaxBytes"`
	// Secret 用于 roomKey HMAC。生产环境必须固定配置，否则重启或迁移后旧邀请会失效。
	Secret string `json:"secret"`
	// CORSOrigins 是公网/局域网部署的显式白名单；本机开发由 AllowLoopbackOrigins 兜底。
	CORSOrigins          []string `json:"corsOrigins"`
	AllowLoopbackOrigins bool     `json:"allowLoopbackOrigins"`
	// MaxMessageBytes 限制单条 WebSocket 消息，主要防止异常快照或大素材包打爆服务端内存。
	MaxMessageBytes int64 `json:"maxMessageBytes"`
	// MaxUpdateBytes 限制单条 Yjs 增量二进制大小，超过则拒绝并通知客户端。
	MaxUpdateBytes int64 `json:"maxUpdateBytes"`
	// MaxSnapshotBytes 限制单次 Yjs 全量快照二进制大小。
	MaxSnapshotBytes int64 `json:"maxSnapshotBytes"`
	// RoomMaxStorageBytes 限制单房间累计 SQLite 存储字节数，超过则拒绝新的 update/snapshot。
	RoomMaxStorageBytes int64 `json:"roomMaxStorageBytes"`
	// AuthTimeout 是 WebSocket 首帧 auth 等待超时；慢网络可调大。
	AuthTimeout time.Duration `json:"authTimeout"`
	// ReadDeadline 是认证后单次读超时间隔，主要支撑心跳检测。
	ReadDeadline time.Duration `json:"readDeadline"`
	// RoomTTLDays 是房间不活跃（updated_at）超过该天数后由清理 goroutine 删除的阈值。
	RoomTTLDays int `json:"roomTTLDays"`
	// CleanupInterval 是后台清理 goroutine 的执行间隔。
	CleanupInterval time.Duration `json:"cleanupInterval"`
	// MaxRoomsPerIPPerMinute 限制单 IP 调用 /api/rooms 的频率，防止数据库放大攻击。
	MaxRoomsPerIPPerMinute int `json:"maxRoomsPerIPPerMinute"`
	// MaxWSConnPerIPPerMinute 限制单 IP WebSocket 连接频率，防止连接耗尽和通过 WS 路径批量创建房间。
	MaxWSConnPerIPPerMinute int `json:"maxWSConnPerIPPerMinute"`
	// AllowedServerHosts 限制 handleCreateRoom 接受的 serverUrl host 白名单；空表示只校验格式。
	AllowedServerHosts []string `json:"allowedServerHosts"`
	// TrustedProxies 是反向代理 IP/CIDR 白名单；只有来自受信任代理的 X-Forwarded-For 才会被信任。
	// 不配置时 X-Forwarded-For 头被忽略，速率限制等 IP 维度防护使用 TCP 对端地址。
	TrustedProxies []string `json:"trustedProxies"`
	// InsecureAllowDefaultSecret 仅供本地开发：允许非 loopback 监听且未配置 secret 时仍启动。
	// 生产环境绝对不要开启，否则任何人都能用源码里的开发默认值伪造 roomKey。
	InsecureAllowDefaultSecret bool `json:"insecureAllowDefaultSecret"`
	// ConfigPath 不写入 JSON，只用于启动日志展示实际加载了哪个配置文件。
	ConfigPath string `json:"-"`
}

// devDefaultSecret 与 NewHub 中的兜底值保持一致，validateConfig 据此识别"未配置 secret"。
const devDefaultSecret = "timenotes-dev-secret"

// loadConfig 的优先级是：默认值 -> config.json/TIMENOTES_CONFIG 指定文件 -> 环境变量覆盖。
// 这样本地双击运行可以零配置，服务部署又能通过环境变量注入密钥和路径。
func loadConfig() (AppConfig, error) {
	cfg := defaultConfig()
	configPath := strings.TrimSpace(os.Getenv("TIMENOTES_CONFIG"))
	if configPath == "" {
		configPath = defaultConfigPath
	}
	cfg.ConfigPath = configPath

	if body, err := os.ReadFile(configPath); err == nil {
		// JSON 配置允许覆盖默认值；未写的字段保留 defaultConfig 里的安全/开发默认。
		if err := json.Unmarshal(body, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", configPath, err)
		}
		cfg.ConfigPath = configPath
	} else if !errors.Is(err, os.ErrNotExist) || strings.TrimSpace(os.Getenv("TIMENOTES_CONFIG")) != "" {
		// 用户显式指定配置文件时，读不到应视为启动错误；默认 config.json 不存在则允许用默认配置启动。
		return cfg, fmt.Errorf("read config %s: %w", configPath, err)
	}

	applyEnvOverrides(&cfg)
	return cfg, validateConfig(cfg)
}

// defaultConfig 偏向本机开发：只监听 127.0.0.1、允许 loopback CORS、数据库和日志落在服务端目录下。
// 大小上限采用「宽松档」默认值，覆盖富媒体笔记（3D 模型、长音频）场景。
func defaultConfig() AppConfig {
	return AppConfig{
		Addr:                     "127.0.0.1:8787",
		DBPath:                   filepath.Join("data", "timenotes-collab.db"),
		LogPath:                  filepath.Join("logs", "timenotes-collab.log"),
		LogMaxBytes:              5 * 1024 * 1024,
		CORSOrigins:              []string{},
		AllowLoopbackOrigins:     true,
		MaxMessageBytes:          128 * 1024 * 1024,
		MaxUpdateBytes:           8 * 1024 * 1024,
		MaxSnapshotBytes:         64 * 1024 * 1024,
		RoomMaxStorageBytes:      512 * 1024 * 1024,
		AuthTimeout:              15 * time.Second,
		ReadDeadline:             30 * time.Second,
		RoomTTLDays:              30,
		CleanupInterval:          time.Hour,
		MaxRoomsPerIPPerMinute:   10,
		MaxWSConnPerIPPerMinute:  30,
		InsecureAllowDefaultSecret: false,
	}
}

// applyEnvOverrides 给容器、systemd、计划任务等部署方式提供覆盖入口。
// 这里不因环境变量解析失败直接退出，最终由 validateConfig 做统一校验。
func applyEnvOverrides(cfg *AppConfig) {
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_ADDR")); value != "" {
		cfg.Addr = value
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_DB")); value != "" {
		cfg.DBPath = value
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_LOG")); value != "" {
		cfg.LogPath = value
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_SECRET")); value != "" {
		cfg.Secret = value
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_CORS_ORIGINS")); value != "" {
		// 多 origin 用逗号分隔，避免在 JSON 文件外还要维护复杂数组格式。
		cfg.CORSOrigins = splitCSV(value)
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_LOG_MAX_BYTES")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			cfg.LogMaxBytes = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_MAX_MESSAGE_BYTES")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			cfg.MaxMessageBytes = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_MAX_UPDATE_BYTES")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			cfg.MaxUpdateBytes = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_MAX_SNAPSHOT_BYTES")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			cfg.MaxSnapshotBytes = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_ROOM_MAX_STORAGE")); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			cfg.RoomMaxStorageBytes = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_MAX_WS_CONN_PER_MIN")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			cfg.MaxWSConnPerIPPerMinute = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_AUTH_TIMEOUT")); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			cfg.AuthTimeout = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_ROOM_TTL_DAYS")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			cfg.RoomTTLDays = parsed
		}
	}
	if value := strings.TrimSpace(os.Getenv("TIMENOTES_INSECURE_DEFAULT_SECRET")); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			cfg.InsecureAllowDefaultSecret = parsed
		}
	}
}

// validateConfig 只做启动必须的硬约束；端口占用、数据库权限等留给后续实际初始化返回具体错误。
func validateConfig(cfg AppConfig) error {
	if strings.TrimSpace(cfg.Addr) == "" {
		return fmt.Errorf("config addr cannot be empty")
	}
	if strings.TrimSpace(cfg.DBPath) == "" {
		return fmt.Errorf("config dbPath cannot be empty")
	}
	if strings.TrimSpace(cfg.LogPath) == "" {
		return fmt.Errorf("config logPath cannot be empty")
	}
	if cfg.LogMaxBytes < 1024 {
		return fmt.Errorf("config logMaxBytes must be >= 1024")
	}
	if cfg.MaxMessageBytes < 1024 {
		return fmt.Errorf("config maxMessageBytes must be >= 1024")
	}
	if cfg.MaxUpdateBytes < 1024 {
		return fmt.Errorf("config maxUpdateBytes must be >= 1024")
	}
	if cfg.MaxSnapshotBytes < 1024 {
		return fmt.Errorf("config maxSnapshotBytes must be >= 1024")
	}
	if cfg.RoomMaxStorageBytes < 1024 {
		return fmt.Errorf("config roomMaxStorageBytes must be >= 1024")
	}
	if cfg.AuthTimeout < time.Second {
		return fmt.Errorf("config authTimeout must be >= 1s")
	}
	if cfg.ReadDeadline < time.Second {
		return fmt.Errorf("config readDeadline must be >= 1s")
	}
	if cfg.RoomTTLDays < 1 {
		return fmt.Errorf("config roomTTLDays must be >= 1")
	}
	if cfg.CleanupInterval < time.Minute {
		return fmt.Errorf("config cleanupInterval must be >= 1m")
	}
	if cfg.MaxRoomsPerIPPerMinute < 1 {
		return fmt.Errorf("config maxRoomsPerIPPerMinute must be >= 1")
	}
	if cfg.MaxWSConnPerIPPerMinute < 1 {
		return fmt.Errorf("config maxWSConnPerIPPerMinute must be >= 1")
	}
	// 生产安全约束：监听非 loopback 地址时禁止使用默认/空 secret，避免任何人下载源码即可伪造 roomKey。
	// 本机开发（127.0.0.1 / ::1）或显式豁免标志可跳过。
	// 同时要求 secret 长度 >= 16 字符，防止弱密钥（如示例文件中的占位符）被直接部署到生产。
	trimmedSecret := strings.TrimSpace(cfg.Secret)
	if !cfg.InsecureAllowDefaultSecret && !addrIsLoopback(cfg.Addr) {
		if trimmedSecret == "" || trimmedSecret == devDefaultSecret {
			return fmt.Errorf("config secret must be set to a strong random value when listening on non-loopback addr (got default/empty); set \"secret\" in config.json or pass -insecure via TIMENOTES_INSECURE_DEFAULT_SECRET=1")
		}
		if len(trimmedSecret) < 16 {
			return fmt.Errorf("config secret must be at least 16 characters when listening on non-loopback addr (got %d chars); generate a long random value, e.g. openssl rand -hex 32", len(trimmedSecret))
		}
	}
	return nil
}

// addrIsLoopback 判断 host:port 形式的监听地址是否仅限本机。
// 空地址、"localhost"、127.x.x.x、::1 视为 loopback；0.0.0.0 / 公网 IP 视为非 loopback。
func addrIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.TrimSpace(host)
	if host == "" || strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// splitCSV 用于环境变量白名单解析，自动忽略空段，避免多余逗号影响启动。
func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
