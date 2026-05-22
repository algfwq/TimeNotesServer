package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	// ConfigPath 不写入 JSON，只用于启动日志展示实际加载了哪个配置文件。
	ConfigPath string `json:"-"`
}

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
func defaultConfig() AppConfig {
	return AppConfig{
		Addr:                 "127.0.0.1:8787",
		DBPath:               filepath.Join("data", "timenotes-collab.db"),
		LogPath:              filepath.Join("logs", "timenotes-collab.log"),
		LogMaxBytes:          5 * 1024 * 1024,
		CORSOrigins:          []string{},
		AllowLoopbackOrigins: true,
		MaxMessageBytes:      64 * 1024 * 1024,
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
	return nil
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
