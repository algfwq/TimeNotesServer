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
	Addr                 string   `json:"addr"`
	DBPath               string   `json:"dbPath"`
	LogPath              string   `json:"logPath"`
	LogMaxBytes          int64    `json:"logMaxBytes"`
	Secret               string   `json:"secret"`
	CORSOrigins          []string `json:"corsOrigins"`
	AllowLoopbackOrigins bool     `json:"allowLoopbackOrigins"`
	MaxMessageBytes      int64    `json:"maxMessageBytes"`
	ConfigPath           string   `json:"-"`
}

func loadConfig() (AppConfig, error) {
	cfg := defaultConfig()
	configPath := strings.TrimSpace(os.Getenv("TIMENOTES_CONFIG"))
	if configPath == "" {
		configPath = defaultConfigPath
	}
	cfg.ConfigPath = configPath

	if body, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(body, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", configPath, err)
		}
		cfg.ConfigPath = configPath
	} else if !errors.Is(err, os.ErrNotExist) || strings.TrimSpace(os.Getenv("TIMENOTES_CONFIG")) != "" {
		return cfg, fmt.Errorf("read config %s: %w", configPath, err)
	}

	applyEnvOverrides(&cfg)
	return cfg, validateConfig(cfg)
}

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
