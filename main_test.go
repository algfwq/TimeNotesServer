package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAllowOrigin(t *testing.T) {
	configured := []string{"http://192.168.1.10:9245"}
	cases := map[string]bool{
		"http://127.0.0.1:9245":    true,
		"http://localhost:5173":    true,
		"wails://wails.localhost":  true,
		"http://wails.localhost":   true,
		"http://192.168.1.10:9245": true,
		"http://192.168.1.11:9245": false,
		"file://":                  false,
	}
	for origin, want := range cases {
		if got := allowOrigin(origin, configured, true); got != want {
			t.Fatalf("allowOrigin(%q)=%t, want %t", origin, got, want)
		}
	}
}

func TestAllowOriginWithoutLoopback(t *testing.T) {
	if got := allowOrigin("http://127.0.0.1:9245", nil, false); got {
		t.Fatalf("loopback origin should be rejected when allowLoopbackOrigins=false")
	}
	if got := allowOrigin("https://notes.example.com", []string{"https://notes.example.com"}, false); !got {
		t.Fatalf("configured production origin should be accepted")
	}
}

func TestLoadConfigFromJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	body := []byte(`{
  "addr": "0.0.0.0:9000",
  "dbPath": "data/test.db",
  "logPath": "logs/test.log",
  "logMaxBytes": 2048,
  "secret": "json-secret",
  "corsOrigins": ["https://notes.example.com"],
  "allowLoopbackOrigins": false,
  "maxMessageBytes": 4096
}`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TIMENOTES_CONFIG", path)
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Addr != "0.0.0.0:9000" || cfg.Secret != "json-secret" || cfg.AllowLoopbackOrigins {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if len(cfg.CORSOrigins) != 1 || cfg.CORSOrigins[0] != "https://notes.example.com" {
		t.Fatalf("unexpected cors origins: %+v", cfg.CORSOrigins)
	}
}
