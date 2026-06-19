package main

import (
	"context"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"

	"timenotesserver/internal/server"
	"timenotesserver/internal/storage/sqlite"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	// configureLogging 必须最先执行：
	// 后续初始化数据库、创建路由、启动监听的错误都需要落到同一个日志文件中，
	// 这样用户反馈"无法加入房间""聊天没收到"时，可以先看一次启动日志和连接日志。
	logFile, err := configureLogging(cfg.LogPath, cfg.LogMaxBytes)
	if err != nil {
		log.Fatalf("configure logging: %v", err)
	}
	if logFile != nil {
		// 文件句柄只在进程退出时关闭；运行期间 log 包会持续向 stdout 和文件双写。
		defer logFile.Close()
	}

	// addr/dbPath 等必要参数来自 JSON 配置文件；环境变量只作为部署平台的覆盖方式。
	// 注意：SQLite 只是第一阶段本地/小团队部署方案；业务层依赖 storage.Store，
	// 后续迁移 PostgreSQL 时应替换 storage 实现，而不是改 WebSocket 协议。
	store, err := sqlite.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	app := fiber.New(fiber.Config{
		AppName:     "TimeNotes Collaboration Server",
		ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout: 30 * time.Second,
	})
	// CORS 与 WebSocket Origin 校验共享同一份 allowOrigin 闭包，保证策略一致。
	allowOriginFn := func(origin string) bool {
		return allowOrigin(origin, cfg.CORSOrigins, cfg.AllowLoopbackOrigins)
	}
	// 前端开发端口 9245 会跨源调用 /api/rooms 创建房间，因此需要 CORS。
	// 默认动态放行 loopback 与 Wails WebView origin；LAN/反代部署可用 TIMENOTES_CORS_ORIGINS 追加精确 origin。
	// 生产环境不要依赖默认放行逻辑，应显式配置应用域名，例如：
	// "corsOrigins": ["https://notes.example.com", "timenotes://collab"]
	app.Use(cors.New(cors.Config{
		AllowOriginsFunc: allowOriginFn,
		AllowMethods: []string{
			fiber.MethodGet,
			fiber.MethodPost,
			fiber.MethodOptions,
		},
		AllowHeaders: []string{"Content-Type", "Upgrade", "Connection"},
	}))
	// Hub 持有房间内存状态和所有 WebSocket 连接；HTTP 路由本身保持薄层，
	// 这样 Fiber 可替换、存储可替换，而协作协议只集中在 internal/server。
	hub := server.NewHub(store, cfg.Secret, server.HubOptions{
		MaxMessageBytes:        cfg.MaxMessageBytes,
		MaxUpdateBytes:         cfg.MaxUpdateBytes,
		MaxSnapshotBytes:       cfg.MaxSnapshotBytes,
		RoomMaxStorageBytes:    cfg.RoomMaxStorageBytes,
		AuthTimeout:            cfg.AuthTimeout,
		ReadDeadline:           cfg.ReadDeadline,
		RoomTTLDays:            cfg.RoomTTLDays,
		CleanupInterval:        cfg.CleanupInterval,
		MaxRoomsPerIPPerMinute:  cfg.MaxRoomsPerIPPerMinute,
		MaxWSConnPerIPPerMinute: cfg.MaxWSConnPerIPPerMinute,
		MaxClientsPerRoom:       cfg.MaxClientsPerRoom,
		MaxGlobalRooms:          cfg.MaxGlobalRooms,
		AllowedServerHosts:     cfg.AllowedServerHosts,
		TrustedProxies:         cfg.TrustedProxies,
	})
	hub.RegisterRoutes(app, allowOriginFn)

	// 启动后台房间清理 goroutine：删除过期关闭房间 + 清理限流器缓存。
	hub.StartCleanup(context.Background())

	// 内置 STUN 跟随服务地址监听同一 host:port 的 UDP。
	// 注意反向代理只转发 TCP 443 还不够，公网 P2P 需要把 UDP 443/8787 同样转发到本进程。
	stunServer, err := server.StartSTUNServer(cfg.Addr)
	if err != nil {
		log.Fatalf("start stun server: %v", err)
	}
	defer stunServer.Close()

	log.Printf("TimeNotes collaboration server config=%s addr=%s stun_udp=%s db=%s max_message=%d max_update=%d max_snapshot=%d room_max_storage=%d ttl_days=%d", cfg.ConfigPath, cfg.Addr, cfg.Addr, cfg.DBPath, cfg.MaxMessageBytes, cfg.MaxUpdateBytes, cfg.MaxSnapshotBytes, cfg.RoomMaxStorageBytes, cfg.RoomTTLDays)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("collab shutting down gracefully...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = stunServer.Close()
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Printf("collab shutdown error: %v", err)
		}
	}()

	if err := app.Listen(cfg.Addr); err != nil {
		log.Fatal(err)
	}
	log.Println("collab server stopped")
}

func configureLogging(logPath string, maxBytes int64) (*os.File, error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// 默认把日志写入服务端目录下的 logs/timenotes-collab.log。
	// 部署或排查问题时可以用 TIMENOTES_LOG 指向自定义位置，例如 C:\logs\timenotes-collab.log。
	// 日志内容不能包含 roomKey、聊天正文、Yjs 二进制内容，只记录 room/client/messageId/字节数等元数据。
	if logPath == "" {
		logPath = filepath.Join("logs", "timenotes-collab.log")
	}
	// 日志目录可能不存在，例如首次启动或容器挂载空目录时；这里主动创建，避免启动失败。
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}

	if info, err := os.Stat(logPath); err == nil && info.Size() > maxBytes {
		// 这里选择启动时截断，而不是运行时轮转：实现简单，且不会在调试中产生多个分散日志文件。
		// 真正生产部署建议交给 systemd/journald、Docker logging driver 或 logrotate 做轮转。
		if err := os.Truncate(logPath, 0); err != nil {
			return nil, err
		}
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// 同时写 stdout 和文件：开发时终端能即时看到，问题复盘时也能从磁盘日志追踪。
	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.Printf("TimeNotes collaboration server log file: %s", logPath)
	return file, nil
}

func allowOrigin(origin string, configuredOrigins []string, allowLoopback bool) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		// 浏览器 CORS 预检一定会带 Origin；空 Origin 不属于正常前端调用。
		return false
	}
	// 显式白名单优先，供公网域名、反向代理域名和自定义协议使用。
	for _, item := range configuredOrigins {
		if strings.EqualFold(strings.TrimSpace(item), origin) {
			return true
		}
	}
	if !allowLoopback {
		return false
	}

	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	switch parsed.Scheme {
	case "http", "https", "wails":
	default:
		// 只接受浏览器/WebView 会实际使用的 scheme，避免把任意 origin 误放行。
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "localhost" || host == "wails.localhost" {
		return true
	}
	ip := net.ParseIP(host)
	// 默认只放行 loopback，方便本机开发；LAN 或公网测试必须走 TIMENOTES_CORS_ORIGINS。
	return ip != nil && ip.IsLoopback()
}
