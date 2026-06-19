package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"
	"golang.org/x/time/rate"

	"timenotesserver/internal/protocol"
	"timenotesserver/internal/storage"
)

const defaultMaxMessageBytes = 128 * 1024 * 1024
const joinApprovalTimeout = 60 * time.Second
const clientSendQueueSize = 2048
const reliableQueueTimeout = 5 * time.Second
const defaultReadDeadlineInterval = 30 * time.Second
const defaultAuthTimeout = 15 * time.Second
const maxMessagesPerSecond = 200
const rateBurst = 50
const rateLimitMaxConsecutiveViolations = 3
const autoCompactUpdateThreshold = 200
const defaultMaxUpdateBytes = 8 * 1024 * 1024
const defaultMaxSnapshotBytes = 64 * 1024 * 1024
const defaultRoomMaxStorageBytes = 512 * 1024 * 1024
const devDefaultSecret = "timenotes-dev-secret"

type HubOptions struct {
	MaxMessageBytes    int64
	MaxUpdateBytes     int64
	MaxSnapshotBytes   int64
	RoomMaxStorageBytes int64
	AuthTimeout        time.Duration
	ReadDeadline       time.Duration
	RoomTTLDays        int
	CleanupInterval    time.Duration
	MaxRoomsPerIPPerMinute int
	MaxWSConnPerIPPerMinute int
	AllowedServerHosts []string
	TrustedProxies     []string
	// MaxClientsPerRoom 限制单个房间的最大在线成员数。默认 20，设为 0 表示不限制。
	MaxClientsPerRoom  int
	// MaxGlobalRooms 限制全局同时在线房间数。默认 0 表示不限制。
	MaxGlobalRooms     int
}

type ipLimiter struct {
	limiter  *rate.Limiter
	lastUse  time.Time
}

type Hub struct {
	store              storage.Store
	secret             []byte
	rooms              map[string]*Room
	mu                 sync.Mutex
	maxMessageBytes    int64
	maxUpdateBytes     int64
	maxSnapshotBytes   int64
	roomMaxStorageBytes int64
	authTimeout        time.Duration
	readDeadline       time.Duration
	roomTTLDays        int
	cleanupInterval    time.Duration
	maxRoomsPerIPPerMinute int
	maxWSConnPerIPPerMinute int
	allowedServerHosts []string
	trustedProxies     []string
	startTime          time.Time
	maxClientsPerRoom  int
	maxGlobalRooms     int
	// roomSerializers 确保同一房间内的所有数据库写入串行化，避免 SQLite 并发写入冲突。
	roomSerializers map[string]chan struct{}
	roomSerialMu    sync.Mutex
	// createRoomLimiters 限制单 IP 创建房间频率（HTTP /api/rooms），使用 TTL 回收。
	createRoomLimiters map[string]*ipLimiter
	crlMu              sync.Mutex
	// wsConnLimiters 限制单 IP WebSocket 连接频率，防止连接耗尽和通过 WS 路径批量创建房间。
	wsConnLimiters map[string]*ipLimiter
	wslMu          sync.Mutex
}

type Room struct {
	id      string
	hostID  string
	closed  bool
	clients map[string]*Client
	pending map[string]*PendingJoin
	mu      sync.RWMutex
}

type PendingJoin struct {
	requestID string
	client    *Client
	state     storage.RoomState
	createdAt time.Time
	result    chan joinDecision
}

type joinDecision struct {
	approved bool
	reason   string
}

func (pending *PendingJoin) decide(decision joinDecision) bool {
	select {
	case pending.result <- decision:
		return true
	default:
		return false
	}
}

type Client struct {
	id         string
	roomID     string
	user       protocol.User
	// userMu 保护 user 字段，presence handler 无 room.mu 写入时不会与 peersLocked/joinRoom 读产生数据竞争。
	userMu     sync.RWMutex
	conn       *websocket.Conn
	send       chan protocol.Envelope
	hub        *Hub
	limiter    *rate.Limiter
	msgViolations atomic.Int64
	closeOnce sync.Once
	closed    atomic.Bool
	// JoinedAt 是客户端成功加入房间的时间戳，用于房主迁移选举。
	JoinedAt time.Time
	// ctx 随客户端生命周期取消；当 readLoop 退出时 cancel，goroutine 中的 store 调用可快速退出。
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewHub(store storage.Store, secret string, options ...HubOptions) *Hub {
	if strings.TrimSpace(secret) == "" {
		secret = devDefaultSecret
		log.Println("collab config warning: TIMENOTES_SECRET is not set; using development-only room key secret")
	}
	maxBytes := int64(defaultMaxMessageBytes)
	maxUpdate := int64(defaultMaxUpdateBytes)
	maxSnapshot := int64(defaultMaxSnapshotBytes)
	roomMax := int64(defaultRoomMaxStorageBytes)
	authTO := defaultAuthTimeout
	readDL := defaultReadDeadlineInterval
	ttlDays := 30
	cleanup := time.Hour
	maxRoomsPerIP := 10
	maxWSConnPerIP := 30
	var allowedHosts []string
	var trustedProxies []string
	maxClients := 20
	maxGlobal := 0
	if len(options) > 0 {
		opt := options[0]
		if opt.MaxMessageBytes > 0 {
			maxBytes = opt.MaxMessageBytes
		}
		if opt.MaxUpdateBytes > 0 {
			maxUpdate = opt.MaxUpdateBytes
		}
		if opt.MaxSnapshotBytes > 0 {
			maxSnapshot = opt.MaxSnapshotBytes
		}
		if opt.RoomMaxStorageBytes > 0 {
			roomMax = opt.RoomMaxStorageBytes
		}
		if opt.AuthTimeout > 0 {
			authTO = opt.AuthTimeout
		}
		if opt.ReadDeadline > 0 {
			readDL = opt.ReadDeadline
		}
		if opt.RoomTTLDays > 0 {
			ttlDays = opt.RoomTTLDays
		}
		if opt.CleanupInterval > 0 {
			cleanup = opt.CleanupInterval
		}
		if opt.MaxRoomsPerIPPerMinute > 0 {
			maxRoomsPerIP = opt.MaxRoomsPerIPPerMinute
		}
		if opt.MaxWSConnPerIPPerMinute > 0 {
			maxWSConnPerIP = opt.MaxWSConnPerIPPerMinute
		}
		if opt.MaxClientsPerRoom > 0 {
			maxClients = opt.MaxClientsPerRoom
		}
		if opt.MaxGlobalRooms > 0 {
			maxGlobal = opt.MaxGlobalRooms
		}
		allowedHosts = opt.AllowedServerHosts
		trustedProxies = opt.TrustedProxies
	}
	return &Hub{
		store:                store,
		secret:               []byte(secret),
		rooms:                make(map[string]*Room),
		maxMessageBytes:      maxBytes,
		maxUpdateBytes:       maxUpdate,
		maxSnapshotBytes:     maxSnapshot,
		roomMaxStorageBytes:  roomMax,
		authTimeout:          authTO,
		readDeadline:         readDL,
		roomTTLDays:          ttlDays,
		cleanupInterval:      cleanup,
		maxRoomsPerIPPerMinute: maxRoomsPerIP,
		maxWSConnPerIPPerMinute: maxWSConnPerIP,
		allowedServerHosts:   allowedHosts,
		trustedProxies:       trustedProxies,
		startTime:            time.Now(),
		maxClientsPerRoom:    maxClients,
		maxGlobalRooms:       maxGlobal,
		roomSerializers:      make(map[string]chan struct{}),
		createRoomLimiters:   make(map[string]*ipLimiter),
		wsConnLimiters:       make(map[string]*ipLimiter),
	}
}

// StartCleanup 启动后台 goroutine 定期清理过期房间和限流器缓存。
func (h *Hub) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(h.cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().AddDate(0, 0, -h.roomTTLDays)
				n, err := h.store.DeleteInactiveRooms(ctx, cutoff)
				if err != nil {
					log.Printf("collab cleanup failed err=%v", err)
				} else if n > 0 {
					log.Printf("collab cleanup deleted %d inactive rooms (cutoff=%s)", n, cutoff.Format(time.RFC3339))
				}
				// 清理超过 1 小时未使用的限流器，防止内存随独立 IP 累积泄漏。
				h.cleanupIPLimiters()
			}
		}
	}()
}

// cleanupIPLimiters 移除超过 1 小时未使用的创建房间和 WS 连接限流器。
func (h *Hub) cleanupIPLimiters() {
	cutoff := time.Now().Add(-time.Hour)
	h.crlMu.Lock()
	for ip, lim := range h.createRoomLimiters {
		if lim.lastUse.Before(cutoff) {
			delete(h.createRoomLimiters, ip)
		}
	}
	h.crlMu.Unlock()
	h.wslMu.Lock()
	for ip, lim := range h.wsConnLimiters {
		if lim.lastUse.Before(cutoff) {
			delete(h.wsConnLimiters, ip)
		}
	}
	h.wslMu.Unlock()
}

// Stats 返回当前服务状态快照，供 /stats 端点使用。
func (h *Hub) Stats() fiber.Map {
	h.mu.Lock()
	roomCount := len(h.rooms)
	// 快照房间列表，避免在迭代 map 时发生并发写入导致 panic。
	roomList := make([]*Room, 0, len(h.rooms))
	for _, room := range h.rooms {
		roomList = append(roomList, room)
	}
	h.mu.Unlock()
	clientCount := 0
	for _, room := range roomList {
		room.mu.RLock()
		clientCount += len(room.clients)
		room.mu.RUnlock()
	}
	return fiber.Map{
		"rooms":   roomCount,
		"clients": clientCount,
		"uptime":  time.Since(h.startTime).String(),
	}
}

func (h *Hub) RegisterRoutes(app *fiber.App, allowOrigin func(string) bool) {
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"ok": true, "service": "timenotes-collab"})
	})
	app.Get("/stats", func(c fiber.Ctx) error {
		return c.JSON(h.Stats())
	})
	app.Post("/api/rooms", h.handleCreateRoom)
	app.Use("/ws/collab", func(c fiber.Ctx) error {
		if !websocket.IsWebSocketUpgrade(c) {
			return fiber.ErrUpgradeRequired
		}
		// WebSocket 握手不经过 CORS 中间件，必须显式校验 Origin 防止跨站 WebSocket 劫持。
		// 浏览器发起 ws/wss 连接一定会带 Origin；非浏览器客户端（CLI、桌面应用）允许无 Origin。
		origin := strings.TrimSpace(c.Get("Origin"))
		if origin != "" && allowOrigin != nil && !allowOrigin(origin) {
			log.Printf("collab ws_origin_rejected origin=%s remote=%s", origin, c.IP())
			return fiber.ErrForbidden
		}
		// IP 维度 WebSocket 连接速率限制，防止连接耗尽和通过 WS 路径批量创建房间。
		clientIP := h.clientIP(c)
		if !h.checkWSRateLimit(clientIP) {
			log.Printf("collab ws_rate_limited remote=%s", clientIP)
			return fiber.ErrTooManyRequests
		}
		return c.Next()
	})
	app.Get("/ws/collab", websocket.New(h.handleSocket, websocket.Config{ReadBufferSize: 4096, WriteBufferSize: 4096}))
}

func (h *Hub) handleCreateRoom(c fiber.Ctx) error {
	// IP 维度速率限制，防止数据库放大攻击。
	clientIP := h.clientIP(c)
	if !h.checkCreateRoomRateLimit(clientIP) {
		log.Printf("collab create_room rate_limited remote=%s", clientIP)
		return c.Status(fiber.StatusTooManyRequests).JSON(protocol.ErrorPayload{Code: "rate_limited", Message: "创建房间过于频繁，请稍后再试"})
	}
	var req protocol.CreateRoomRequest
	if len(c.Body()) > 0 {
		if err := c.Bind().Body(&req); err != nil {
			log.Printf("collab create_room invalid_body remote=%s err=%v", clientIP, err)
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_request", Message: "创建房间请求格式无效"})
		}
	}
	// ServerURL 校验：白名单或 host 必须与当前监听地址一致，防止 inviteURL 域注入。
	if req.ServerURL != "" {
		parsed, err := url.Parse(strings.TrimSpace(req.ServerURL))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_server_url", Message: "服务器地址格式无效"})
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_server_url", Message: "服务器地址 scheme 必须是 http 或 https"})
		}
		if !h.isAllowedServerHost(parsed.Host) {
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "server_host_not_allowed", Message: "服务器地址不在白名单中"})
		}
	}
	// AppURL 校验：只允许安全 scheme。
	if req.AppURL != "" {
		appParsed, err := url.Parse(strings.TrimSpace(req.AppURL))
		if err != nil || appParsed.Scheme == "" {
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_app_url", Message: "应用地址格式无效"})
		}
		switch appParsed.Scheme {
		case "http", "https", "timenotes", "wails":
		default:
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_app_url", Message: "应用地址 scheme 不受支持"})
		}
	}
	roomID := protocol.NewID("room")
	roomKey := protocol.NewSecret("rk")
	if err := h.store.EnsureRoom(context.Background(), roomID, h.roomKeyHash(roomID, roomKey)); err != nil {
		log.Printf("collab create_room failed room=%s remote=%s err=%v", roomID, clientIP, err)
		return c.Status(fiber.StatusInternalServerError).JSON(protocol.ErrorPayload{Code: "create_room_failed", Message: "创建协作房间失败"})
	}
	wsURL := ""
	inviteURL := ""
	iceServers := []protocol.ICEServer{}
	if req.ServerURL != "" {
		parsed, _ := url.Parse(strings.TrimSpace(req.ServerURL))
		wsURL = fmt.Sprintf("ws://%s/ws/collab", parsed.Host)
		if parsed.Scheme == "https" {
			wsURL = fmt.Sprintf("wss://%s/ws/collab", parsed.Host)
		}
		stun := buildSTUNURL(req.ServerURL)
		if stun != "" {
			iceServers = append(iceServers, protocol.ICEServer{URLs: []string{stun}})
		}
		if strings.TrimSpace(req.AppURL) != "" {
			inviteURL = fmt.Sprintf("%s#server=%s&roomId=%s&roomKey=%s",
				strings.TrimRight(req.AppURL, "/"),
				url.QueryEscape(req.ServerURL),
				roomID,
				roomKey,
			)
		}
	}
	return c.Status(fiber.StatusCreated).JSON(protocol.CreateRoomResponse{RoomID: roomID, RoomKey: roomKey, WSURL: wsURL, InviteURL: inviteURL, ICEServers: iceServers})
}

func (h *Hub) isAllowedServerHost(host string) bool {
	if len(h.allowedServerHosts) == 0 {
		// 无白名单时，简单校验 host 非空即可。Listener 地址在注册路由前已由 main.go 确定。
		return true
	}
	hostLower := strings.ToLower(host)
	for _, item := range h.allowedServerHosts {
		if strings.EqualFold(strings.TrimSpace(item), hostLower) {
			return true
		}
	}
	return false
}

func (h *Hub) checkCreateRoomRateLimit(ip string) bool {
	return h.checkIPRateLimit(ip, h.maxRoomsPerIPPerMinute, &h.crlMu, h.createRoomLimiters)
}

// checkWSRateLimit 限制单 IP WebSocket 连接频率，防止连接耗尽和通过 WS 路径批量创建房间。
func (h *Hub) checkWSRateLimit(ip string) bool {
	return h.checkIPRateLimit(ip, h.maxWSConnPerIPPerMinute, &h.wslMu, h.wsConnLimiters)
}

func (h *Hub) checkIPRateLimit(ip string, maxPerMinute int, mu *sync.Mutex, limiters map[string]*ipLimiter) bool {
	mu.Lock()
	lim, ok := limiters[ip]
	if !ok {
		lim = &ipLimiter{limiter: rate.NewLimiter(rate.Limit(float64(maxPerMinute)/60.0), maxPerMinute)}
		limiters[ip] = lim
	}
	lim.lastUse = time.Now()
	mu.Unlock()
	return lim.limiter.Allow()
}

// clientIP 提取客户端真实 IP。优先从 X-Forwarded-For 取真实 IP（反向代理场景），
// 但仅当请求来自受信任代理时才信任 XFF 头；否则回退到 TCP 对端地址。
// 从右往左扫描 XFF 列表，跳过受信任代理，取第一个非受信任 IP 作为真实客户端地址，
// 防止攻击者伪造最左值绕过 IP 维度限流。
func (h *Hub) clientIP(c fiber.Ctx) string {
	remoteIP := net.ParseIP(strings.TrimSpace(c.IP()))
	if remoteIP == nil {
		remoteIP = net.IPv4zero
	}
	if h.isTrustedProxy(remoteIP) {
		if xff := strings.TrimSpace(c.Get("X-Forwarded-For")); xff != "" {
			// XFF 是逗号分隔列表 "client, proxy1, ..., last-proxy"。
			// 从右往左扫描，跳过受信任代理，取第一个非受信任 IP。
			parts := strings.Split(xff, ",")
			for i := len(parts) - 1; i >= 0; i-- {
				ipStr := strings.TrimSpace(parts[i])
				if ipStr == "" {
					continue
				}
				ip := net.ParseIP(ipStr)
				if ip == nil {
					continue
				}
				if !h.isTrustedProxy(ip) {
					return ipStr
				}
			}
		}
	}
	ip := remoteIP.String()
	if ip == "" {
		ip = "unknown"
	}
	return ip
}

// isTrustedProxy 检查 IP 是否在受信任代理列表中。
func (h *Hub) isTrustedProxy(ip net.IP) bool {
	if len(h.trustedProxies) == 0 {
		// 默认不信任任何代理；部署在反向代理后必须显式配置 trustedProxies。
		return false
	}
	for _, cidr := range h.trustedProxies {
		_, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func (h *Hub) handleSocket(conn *websocket.Conn) {
	defer conn.Close()
	// 认证阶段限制读大小 64KB，防止未认证客户端发送大帧撑大内存。
	conn.SetReadLimit(65536)
	conn.SetReadDeadline(time.Now().Add(h.authTimeout))
	var authEnv protocol.Envelope
	if err := conn.ReadJSON(&authEnv); err != nil {
		log.Printf("collab auth_read_failed remote=%s err=%v", conn.RemoteAddr(), err)
		return
	}
	if authEnv.Type != protocol.TypeAuth {
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: "auth_required", Message: "首条消息必须是 auth"}))
		return
	}
	authPayload, err := protocol.DecodePayload[protocol.AuthPayload](authEnv)
	if err != nil {
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: "bad_auth", Message: "认证参数无效"}))
		return
	}
	roomID := strings.TrimSpace(authPayload.RoomID)
	roomKey := strings.TrimSpace(authPayload.RoomKey)
	if roomID == "" || roomKey == "" {
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: "bad_auth", Message: "房间 ID 和密钥不能为空"}))
		return
	}
	// 清洗客户端上报的字段，防止超长字符串撑大内存和日志。CleanID/CleanText 已在 protocol/ids.go 定义。
	user := authPayload.User
	user.ID = protocol.CleanID(user.ID, "client")
	user.Name = protocol.CleanText(user.Name, "匿名", 64)
	user.Color = protocol.CleanText(user.Color, "#888888", 16)
	user.PageID = protocol.CleanText(user.PageID, "", 64)
	user.SelectedElementID = protocol.CleanText(user.SelectedElementID, "", 64)
	user.EditingElementID = protocol.CleanText(user.EditingElementID, "", 64)

	expectedHash := h.roomKeyHash(roomID, roomKey)
	if err := h.store.EnsureRoom(context.Background(), roomID, expectedHash); err != nil {
		code := "room_key_invalid"
		if errors.Is(err, storage.ErrRoomClosed) {
			code = "room_closed"
		}
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: code, Message: "房间不可用"}))
		return
	}
	state, err := h.store.LoadRoomState(context.Background(), roomID)
	if err != nil {
		log.Printf("collab load_state_failed room=%s err=%v", roomID, err)
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: "load_failed", Message: "加载房间状态失败"}))
		return
	}
		user.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
		ctx, cancel := context.WithCancel(context.Background())
		client := &Client{
			id:        user.ID,
			roomID:    roomID,
			user:      user,
			conn:      conn,
			send:      make(chan protocol.Envelope, clientSendQueueSize),
			hub:       h,
			limiter:   rate.NewLimiter(rate.Limit(maxMessagesPerSecond), rateBurst),
			JoinedAt:  time.Now(),
			ctx:       ctx,
			cancel:    cancel,
		}
		go client.writeLoop()
		room, joined := h.joinRoom(client, state)
		if !joined {
			return
		}
		// 认证成功后释放读限制，允许后续大消息（snapshot/replay 等）。
		if h.maxMessageBytes > 0 {
			conn.SetReadLimit(h.maxMessageBytes)
		}
		client.readLoop(room)
		h.leaveRoom(room, client)
}

func (h *Hub) roomKeyHash(roomID string, roomKey string) string {
	mac := hmac.New(sha256.New, h.secret)
	mac.Write([]byte(roomID))
	mac.Write([]byte{0})
	mac.Write([]byte(roomKey))
	return hex.EncodeToString(mac.Sum(nil))
}

func (h *Hub) room(roomID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	room := h.rooms[roomID]
	if room == nil {
		if h.maxGlobalRooms > 0 && len(h.rooms) >= h.maxGlobalRooms {
			return nil
		}
		room = &Room{id: roomID, clients: make(map[string]*Client), pending: make(map[string]*PendingJoin)}
		h.rooms[roomID] = room
	}
	return room
}

// roomWriteAcquireTimeout 限制单条 doc_update 写入等待串行锁的最长时间，
// 防止某个慢写入（如大 snapshot）让后续 update 的 goroutine 无限堆积。
const roomWriteAcquireTimeout = 5 * time.Second

// roomSerializer 返回指定房间的写入串行化 channel，保证同一房间内数据库写入顺序。
func (h *Hub) roomSerializer(roomID string) chan struct{} {
	h.roomSerialMu.Lock()
	defer h.roomSerialMu.Unlock()
	ch, ok := h.roomSerializers[roomID]
	if !ok {
		ch = make(chan struct{}, 1)
		h.roomSerializers[roomID] = ch
	}
	return ch
}

// acquireRoomWrite 获取房间写入许可，必须在 defer releaseRoomWrite(writeCh) 前调用。
// 返回 nil 表示获取超时，调用方应跳过本次写入并回压客户端。
func (h *Hub) acquireRoomWrite(roomID string) chan struct{} {
	ch := h.roomSerializer(roomID)
	timer := time.NewTimer(roomWriteAcquireTimeout)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return ch
	case <-timer.C:
		return nil
	}
}

func releaseRoomWrite(ch chan struct{}) {
	if ch != nil {
		<-ch
	}
}

func (h *Hub) joinRoom(client *Client, state storage.RoomState) (*Room, bool) {
	room := h.room(client.roomID)
	if room == nil {
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "服务端房间数已达上限，请稍后重试"}))
		client.shutdown()
		return nil, false
	}
	room.mu.Lock()
	requestedID := client.id
	client.id = room.uniqueClientIDLocked(requestedID)
	u := client.getUser()
	u.ID = client.id
	isHost := room.hostID == ""
	if isHost {
		room.hostID = client.id
		u.Role = "host"
		client.setUser(u)
		peers := room.peersLocked(client.id)
		room.clients[client.id] = client
		client.queue(protocol.NewEnvelope(protocol.TypeAuthOK, "server", client.id, protocol.AuthOKPayload{
			ClientID:           client.id,
			Peers:              peers,
			CompactStateBase64: protocol.EncodeBytes(state.CompactState),
			Updates:            storedUpdatesForProtocol(state.Updates),
			IsHost:             true,
			HostID:             room.hostID,
		}))
		total := len(room.clients)
		room.mu.Unlock()
		if requestedID != client.id {
			log.Printf("collab client_id rewritten room=%s requested=%s assigned=%s", client.roomID, requestedID, client.id)
		}
		log.Printf("collab join room=%s client=%s role=%s host=%s peers_sent=%d notified=0 total=%d updates=%d compact_bytes=%d", client.roomID, client.id, u.Role, room.hostID, len(peers), total, len(state.Updates), len(state.CompactState))
		return room, true
	}
	u.Role = "collaborator"
	client.setUser(u)
	if requestedID != client.id {
		log.Printf("collab client_id rewritten room=%s requested=%s assigned=%s", client.roomID, requestedID, client.id)
	}
	if room.closed {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "房间已关闭或房主已离线"}))
		client.shutdown()
		return room, false
	}
	if h.maxClientsPerRoom > 0 && len(room.clients) >= h.maxClientsPerRoom {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: fmt.Sprintf("房间成员数已达上限 (%d)", h.maxClientsPerRoom)}))
		client.shutdown()
		return room, false
	}
	pending := &PendingJoin{
		requestID: protocol.NewID("join"),
		client:    client,
		state:     state,
		createdAt: time.Now(),
		result:    make(chan joinDecision, 1),
	}
	room.pending[client.id] = pending
	hostClient := room.clients[room.hostID]
	room.mu.Unlock()
	client.queue(protocol.NewEnvelope(protocol.TypeJoinPending, "server", client.id, protocol.JoinPendingPayload{HostID: room.hostID}))
	if hostClient != nil {
		hostClient.queue(protocol.NewEnvelope(protocol.TypeJoinRequest, "server", hostClient.id, protocol.JoinRequestPayload{
			RequestID: pending.requestID,
			User:      client.getUser(),
		}))
	}
	var decision joinDecision
	select {
	case decision = <-pending.result:
	case <-time.After(joinApprovalTimeout):
		decision = joinDecision{approved: false, reason: "审批超时"}
	}
	room.mu.Lock()
	delete(room.pending, client.id)
	if room.closed || (room.hostID != "" && room.clients[room.hostID] == nil) {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "房间已关闭或房主已离线"}))
		client.shutdown()
		return room, false
	}
	if !decision.approved {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: decision.reason}))
		client.shutdown()
		return room, false
	}
	peers := room.peersLocked(client.id)
	room.clients[client.id] = client
	client.queue(protocol.NewEnvelope(protocol.TypeAuthOK, "server", client.id, protocol.AuthOKPayload{
		ClientID:           client.id,
		Peers:              peers,
		CompactStateBase64: protocol.EncodeBytes(pending.state.CompactState),
		Updates:            storedUpdatesForProtocol(pending.state.Updates),
		IsHost:             false,
		HostID:             room.hostID,
	}))
	total := len(room.clients)
	room.mu.Unlock()
	delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerJoined, "server", "", client.getUser()), client.id)
	user := client.getUser()
	log.Printf("collab join_approved room=%s client=%s role=%s host=%s request=%s peers_sent=%d notified=%d total=%d updates=%d compact_bytes=%d", client.roomID, client.id, user.Role, room.hostID, pending.requestID, len(peers), delivered, total, len(pending.state.Updates), len(pending.state.CompactState))
	return room, true
}

func (h *Hub) leaveRoom(room *Room, client *Client) {
	client.shutdown()
	var roomClosed bool
	var roomClosedDelivered int
	room.mu.Lock()
	_, existed := room.clients[client.id]
	if existed {
		delete(room.clients, client.id)
	}
	hostLeft := existed && client.id == room.hostID && !room.closed
	if hostLeft {
		// 尝试房主迁移：若有剩余协作者，选加入最早的升级为 host。
		if migrated := h.tryHostMigrationLocked(room); migrated {
			hostLeft = false
		}
	}
	if hostLeft {
		room.closed = true
		for key, pending := range room.pending {
			pending.decide(joinDecision{approved: false, reason: "房主已退出协作，房间已关闭"})
			delete(room.pending, key)
		}
		closeEnv := protocol.NewEnvelope(protocol.TypeRoomClosed, "server", "", protocol.RoomClosedPayload{Reason: "host_left", HostID: client.id})
		for id, peer := range room.clients {
			if peer.queue(closeEnv) {
				roomClosedDelivered++
			}
			delete(room.clients, id)
			peer.shutdown()
		}
		roomClosed = true
	}
	empty := len(room.clients) == 0
	// 在持锁期间收集 peers 快照，解锁后再广播，避免 broadcast 与 leaveRoom 竞态。
	leaveTargets := make([]*Client, 0)
	if existed && !roomClosed {
		for id, c := range room.clients {
			if id != client.id {
				leaveTargets = append(leaveTargets, c)
			}
		}
	}
	room.mu.Unlock()
	delivered := 0
	if existed && !roomClosed {
		leaveEnv := protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": client.id})
		for _, c := range leaveTargets {
			if c.queue(leaveEnv) {
				delivered++
			}
		}
	}
	log.Printf("collab leave room=%s client=%s closed=%t delivered=%d host_left=%t total=%d", client.roomID, client.id, roomClosed, delivered, hostLeft, len(room.clients))
	if !hostLeft && !empty {
		return
	}
	h.mu.Lock()
	if h.rooms[client.roomID] == room {
		delete(h.rooms, client.roomID)
	}
	h.mu.Unlock()
	// 清理房间写入串行化 channel。
	h.roomSerialMu.Lock()
	delete(h.roomSerializers, client.roomID)
	h.roomSerialMu.Unlock()
	if roomClosed {
		_ = h.store.CloseRoom(context.Background(), client.roomID)
	}
	log.Printf("collab room_gone room=%s host_left=%t empty=%t closed_delivered=%d", client.roomID, hostLeft, empty, roomClosedDelivered)
}

// tryHostMigrationLocked 在锁内尝试将房主角色迁移给加入最早的协作者。
// 返回 true 表示迁移成功，房间不关闭。
func (h *Hub) tryHostMigrationLocked(room *Room) bool {
	if len(room.clients) == 0 {
		return false
	}
	var earliest *Client
	for _, c := range room.clients {
		if earliest == nil || c.JoinedAt.Before(earliest.JoinedAt) {
			earliest = c
		}
	}
	if earliest == nil {
		return false
	}
	oldHostID := room.hostID
	room.hostID = earliest.id
	u := earliest.getUser()
	u.Role = "host"
	earliest.setUser(u)
	hostChangedEnv := protocol.NewEnvelope(protocol.TypeHostChanged, "server", "", protocol.HostChangedPayload{
		NewHostID: earliest.id,
		OldHostID: oldHostID,
	})
	for _, c := range room.clients {
		c.queue(hostChangedEnv)
	}
	log.Printf("collab host_migrated room=%s old=%s new=%s remaining=%d", room.id, oldHostID, earliest.id, len(room.clients))
	return true
}

func (room *Room) peersLocked(excludeID string) []protocol.User {
	peers := make([]protocol.User, 0, len(room.clients))
	for id, client := range room.clients {
		if id == excludeID {
			continue
		}
		peers = append(peers, client.getUser())
	}
	return peers
}

func (room *Room) uniqueClientIDLocked(base string) string {
	if _, exists := room.clients[base]; !exists {
		if _, pending := room.pending[base]; !pending {
			return base
		}
	}
	for i := 0; i < 8; i++ {
		candidate := fmt.Sprintf("%s-%s", base, strings.TrimPrefix(protocol.NewID("c"), "c-")[:8])
		if _, exists := room.clients[candidate]; exists {
			continue
		}
		if _, pending := room.pending[candidate]; pending {
			continue
		}
		return candidate
	}
	return protocol.NewID("client")
}

func (room *Room) broadcast(env protocol.Envelope, excludeID string) int {
	room.mu.RLock()
	targets := make([]*Client, 0, len(room.clients))
	for id, client := range room.clients {
		if id != excludeID {
			targets = append(targets, client)
		}
	}
	room.mu.RUnlock()
	delivered := 0
	for _, client := range targets {
		if client.queue(env) {
			delivered++
		}
	}
	return delivered
}

func (room *Room) sendTo(clientID string, env protocol.Envelope) bool {
	room.mu.RLock()
	client := room.clients[clientID]
	room.mu.RUnlock()
	if client == nil {
		return false
	}
	return client.queue(env)
}

func (client *Client) queue(env protocol.Envelope) bool {
	if client.closed.Load() {
		return false
	}
	select {
	case client.send <- env:
		return true
	default:
	}
	if isTransientEnvelope(env) {
		return false
	}
	timer := time.NewTimer(reliableQueueTimeout)
	defer timer.Stop()
	if client.closed.Load() {
		return false
	}
	select {
	case client.send <- env:
		return true
	case <-timer.C:
		log.Printf("collab drop_blocked_client room=%s client=%s type=%s", client.roomID, client.id, env.Type)
		return false
	}
}

// shutdown 安全关闭客户端的 send channel，所有路径统一入口。
func (client *Client) shutdown() {
	client.closeOnce.Do(func() {
		client.closed.Store(true)
		close(client.send)
	})
}

// setUser 写入 client.user，对 presence handler（无 room.mu）和 joinRoom（持 room.mu）统一加锁，消除数据竞争。
func (client *Client) setUser(u protocol.User) {
	client.userMu.Lock()
	client.user = u
	client.userMu.Unlock()
}

// getUser 返回 client.user 的副本，消除与 presence handler 之间的数据竞争。
func (client *Client) getUser() protocol.User {
	client.userMu.RLock()
	defer client.userMu.RUnlock()
	return client.user
}

func isTransientEnvelope(env protocol.Envelope) bool {
	return env.Type == protocol.TypePresence
}

func (client *Client) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			// writeLoop panic（如底层 websocket 库内部异常）不能让进程崩溃。
			// 关闭 conn 让 readLoop 也收到错误并走正常 leaveRoom 清理路径。
			log.Printf("collab write_panic room=%s client=%s panic=%v", client.roomID, client.id, r)
		}
		if client.conn != nil {
			_ = client.conn.Close()
		}
	}()
	for env := range client.send {
		_ = client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := client.conn.WriteJSON(env); err != nil {
			log.Printf("collab write_failed room=%s client=%s type=%s err=%v", client.roomID, client.id, env.Type, err)
			return
		}
	}
}

func (client *Client) readLoop(room *Room) {
	// recover 防止 mustJSON 等 panic 导致整个进程崩溃。
	defer func() {
		if r := recover(); r != nil {
			log.Printf("collab panic room=%s client=%s panic=%v", client.roomID, client.id, r)
		}
	}()
	for {
		_ = client.conn.SetReadDeadline(time.Now().Add(client.hub.readDeadline))
		var env protocol.Envelope
		if err := client.conn.ReadJSON(&env); err != nil {
			log.Printf("collab read_closed room=%s client=%s err=%v", client.roomID, client.id, err)
			return
		}
		if !client.limiter.Allow() {
			violations := client.msgViolations.Add(1)
			if violations > rateLimitMaxConsecutiveViolations {
				log.Printf("collab rate_limit_disconnect room=%s client=%s violations=%d", client.roomID, client.id, violations)
				return
			}
			continue
		}
		client.msgViolations.Store(0)
		env.From = client.id
		env.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
		client.handleEnvelope(room, env)
	}
}

func (client *Client) handleEnvelope(room *Room, env protocol.Envelope) {
	switch env.Type {
	case protocol.TypeDocUpdate:
		payload, err := protocol.DecodePayload[protocol.DocUpdatePayload](env)
		if err != nil {
			client.sendError("bad_doc_update", "文档更新格式无效")
			return
		}
		update, err := protocol.DecodeBytes(payload.UpdateBase64)
		if err != nil {
			client.sendError("bad_doc_update", "文档更新不是有效 base64")
			return
		}
		if len(update) > int(client.hub.maxUpdateBytes) {
			client.sendError("doc_update_too_large", fmt.Sprintf("文档更新超出大小限制 (%d bytes)", client.hub.maxUpdateBytes))
			log.Printf("collab oversized_update room=%s client=%s bytes=%d limit=%d", client.roomID, client.id, len(update), client.hub.maxUpdateBytes)
			return
		}
		if len(update) > 0 {
			updateID := payload.UpdateID
			go func() {
				// 客户端断开后快速退出，避免 goroutine 泄漏。
				select {
				case <-client.ctx.Done():
					return
				default:
				}
				writeCh := client.hub.acquireRoomWrite(client.roomID)
				if writeCh == nil {
					// 写入串行化锁等待超时，回压客户端触发 Yjs 重同步。
					log.Printf("collab acquire_write_timeout room=%s client=%s bytes=%d", client.roomID, client.id, len(update))
					client.queue(protocol.NewEnvelope(protocol.TypeDocUpdateRejected, "server", client.id, protocol.DocUpdateRejectedPayload{UpdateID: updateID, Reason: "服务端繁忙，请稍后重试"}))
					return
				}
				defer releaseRoomWrite(writeCh)
				dbCtx, dbCancel := context.WithTimeout(client.ctx, 10*time.Second)
				defer dbCancel()
				// 写入前检查房间存储是否超限。
				currentBytes, err := client.hub.store.RoomStorageBytes(dbCtx, client.roomID)
				if err == nil && currentBytes+int64(len(update)) > client.hub.roomMaxStorageBytes {
					log.Printf("collab room_storage_exceeded room=%s current=%d new=%d limit=%d", client.roomID, currentBytes, len(update), client.hub.roomMaxStorageBytes)
					client.queue(protocol.NewEnvelope(protocol.TypeDocUpdateRejected, "server", client.id, protocol.DocUpdateRejectedPayload{UpdateID: updateID, Reason: "房间存储空间已满"}))
					return
				}
				seq, err := client.hub.store.AppendUpdate(dbCtx, client.roomID, update)
				if err != nil {
					log.Printf("collab append_update failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(update), err)
					// 写库失败回压：通知来源客户端，触发 Yjs 自愈重同步。
					client.queue(protocol.NewEnvelope(protocol.TypeDocUpdateRejected, "server", client.id, protocol.DocUpdateRejectedPayload{UpdateID: updateID, Reason: "服务端持久化失败"}))
					// 同时广播 sync_request 让全房间重新拉取 SQLite 状态，防止 CRDT 分叉。
					client.hub.broadcastSyncRequest(room)
					return
				}
				log.Printf("collab doc_update persisted room=%s client=%s seq=%d bytes=%d relay=%t", client.roomID, client.id, seq, len(update), payload.Relay)
				go client.maybeAutoCompact()
			}()
		}
		if payload.Relay {
			delivered := room.broadcast(env, client.id)
			log.Printf("collab doc_update relayed room=%s client=%s delivered=%d", client.roomID, client.id, delivered)
		}
	case protocol.TypeDocSnapshot:
		payload, err := protocol.DecodePayload[protocol.DocSnapshotPayload](env)
		if err != nil {
			client.sendError("bad_doc_snapshot", "文档快照格式无效")
			return
		}
		state, err := protocol.DecodeBytes(payload.StateBase64)
		if err != nil {
			client.sendError("bad_doc_snapshot", "文档快照不是有效 base64")
			return
		}
		if len(state) > int(client.hub.maxSnapshotBytes) {
			client.sendError("doc_snapshot_too_large", fmt.Sprintf("文档快照超出大小限制 (%d bytes)", client.hub.maxSnapshotBytes))
			log.Printf("collab oversized_snapshot room=%s client=%s bytes=%d limit=%d", client.roomID, client.id, len(state), client.hub.maxSnapshotBytes)
			return
		}
		go func() {
			// 客户端断开后快速退出。
			select {
			case <-client.ctx.Done():
				return
			default:
			}
			writeCh := client.hub.acquireRoomWrite(client.roomID)
			if writeCh == nil {
				log.Printf("collab snapshot_acquire_timeout room=%s client=%s bytes=%d", client.roomID, client.id, len(state))
				client.sendError("server_busy", "服务端繁忙，快照保存被跳过")
				return
			}
			defer releaseRoomWrite(writeCh)
			dbCtx, dbCancel := context.WithTimeout(client.ctx, 10*time.Second)
			defer dbCancel()
			// 检查房间存储是否超限，与 doc_update 保持一致。
			currentBytes, err := client.hub.store.RoomStorageBytes(dbCtx, client.roomID)
			if err == nil && currentBytes+int64(len(state)) > client.hub.roomMaxStorageBytes {
				log.Printf("collab snapshot_storage_exceeded room=%s current=%d new=%d limit=%d", client.roomID, currentBytes, len(state), client.hub.roomMaxStorageBytes)
				client.sendError("room_storage_full", "房间存储空间已满，快照保存被拒绝")
				return
			}
			if err := client.hub.store.SaveSnapshot(dbCtx, client.roomID, state, payload.BaseSeq); err != nil {
				log.Printf("collab save_snapshot failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(state), err)
				return
			}
			log.Printf("collab snapshot saved room=%s client=%s bytes=%d base_seq=%d", client.roomID, client.id, len(state), payload.BaseSeq)
		}()
	case protocol.TypePresence:
		payload, err := protocol.DecodePayload[protocol.PresencePayload](env)
		if err != nil {
			client.sendError("bad_presence", "在线状态格式无效")
			return
		}
		payload.User.ID = client.id
		payload.User.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
		// 强制覆盖 Role，防止客户端伪造。
		room.mu.RLock()
		if client.id == room.hostID {
			payload.User.Role = "host"
		} else {
			payload.User.Role = "collaborator"
		}
		room.mu.RUnlock()
		client.setUser(payload.User)
		if raw, err := json.Marshal(payload); err != nil {
			log.Printf("collab marshal_presence_failed room=%s client=%s err=%v", client.roomID, client.id, err)
			return
		} else {
			env.Payload = raw
		}
		room.broadcast(env, client.id)
	case protocol.TypeChat:
		payload, err := protocol.DecodePayload[protocol.ChatPayload](env)
		if err != nil {
			client.sendError("bad_chat", "聊天消息格式无效")
			return
		}
		payload.User.ID = client.id
		// 强制覆盖 Role，防止客户端伪造。
		room.mu.RLock()
		if client.id == room.hostID {
			payload.User.Role = "host"
		} else {
			payload.User.Role = "collaborator"
		}
		room.mu.RUnlock()
		// 清洗聊天文本，防止单条消息撑大广播数据。
		payload.Text = protocol.CleanText(payload.Text, "", 4096)
		if payload.Text == "" {
			return
		}
		if raw, err := json.Marshal(payload); err != nil {
			log.Printf("collab marshal_chat_failed room=%s client=%s err=%v", client.roomID, client.id, err)
			return
		} else {
			env.Payload = raw
		}
		room.broadcast(env, client.id)
		case protocol.TypeSignal:
			signal, err := protocol.DecodePayload[protocol.SignalPayload](env)
			if err != nil || (signal.Kind == "" && signal.SDP == "" && len(signal.Candidate) == 0) {
				client.sendError("bad_signal", "信令格式无效")
				return
			}
			// 信令优先点对点投递（To 非空时），fallback 广播；offer 通常不设置 To。
			if env.To != "" {
				room.sendTo(env.To, env)
			} else {
				room.broadcast(env, client.id)
			}
		case protocol.TypeVoiceSignal:
			// 语音媒体信令：按 To 点对点转发（offer/answer）/ 广播（ICE candidate）。
			if env.To != "" {
				room.sendTo(env.To, env)
			} else {
				room.broadcast(env, client.id)
			}
	case protocol.TypeRelay:
		inner := env.Payload
		var relay protocol.RelayPayload
		if err := json.Unmarshal(inner, &relay); err != nil {
			client.sendError("bad_relay", "中转消息格式无效")
			return
		}
			switch relay.Type {
			case protocol.TypeDocUpdate, protocol.TypeChat, protocol.TypeVoiceData, protocol.TypeVoiceCtrl:
				delivered := room.broadcast(env, client.id)
				log.Printf("collab relay type=%s room=%s client=%s delivered=%d", relay.Type, client.roomID, client.id, delivered)
			case protocol.TypeSignal:
				if env.To != "" {
					room.sendTo(env.To, env)
				} else {
					room.broadcast(env, client.id)
				}
		default:
			// 未知 relay 子类型静默丢弃，不广播，防止客户端注入异常消息。
			client.sendError("unknown_relay_type", "不支持的中转子类型")
		}
	case protocol.TypeJoinDecision:
		// 只有房主可以批准或拒绝待加入连接。
		room.mu.RLock()
		isHost := client.id == room.hostID
		room.mu.RUnlock()
		if !isHost {
			client.sendError("not_host", "仅房主可以审批加入请求")
			return
		}
		payload, err := protocol.DecodePayload[protocol.JoinDecisionPayload](env)
		if err != nil {
			return
		}
		room.mu.RLock()
		var pending *PendingJoin
		for _, p := range room.pending {
			if p.requestID == payload.RequestID {
				pending = p
				break
			}
		}
		room.mu.RUnlock()
		if pending != nil {
			pending.decide(joinDecision{approved: payload.Approved, reason: payload.Reason})
		}
	case protocol.TypePing:
		client.queue(protocol.Envelope{
			Version: protocol.Version,
			Type:    protocol.TypePong,
			ID:      protocol.NewID("msg"),
			From:    "server",
			To:      client.id,
			SentAt:  time.Now().UTC().Format(time.RFC3339Nano),
			Payload: env.Payload,
		})
	case protocol.TypePong:
	case protocol.TypePeerKick:
		// 只有房主可以踢人。
		room.mu.RLock()
		isHost := client.id == room.hostID
		room.mu.RUnlock()
		if !isHost {
			client.sendError("not_host", "仅房主可以踢出成员")
			return
		}
		payload, err := protocol.DecodePayload[protocol.PeerKickPayload](env)
		if err != nil || payload.ClientID == "" {
			return
		}
		room.mu.RLock()
		target := room.clients[payload.ClientID]
		room.mu.RUnlock()
		if target == nil {
			return
		}
		kickedEnv := protocol.NewEnvelope(protocol.TypePeerKicked, "server", payload.ClientID, protocol.PeerKickedPayload{Reason: payload.Reason, By: client.id})
		target.queue(kickedEnv)
		target.shutdown()
		room.mu.Lock()
		delete(room.clients, payload.ClientID)
		room.mu.Unlock()
		delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": payload.ClientID}), "")
		log.Printf("collab kicked room=%s target=%s by=%s reason=%s delivered=%d", client.roomID, payload.ClientID, client.id, payload.Reason, delivered)
		case protocol.TypeVoiceData:
			// voice_data 仅通过 relay 中转到达，直接消息不做处理。
		case protocol.TypeVoiceCtrl:
			// voice_ctrl 仅通过 relay 中转到达，直接消息不做处理。
		default:
			client.sendError("unknown_type", "未知消息类型")
	}
}

// broadcastSyncRequest 向房间内所有客户端广播 sync_request，触发全局重同步。
func (h *Hub) broadcastSyncRequest(room *Room) {
	room.mu.RLock()
	targets := make([]*Client, 0, len(room.clients))
	for _, c := range room.clients {
		targets = append(targets, c)
	}
	room.mu.RUnlock()
	for _, c := range targets {
		c.queue(protocol.NewEnvelope(protocol.TypeSyncRequest, "server", c.id, fiber.Map{"roomId": c.roomID}))
	}
	log.Printf("collab sync_request_broadcast room=%s targets=%d", room.id, len(targets))
}

func (client *Client) maybeAutoCompact() {
	dbCtx, dbCancel := context.WithTimeout(client.ctx, 5*time.Second)
	defer dbCancel()
	count, err := client.hub.store.UpdateCount(dbCtx, client.roomID)
	if err != nil || count < autoCompactUpdateThreshold {
		return
	}
	log.Printf("collab auto_compact triggered room=%s update_count=%d threshold=%d", client.roomID, count, autoCompactUpdateThreshold)
	// 向房主发送 compaction_request，提示客户端生成新 snapshot。
	client.hub.mu.Lock()
	room := client.hub.rooms[client.roomID]
	client.hub.mu.Unlock()
	if room == nil {
		return
	}
	room.mu.RLock()
	hostID := room.hostID
	hostClient := room.clients[hostID]
	room.mu.RUnlock()
	if hostClient != nil {
		hostClient.queue(protocol.NewEnvelope(protocol.TypeCompactionRequest, "server", hostClient.id, protocol.CompactionRequestPayload{
			RoomID:      client.roomID,
			UpdateCount: count,
		}))
		log.Printf("collab compaction_requested room=%s host=%s update_count=%d", client.roomID, hostID, count)
	}
}

func (client *Client) sendError(code string, message string) {
	client.queue(protocol.NewEnvelope(protocol.TypeError, "server", client.id, protocol.ErrorPayload{Code: code, Message: message}))
}

func storedUpdatesForProtocol(updates []storage.RoomUpdate) []protocol.StoredUpdate {
	if len(updates) == 0 {
		return []protocol.StoredUpdate{}
	}
	result := make([]protocol.StoredUpdate, 0, len(updates))
	for _, u := range updates {
		result = append(result, protocol.StoredUpdate{Seq: u.Seq, UpdateBase64: protocol.EncodeBytes(u.Update)})
	}
	return result
}

func buildSTUNURL(addr string) string {
	parsed, err := url.Parse(addr)
	if err != nil {
		return addr
	}
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	host := parsed.Hostname()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "stun:" + host + ":" + port
}