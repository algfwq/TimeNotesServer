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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"

	"timenotesserver/internal/protocol"
	"timenotesserver/internal/storage"
)

const defaultMaxMessageBytes = 64 * 1024 * 1024
const joinApprovalTimeout = 60 * time.Second

// HubOptions 是协作协调器的运行参数，由 JSON 配置文件传入。
type HubOptions struct {
	// MaxMessageBytes 是单条 WebSocket 消息上限。
	// 协同消息里最大的通常是 Yjs snapshot；生产环境应结合反向代理限制和素材同步策略调整。
	MaxMessageBytes int64
}

// Hub 是协作服务的内存协调器。
// SQLite 只保存房间密钥哈希和 Yjs 二进制状态，在线成员、鼠标、聊天都只存在内存和 WebSocket 流里。
type Hub struct {
	store storage.Store
	// secret 是 roomKey HMAC 的服务端密钥，不直接参与 WebSocket 消息传输。
	// 更换 secret 会让旧 roomKey 无法通过校验，因此生产环境要把它作为持久化机密管理。
	secret []byte
	// rooms 只保存当前进程内的在线房间。进程重启后在线成员会丢失，但文档状态仍可从 SQLite 恢复。
	rooms map[string]*Room
	// mu 只保护 rooms 这张房间索引表；具体房间内的成员表由 Room.mu 单独保护，降低锁粒度。
	mu              sync.Mutex
	maxMessageBytes int64
}

// Room 代表一个正在被在线用户使用的协作房间。
// room.clients 的 key 必须是服务端确认后的唯一连接 ID，不能直接信任前端传入的本地用户 ID。
type Room struct {
	id string
	// hostID 是房主连接 ID。第一位加入房间的客户端成为房主；房主离开时房间关闭。
	hostID string
	// closed 表示房间正在关闭或已经关闭，防止并发 leaveRoom 重复广播 room_closed。
	closed bool
	// clients 是当前在线连接表。连接 ID 由服务端 joinRoom 最终确认，避免多个浏览器窗口共用本地 ID 时互相覆盖。
	clients map[string]*Client
	// pending 是已经通过 roomKey 鉴权、但还没有被房主批准的连接。
	// 待审批连接不能收发文档、聊天、presence 或 WebRTC 信令，只有批准后才进入 clients。
	pending map[string]*PendingJoin
	// 房间内广播、定向投递、成员加入/离开都会访问 clients，需要读写锁保护。
	mu sync.RWMutex
}

// PendingJoin 代表一条等待房主审批的连接。
// result 是 1 容量通道，保证房主重复点击同一个审批请求时不会阻塞服务端读循环。
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

// Client 代表一条已经鉴权的 WebSocket 连接。
// user 是最新 presence 快照；send 是单连接写队列，避免多个 goroutine 同时写 WebSocket。
type Client struct {
	id     string
	roomID string
	// user 是最后一次 presence 上报的用户状态，后加入者通过 peers 快照拿到它。
	user protocol.User
	conn *websocket.Conn
	// send 是唯一允许写 WebSocket 的队列。gorilla/fasthttp websocket 都不适合多 goroutine 并发写。
	send chan protocol.Envelope
	hub  *Hub
}

// NewHub 初始化协作协调器。
// TIMENOTES_SECRET/JSON secret 缺失时只允许本地开发使用固定 secret；日志只提示配置问题，不输出任何房间密钥。
func NewHub(store storage.Store, secret string, options ...HubOptions) *Hub {
	if strings.TrimSpace(secret) == "" {
		secret = "timenotes-dev-secret"
		log.Println("collab config warning: TIMENOTES_SECRET is not set; using development-only room key secret")
	}
	maxBytes := int64(defaultMaxMessageBytes)
	if len(options) > 0 && options[0].MaxMessageBytes > 0 {
		maxBytes = options[0].MaxMessageBytes
	}
	return &Hub{store: store, secret: []byte(secret), rooms: make(map[string]*Room), maxMessageBytes: maxBytes}
}

// RegisterRoutes 注册健康检查、房间创建 API 和 WebSocket 协作入口。
func (h *Hub) RegisterRoutes(app *fiber.App) {
	// /healthz 只表示进程和路由可达，不检查数据库写入能力；生产可扩展为深度健康检查。
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"ok": true, "service": "timenotes-collab"})
	})
	// /api/rooms 给“发起联机”使用，返回一次性的 roomId/roomKey 和邀请链接。
	app.Post("/api/rooms", h.handleCreateRoom)
	// Fiber websocket 中间件要求先确认 Upgrade，否则普通 GET 会进入 websocket handler 产生不清晰错误。
	app.Use("/ws/collab", func(c fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	// /ws/collab 是长期连接。客户端首帧必须发 auth，否则服务端立即关闭连接。
	app.Get("/ws/collab", websocket.New(h.handleSocket, websocket.Config{ReadBufferSize: 4096, WriteBufferSize: 4096}))
}

// handleCreateRoom 是“用户甲发起联机”的入口：
// 1. 服务端生成 roomId 和高熵 roomKey；
// 2. SQLite 保存 roomKey 的 HMAC 哈希；
// 3. 响应 WebSocket 地址和邀请链接，邀请链接把敏感 roomKey 放在 URL fragment 中。
func (h *Hub) handleCreateRoom(c fiber.Ctx) error {
	var req protocol.CreateRoomRequest
	if len(c.Body()) > 0 {
		if err := c.Bind().Body(&req); err != nil {
			log.Printf("collab create_room invalid_body remote=%s err=%v", c.IP(), err)
			return c.Status(fiber.StatusBadRequest).JSON(protocol.ErrorPayload{Code: "bad_request", Message: "创建房间请求格式无效"})
		}
	}

	roomID := protocol.NewID("room")
	roomKey := protocol.NewSecret("rk")
	// 只保存 roomKey 的 HMAC，数据库泄漏时攻击者不能直接拿到可加入房间的密钥。
	if err := h.store.EnsureRoom(context.Background(), roomID, h.roomKeyHash(roomID, roomKey)); err != nil {
		log.Printf("collab create_room failed room=%s remote=%s err=%v", roomID, c.IP(), err)
		return c.Status(fiber.StatusInternalServerError).JSON(protocol.ErrorPayload{Code: "create_room_failed", Message: "创建协作房间失败"})
	}

	// ServerURL 用于生成 ws/wss 连接地址；AppURL 用于生成客户端可打开的邀请链接。
	// roomKey 放在 fragment 中，正常 HTTP 请求和反向代理日志不会带 fragment。
	serverBase := normalizeServerBase(req.ServerURL, requestBaseURL(c))
	wsURL := buildWSURL(serverBase)
	inviteURL := buildInviteURL(req.AppURL, serverBase, roomID, roomKey)
	log.Printf("collab create_room ok room=%s remote=%s ws=%s", roomID, c.IP(), wsURL)

	return c.Status(fiber.StatusCreated).JSON(protocol.CreateRoomResponse{
		RoomID:    roomID,
		RoomKey:   roomKey,
		WSURL:     wsURL,
		InviteURL: inviteURL,
	})
}

// handleSocket 处理一条协作 WebSocket 连接。
// 协议要求首帧必须是 auth，之后才允许进入房间并收发协作 envelope。
func (h *Hub) handleSocket(conn *websocket.Conn) {
	// 先设置读大小，避免异常客户端一次性发超大帧拖垮进程内存。
	conn.SetReadLimit(h.maxMessageBytes)
	client, state, err := h.authenticate(conn)
	if err != nil {
		log.Printf("collab auth failed err=%v", err)
		_ = conn.WriteJSON(protocol.NewEnvelope(protocol.TypeError, "server", "", protocol.ErrorPayload{Code: "auth_failed", Message: err.Error()}))
		_ = conn.Close()
		return
	}
	// 写循环要先启动：协作者进入待审批状态时，服务端需要立即发 join_pending；
	// 如果房主拒绝或审批超时，也要通过同一写队列把 join_rejected 发回去。
	go client.writeLoop()

	room, joined := h.joinRoom(client, state)
	if !joined {
		log.Printf("collab ws join_rejected room=%s client=%s user=%q", client.roomID, client.id, client.user.Name)
		return
	}
	// readLoop 返回代表连接已经断开或无法继续读，此时必须从在线房间移除并广播 peer_left。
	defer h.leaveRoom(room, client)

	log.Printf("collab ws connected room=%s client=%s user=%q", client.roomID, client.id, client.user.Name)
	// 读写循环分离：读循环负责解析客户端消息，写循环串行消费 send 队列。
	client.readLoop(room)
}

// authenticate 只做首帧校验、房间密钥校验和历史状态读取。
// 服务端唯一 client id 必须在 joinRoom 中拿房间锁后再确定，避免同一浏览器多窗口 ID 冲突。
func (h *Hub) authenticate(conn *websocket.Conn) (*Client, storage.RoomState, error) {
	// 首帧鉴权给 5 秒期限，避免半开连接长期占用 goroutine。
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var env protocol.Envelope
	if err := conn.ReadJSON(&env); err != nil {
		return nil, storage.RoomState{}, errors.New("协作连接需要先发送鉴权消息")
	}
	_ = conn.SetReadDeadline(time.Time{})
	if env.Type != protocol.TypeAuth {
		return nil, storage.RoomState{}, errors.New("首帧必须是 auth")
	}
	// auth payload 中包含 roomId、roomKey 和本地用户信息；其中 roomKey 只参与 HMAC 校验，不写日志。
	payload, err := protocol.DecodePayload[protocol.AuthPayload](env)
	if err != nil {
		return nil, storage.RoomState{}, errors.New("鉴权消息格式无效")
	}
	roomID := protocol.CleanText(payload.RoomID, "", 128)
	if roomID == "" || strings.TrimSpace(payload.RoomKey) == "" {
		return nil, storage.RoomState{}, errors.New("房间 ID 和密钥不能为空")
	}
	keyHash := h.roomKeyHash(roomID, payload.RoomKey)
	// EnsureRoom 同时支持创建者刚建房后的首次加入，以及后续协作者加入已有房间。
	// 如果 roomId 存在但 keyHash 不一致，就返回 ErrRoomKeyMismatch。
	if err := h.store.EnsureRoom(context.Background(), roomID, keyHash); err != nil {
		if errors.Is(err, storage.ErrRoomKeyMismatch) {
			return nil, storage.RoomState{}, errors.New("房间密钥无效")
		}
		if errors.Is(err, storage.ErrRoomClosed) {
			return nil, storage.RoomState{}, errors.New("房间已关闭，请让房主重新发起联机")
		}
		log.Printf("collab ensure_room failed room=%s err=%v", roomID, err)
		return nil, storage.RoomState{}, errors.New("房间初始化失败")
	}

	user := payload.User
	// 用户名、颜色、ID 都来自客户端，进入内存前必须清洗长度和默认值。
	// 这里不做账号系统校验，第一阶段安全边界是 roomId + roomKey + TIMENOTES_SECRET。
	user.ID = protocol.CleanID(user.ID, "client")
	user.Name = protocol.CleanText(user.Name, "匿名用户", 64)
	user.Color = protocol.CleanText(user.Color, "#2f6fed", 32)
	user.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
	// 连接刚建立时 WebRTC DataChannel 还没完成，所以先标为 relay；前端后续 presence 会更新成 p2p/relay。
	user.Transport = "relay"

	// 后加入者需要先拿到 SQLite 中的 compact_state + 增量队列，再和在线成员建立 P2P。
	state, err := h.store.LoadRoomState(context.Background(), roomID)
	if err != nil {
		log.Printf("collab load_state failed room=%s err=%v", roomID, err)
		return nil, storage.RoomState{}, errors.New("读取房间状态失败")
	}

	client := &Client{id: user.ID, roomID: roomID, user: user, conn: conn, send: make(chan protocol.Envelope, 64), hub: h}
	return client, state, nil
}

// roomKeyHash 用 TIMENOTES_SECRET 对 roomId+roomKey 做 HMAC。
// 数据库只保存哈希，日志也绝不输出 roomKey 原文。
func (h *Hub) roomKeyHash(roomID string, roomKey string) string {
	mac := hmac.New(sha256.New, h.secret)
	mac.Write([]byte(roomID))
	mac.Write([]byte{0})
	mac.Write([]byte(roomKey))
	return hex.EncodeToString(mac.Sum(nil))
}

// room 获取或创建内存房间。SQLite 房间存在性由 EnsureRoom 保证。
func (h *Hub) room(roomID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()
	room := h.rooms[roomID]
	if room == nil {
		room = &Room{id: roomID, clients: make(map[string]*Client), pending: make(map[string]*PendingJoin)}
		h.rooms[roomID] = room
	}
	return room
}

// joinRoom 在房间锁内完成唯一连接 ID 分配、房主审批、在线成员快照和成员表写入。
// 第一位进入房间的连接直接成为房主；后续协作者必须先进入 pending，房主批准后才能进入 clients。
func (h *Hub) joinRoom(client *Client, state storage.RoomState) (*Room, bool) {
	room := h.room(client.roomID)

	room.mu.Lock()
	requestedID := client.id
	// 必须在锁内分配唯一 ID，否则两个连接同时加入可能得到同一个 ID。
	client.id = room.uniqueClientIDLocked(requestedID)
	client.user.ID = client.id
	isHost := room.hostID == ""
	if isHost {
		room.hostID = client.id
		client.user.Role = "host"
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
		log.Printf("collab join room=%s client=%s role=%s host=%s peers_sent=%d notified=0 total=%d updates=%d compact_bytes=%d", client.roomID, client.id, client.user.Role, room.hostID, len(peers), total, len(state.Updates), len(state.CompactState))
		return room, true
	}

	if requestedID != client.id {
		log.Printf("collab client_id rewritten room=%s requested=%s assigned=%s", client.roomID, requestedID, client.id)
	}
	client.user.Role = "collaborator"
	pending := &PendingJoin{
		requestID: protocol.NewID("join"),
		client:    client,
		state:     state,
		createdAt: time.Now(),
		result:    make(chan joinDecision, 1),
	}
	room.pending[client.id] = pending
	host := room.clients[room.hostID]
	if room.closed || host == nil {
		delete(room.pending, client.id)
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "房间已关闭或房主已离线"}))
		close(client.send)
		return room, false
	}
	client.queue(protocol.NewEnvelope(protocol.TypeJoinPending, "server", client.id, protocol.JoinPendingPayload{HostID: room.hostID}))
	host.queue(protocol.NewEnvelope(protocol.TypeJoinRequest, "server", room.hostID, protocol.JoinRequestPayload{RequestID: pending.requestID, User: client.user}))
	room.mu.Unlock()

	log.Printf("collab join_pending room=%s client=%s host=%s request=%s", client.roomID, client.id, room.hostID, pending.requestID)

	decision := joinDecision{approved: false, reason: "房主审批超时"}
	select {
	case decision = <-pending.result:
	case <-time.After(joinApprovalTimeout):
	}

	room.mu.Lock()
	current := room.pending[client.id]
	if current != pending {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "加入请求已失效"}))
		close(client.send)
		return room, false
	}
	delete(room.pending, client.id)
	if !decision.approved {
		room.mu.Unlock()
		reason := protocol.CleanText(decision.reason, "房主已拒绝加入", 120)
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: reason}))
		close(client.send)
		log.Printf("collab join_denied room=%s client=%s request=%s reason=%q", client.roomID, client.id, pending.requestID, reason)
		return room, false
	}
	if room.closed || room.clients[room.hostID] == nil {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "房间已关闭或房主已离线"}))
		close(client.send)
		return room, false
	}
	// peers 快照要在把自己插入 clients 之前取，避免 auth_ok 里包含自己。
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

	// peer_joined 只在审批通过后广播，确保其他客户端不会提前与未获准连接建立 P2P 或显示在线。
	delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerJoined, "server", "", client.user), client.id)
	log.Printf("collab join_approved room=%s client=%s role=%s host=%s request=%s peers_sent=%d notified=%d total=%d updates=%d compact_bytes=%d", client.roomID, client.id, client.user.Role, room.hostID, pending.requestID, len(peers), delivered, total, len(pending.state.Updates), len(pending.state.CompactState))
	return room, true
}

// leaveRoom 移除在线连接，并通知其他成员该用户离线。
func (h *Hub) leaveRoom(room *Room, client *Client) {
	var roomClosed bool
	var roomClosedDelivered int
	room.mu.Lock()
	_, existed := room.clients[client.id]
	if existed {
		delete(room.clients, client.id)
		// 关闭 send 会让 writeLoop 自然退出；只在持锁确认还存在时关闭，避免重复 close panic。
		close(client.send)
	}
	hostLeft := existed && client.id == room.hostID && !room.closed
	if hostLeft {
		room.closed = true
		// 待审批连接还没有进入 clients，不能收到 room_closed 广播；
		// 这里直接唤醒它们的审批等待，让 joinRoom 立刻返回拒绝而不是等 60 秒超时。
		for _, pending := range room.pending {
			pending.decide(joinDecision{approved: false, reason: "房主已退出协作，房间已关闭"})
		}
		closeEnv := protocol.NewEnvelope(protocol.TypeRoomClosed, "server", "", protocol.RoomClosedPayload{Reason: "host_left", HostID: client.id})
		for id, peer := range room.clients {
			if peer.queue(closeEnv) {
				roomClosedDelivered++
			}
			delete(room.clients, id)
			close(peer.send)
		}
		roomClosed = true
	}
	empty := len(room.clients) == 0
	room.mu.Unlock()

	delivered := 0
	if existed && !roomClosed {
		delivered = room.broadcast(protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": client.id}), client.id)
	}
	if roomClosed {
		if err := h.store.CloseRoom(context.Background(), room.id); err != nil {
			log.Printf("collab close_room persist_failed room=%s host=%s err=%v", room.id, client.id, err)
		}
		log.Printf("collab room_closed room=%s host=%s notified=%d", room.id, client.id, roomClosedDelivered)
	}
	log.Printf("collab leave room=%s client=%s existed=%t notified=%d empty=%t closed=%t", room.id, client.id, existed, delivered, empty, roomClosed)

	if empty || roomClosed {
		// 在线房间为空时清掉内存 Room，避免长时间运行后 rooms 表只增不减。
		// 持久文档状态仍在 SQLite 中，下一次加入会重新创建 Room。
		h.mu.Lock()
		delete(h.rooms, room.id)
		h.mu.Unlock()
	}
}

// roomPeers 返回除 excludeID 以外的在线成员快照。
func (h *Hub) roomPeers(roomID string, excludeID string) []protocol.User {
	room := h.room(roomID)
	room.mu.RLock()
	defer room.mu.RUnlock()
	return room.peersLocked(excludeID)
}

// peersLocked 要求调用方已经持有 room.mu。
func (room *Room) peersLocked(excludeID string) []protocol.User {
	peers := make([]protocol.User, 0, len(room.clients))
	for id, client := range room.clients {
		if id == excludeID {
			continue
		}
		// 返回的是 presence 快照副本；调用方不会直接修改 Client.user。
		peers = append(peers, client.user)
	}
	return peers
}

// uniqueClientIDLocked 为连接分配房间内唯一 ID。
// 前端传来的 ID 只是“期望值”，发生冲突时追加随机后缀，避免覆盖旧连接。
func (room *Room) uniqueClientIDLocked(requestedID string) string {
	base := protocol.CleanID(requestedID, "client")
	if _, exists := room.clients[base]; !exists {
		if _, pending := room.pending[base]; !pending {
			return base
		}
	}
	// 冲突通常来自同一客户端多窗口共享 session/local user id，追加随机后缀即可保持在线列表稳定。
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

// broadcast 向房间内除 excludeID 外的所有成员投递消息，返回成功入队数量。
func (room *Room) broadcast(env protocol.Envelope, excludeID string) int {
	room.mu.RLock()
	defer room.mu.RUnlock()
	delivered := 0
	for id, client := range room.clients {
		if id == excludeID {
			continue
		}
		// queue 可能因为慢客户端满队列而失败；失败不影响其他成员，日志里会记录 drop_slow_client。
		if client.queue(env) {
			delivered++
		}
	}
	return delivered
}

// sendTo 定向投递信令或 relay 消息，返回目标是否存在且成功入队。
func (room *Room) sendTo(clientID string, env protocol.Envelope) bool {
	room.mu.RLock()
	client := room.clients[clientID]
	room.mu.RUnlock()
	if client == nil {
		return false
	}
	return client.queue(env)
}

// queue 把消息放入单连接写队列；慢客户端队列满时丢弃并记录日志，避免阻塞整个房间。
func (client *Client) queue(env protocol.Envelope) bool {
	select {
	case client.send <- env:
		return true
	default:
		// 不在这里阻塞等待，否则一个卡住的 WebSocket 会让整个房间广播链路变慢。
		log.Printf("collab drop_slow_client room=%s client=%s type=%s", client.roomID, client.id, env.Type)
		return false
	}
}

// writeLoop 串行写 WebSocket，所有外部发送都必须先进入 client.send。
func (client *Client) writeLoop() {
	defer func() {
		if client.conn != nil {
			_ = client.conn.Close()
		}
	}()
	for env := range client.send {
		// 写超时防止底层 TCP 连接异常时 goroutine 永久卡在 WriteJSON。
		_ = client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := client.conn.WriteJSON(env); err != nil {
			log.Printf("collab write_failed room=%s client=%s type=%s err=%v", client.roomID, client.id, env.Type, err)
			return
		}
	}
}

// readLoop 持续读取客户端 envelope，并统一补齐 from/sentAt。
func (client *Client) readLoop(room *Room) {
	for {
		var env protocol.Envelope
		if err := client.conn.ReadJSON(&env); err != nil {
			log.Printf("collab read_closed room=%s client=%s err=%v", client.roomID, client.id, err)
			return
		}
		// from/sentAt 由服务端覆盖，不能信任客户端伪造的发送者或时间戳。
		env.From = client.id
		env.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
		client.handleEnvelope(room, env)
	}
}

// handleEnvelope 是协作协议的主分发点。
// 文档 update/snapshot 走 SQLite 持久化；presence/chat/signal/relay 只在线转发，不落库。
func (client *Client) handleEnvelope(room *Room, env protocol.Envelope) {
	switch env.Type {
	case protocol.TypeDocUpdate:
		// doc_update 是 Yjs 二进制增量。无论是否需要 relay，都先落 SQLite，保证后加入者能恢复状态。
		// P2P 正常时客户端之间会通过 DataChannel 互传 update；但发起方仍会把 update 发给服务端做持久化。
		// P2P 不可用时 payload.Relay=true，服务端除持久化外还会把同一 envelope 广播给其他成员。
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
		if len(update) > 0 {
			// update 允许为空是为了兼容客户端的控制帧；真正需要恢复状态的二进制增量才写入 room_updates。
			seq, err := client.hub.store.AppendUpdate(context.Background(), client.roomID, update)
			if err != nil {
				log.Printf("collab append_update failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(update), err)
				client.sendError("persist_failed", "文档更新保存失败")
				return
			}
			log.Printf("collab doc_update persisted room=%s client=%s seq=%d bytes=%d relay=%t", client.roomID, client.id, seq, len(update), payload.Relay)
		}
		if payload.Relay {
			// relay 只转发给其他客户端，不回发给发送者，避免客户端应用自己的 update 两次。
			delivered := room.broadcast(env, client.id)
			log.Printf("collab doc_update relayed room=%s client=%s delivered=%d", client.roomID, client.id, delivered)
		}
	case protocol.TypeDocSnapshot:
		// doc_snapshot 是压缩后的 Yjs 全量状态；保存后会清空历史增量，降低 SQLite 重放成本。
		// 客户端应在合适时机生成 snapshot，避免 room_updates 无限制增长。
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
		if err := client.hub.store.SaveSnapshot(context.Background(), client.roomID, state); err != nil {
			log.Printf("collab save_snapshot failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(state), err)
			client.sendError("persist_failed", "文档快照保存失败")
			return
		}
		log.Printf("collab snapshot saved room=%s client=%s bytes=%d", client.roomID, client.id, len(state))
	case protocol.TypePresence:
		// presence 是在线状态：鼠标位置、当前页面、选中/编辑元素和传输状态。它只广播，不持久化。
		// 这类消息频率高，必须保持轻量；服务端只清洗用户 ID 和 lastSeen，不做数据库写入。
		payload, err := protocol.DecodePayload[protocol.PresencePayload](env)
		if err != nil {
			client.sendError("bad_presence", "在线状态格式无效")
			return
		}
		payload.User.ID = client.id
		payload.User.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
		payload.User.Role = client.user.Role
		// 保存最新 presence，后加入者 auth_ok.peers 会用它显示在线成员和所在页面。
		client.user = payload.User
		env.Payload = mustJSON(payload)
		delivered := 0
		if payload.Relay {
			// P2P presence 正常时不需要服务器广播；只有强制中转或 DataChannel 不可用时才走这里。
			delivered = room.broadcast(env, client.id)
			log.Printf("collab presence room=%s client=%s page=%s transport=%s delivered=%d", client.roomID, client.id, payload.User.PageID, payload.User.Transport, delivered)
		}
	case protocol.TypeSignal:
		// signal 只用于 WebRTC 建连：offer/answer/ICE candidate。服务端不解析 SDP，不参与媒体或数据内容。
		// 生产排障时只看 from/to/delivered，不记录 SDP 原文，避免日志过大并减少隐私泄露面。
		if env.To == "" {
			client.sendError("bad_signal", "信令缺少目标用户")
			return
		}
		ok := room.sendTo(env.To, env)
		log.Printf("collab signal room=%s from=%s to=%s delivered=%t", client.roomID, client.id, env.To, ok)
	case protocol.TypeRelay:
		// relay 是 P2P 不可用时的兜底中转，复用同一 envelope payload。
		// env.To 为空表示房间广播；有 To 时只给指定目标，常用于点对点补发。
		delivered := 0
		if env.To != "" {
			if room.sendTo(env.To, env) {
				delivered = 1
			}
		} else {
			delivered = room.broadcast(env, client.id)
		}
		log.Printf("collab relay room=%s client=%s to=%s delivered=%d", client.roomID, client.id, env.To, delivered)
	case protocol.TypeChat:
		// chat 只在线广播，不落库；日志不记录消息正文，避免把用户聊天内容写进服务端日志。
		// 聊天优先 P2P，fallback 时 payload.Relay=true 才会由服务端广播。
		payload, err := protocol.DecodePayload[protocol.ChatPayload](env)
		if err != nil {
			client.sendError("bad_chat", "聊天消息格式无效")
			return
		}
		payload.User = client.user
		if payload.MessageID == "" {
			// 由服务端补 messageId，方便前端去重和日志关联，同时不暴露消息正文。
			payload.MessageID = protocol.NewID("chat")
		}
		env.Payload = mustJSON(payload)
		delivered := 0
		if payload.Relay {
			delivered = room.broadcast(env, client.id)
		}
		log.Printf("collab chat room=%s client=%s message=%s relay=%t delivered=%d", client.roomID, client.id, payload.MessageID, payload.Relay, delivered)
	case protocol.TypeJoinDecision:
		client.handleJoinDecision(room, env)
	case protocol.TypePeerKick:
		client.handlePeerKick(room, env)
	case protocol.TypePing:
		// ping/pong 只用于客户端测量 WebSocket 往返延迟，不访问 SQLite，也不广播给其他成员。
		payload, err := protocol.DecodePayload[protocol.PingPayload](env)
		if err != nil || payload.PingID == "" {
			client.sendError("bad_ping", "延迟探测格式无效")
			return
		}
		client.queue(protocol.NewEnvelope(protocol.TypePong, "server", client.id, protocol.PongPayload{PingID: payload.PingID}))
	case protocol.TypeSyncRequest:
		// sync_request 允许客户端在发现状态异常时主动重新拉取 SQLite 中的房间状态。
		state, err := client.hub.store.LoadRoomState(context.Background(), client.roomID)
		if err != nil {
			log.Printf("collab sync_failed room=%s client=%s err=%v", client.roomID, client.id, err)
			client.sendError("sync_failed", "读取房间状态失败")
			return
		}
		client.queue(protocol.NewEnvelope(protocol.TypeAuthOK, "server", client.id, protocol.AuthOKPayload{
			ClientID:           client.id,
			Peers:              client.hub.roomPeers(client.roomID, client.id),
			CompactStateBase64: protocol.EncodeBytes(state.CompactState),
			Updates:            storedUpdatesForProtocol(state.Updates),
			IsHost:             client.user.Role == "host",
			HostID:             room.hostID,
		}))
		log.Printf("collab sync_ok room=%s client=%s updates=%d compact_bytes=%d", client.roomID, client.id, len(state.Updates), len(state.CompactState))
	default:
		// 未知类型不要静默丢弃，前端调试时需要明确知道协议版本或消息名不一致。
		log.Printf("collab unknown_type room=%s client=%s type=%s", client.roomID, client.id, env.Type)
		client.sendError("unknown_type", "未知协作消息类型")
	}
}

// handleJoinDecision 只允许房主调用。
// 服务端不信任客户端传来的 from/to，审批目标必须从 pending 表中查到才会生效。
func (client *Client) handleJoinDecision(room *Room, env protocol.Envelope) {
	if client.id != room.hostID {
		client.sendError("forbidden", "只有房主可以审批协作者加入")
		return
	}
	payload, err := protocol.DecodePayload[protocol.JoinDecisionPayload](env)
	if err != nil {
		client.sendError("bad_join_decision", "加入审批格式无效")
		return
	}
	requestID := protocol.CleanText(payload.RequestID, "", 128)
	targetID := protocol.CleanID(payload.ClientID, "")
	room.mu.RLock()
	var pending *PendingJoin
	if targetID != "" {
		pending = room.pending[targetID]
	}
	if pending == nil && requestID != "" {
		for _, candidate := range room.pending {
			if candidate.requestID == requestID {
				pending = candidate
				break
			}
		}
	}
	room.mu.RUnlock()
	if pending == nil {
		client.sendError("join_request_expired", "加入请求已失效")
		return
	}
	reason := protocol.CleanText(payload.Reason, "", 120)
	if reason == "" && !payload.Approved {
		reason = "房主已拒绝加入"
	}
	ok := pending.decide(joinDecision{approved: payload.Approved, reason: reason})
	log.Printf("collab join_decision room=%s host=%s target=%s request=%s approved=%t delivered=%t", client.roomID, client.id, pending.client.id, pending.requestID, payload.Approved, ok)
}

// handlePeerKick 只允许房主踢出已经在线的协作者。
// 被踢者会先收到 peer_kicked，再由服务端关闭其 WebSocket；其他成员收到 peer_left 刷新在线列表。
func (client *Client) handlePeerKick(room *Room, env protocol.Envelope) {
	if client.id != room.hostID {
		client.sendError("forbidden", "只有房主可以踢出协作者")
		return
	}
	payload, err := protocol.DecodePayload[protocol.PeerKickPayload](env)
	if err != nil {
		client.sendError("bad_peer_kick", "踢出协作者请求格式无效")
		return
	}
	targetID := protocol.CleanID(payload.ClientID, "")
	if targetID == "" || targetID == client.id {
		client.sendError("bad_peer_kick", "不能踢出该成员")
		return
	}
	reason := protocol.CleanText(payload.Reason, "房主已将你移出协作房间", 120)

	room.mu.Lock()
	target := room.clients[targetID]
	if target == nil {
		room.mu.Unlock()
		client.sendError("peer_not_found", "协作者不在线或已离开")
		return
	}
	if target.id == room.hostID {
		room.mu.Unlock()
		client.sendError("bad_peer_kick", "不能踢出房主")
		return
	}
	delete(room.clients, targetID)
	target.queue(protocol.NewEnvelope(protocol.TypePeerKicked, "server", targetID, protocol.PeerKickedPayload{Reason: reason, By: client.id}))
	close(target.send)
	total := len(room.clients)
	room.mu.Unlock()

	delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": targetID}), targetID)
	log.Printf("collab peer_kicked room=%s host=%s target=%s notified=%d total=%d", client.roomID, client.id, targetID, delivered, total)
}

// sendError 发送结构化错误，前端可以直接展示为 toast/status。
func (client *Client) sendError(code string, message string) {
	client.queue(protocol.NewEnvelope(protocol.TypeError, "server", client.id, protocol.ErrorPayload{Code: code, Message: message}))
}

func storedUpdatesForProtocol(updates []storage.RoomUpdate) []protocol.StoredUpdate {
	// 协议层统一用 base64 传二进制，避免 JSON/WebSocket 中出现不可见字节。
	result := make([]protocol.StoredUpdate, 0, len(updates))
	for _, update := range updates {
		result = append(result, protocol.StoredUpdate{Seq: update.Seq, UpdateBase64: protocol.EncodeBytes(update.Update)})
	}
	return result
}

func mustJSON(value any) json.RawMessage {
	// 这里的 payload 结构来自服务端已知类型，Marshal 失败只可能是编程错误；
	// 为了保持主路径简洁，失败时返回空 RawMessage，由测试覆盖协议结构。
	raw, _ := json.Marshal(value)
	return raw
}

func requestBaseURL(c fiber.Ctx) string {
	// requestBaseURL 从当前 HTTP 请求反推出服务器外部地址，
	// 用于客户端没有显式传 serverUrl 时生成邀请链接和 WebSocket URL。
	scheme := c.Protocol()
	if scheme == "" || strings.HasPrefix(strings.ToUpper(scheme), "HTTP/") {
		scheme = "http"
	}
	host := c.Host()
	if host == "" {
		host = "127.0.0.1:8787"
	}
	return scheme + "://" + host
}

func normalizeServerBase(raw string, fallback string) string {
	// 支持用户在前端输入 1.2.3.4:8787、http://host、ws://host、wss://host 等形式。
	// 内部统一成 http/https base URL，再交给 buildWSURL 转换成 ws/wss。
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = fallback
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return strings.TrimRight(fallback, "/")
	}
	if parsed.Scheme == "ws" {
		parsed.Scheme = "http"
	}
	if parsed.Scheme == "wss" {
		parsed.Scheme = "https"
	}
	// 去掉路径、查询和 fragment，避免用户输入的邀请链接残留参数污染 WebSocket 地址。
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func buildWSURL(serverBase string) string {
	// HTTPS 页面必须连接 WSS，否则浏览器会因为 mixed content 阻止 WebSocket。
	parsed, err := url.Parse(serverBase)
	if err != nil || parsed.Host == "" {
		return "ws://127.0.0.1:8787/ws/collab"
	}
	if parsed.Scheme == "https" || parsed.Scheme == "wss" {
		parsed.Scheme = "wss"
	} else {
		parsed.Scheme = "ws"
	}
	parsed.Path = "/ws/collab"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func buildInviteURL(appURL string, serverBase string, roomID string, roomKey string) string {
	// appURL 指向 TimeNotes 客户端入口，默认使用 timenotes://collab。
	// Web 调试时也可以传 http://127.0.0.1:9245/，客户端从 hash 中读取加入参数。
	appURL = strings.TrimSpace(appURL)
	if appURL == "" {
		appURL = "timenotes://collab"
	}
	if !strings.Contains(appURL, "://") {
		appURL = "http://" + appURL
	}
	parsed, err := url.Parse(appURL)
	if err != nil {
		parsed = &url.URL{Scheme: "timenotes", Host: "collab"}
	}
	query := parsed.Query()
	// query 中只放非敏感标记，方便客户端路由判断这是一次加入协作邀请。
	query.Set("collab", "join")
	parsed.RawQuery = query.Encode()
	parsed.Fragment = ""
	parsed.RawFragment = ""

	// 敏感 roomKey 放 fragment。浏览器不会把 fragment 发送给 HTTP 服务端，
	// 这样反向代理访问日志、服务端请求日志都不会自然记录 roomKey。
	fragment := url.Values{}
	fragment.Set("server", serverBase)
	fragment.Set("roomId", roomID)
	fragment.Set("roomKey", roomKey)
	base := parsed.String()
	return base + "#" + fragment.Encode()
}
