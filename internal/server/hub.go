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
	"sync/atomic"
	"time"

	"github.com/gofiber/contrib/v3/websocket"
	"github.com/gofiber/fiber/v3"

	"timenotesserver/internal/protocol"
	"timenotesserver/internal/storage"
)

const defaultMaxMessageBytes = 64 * 1024 * 1024
const joinApprovalTimeout = 60 * time.Second
const clientSendQueueSize = 2048
const reliableQueueTimeout = 5 * time.Second
const readDeadlineInterval = 30 * time.Second
const maxMessagesPerSecond = 200
const autoCompactUpdateThreshold = 200

type HubOptions struct {
	MaxMessageBytes int64
}

type Hub struct {
	store storage.Store
	secret []byte
	rooms map[string]*Room
	mu              sync.Mutex
	maxMessageBytes int64
}

type Room struct {
	id     string
	hostID string
	closed bool
	clients map[string]*Client
	pending map[string]*PendingJoin
	mu sync.RWMutex
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
	id     string
	roomID string
	user   protocol.User
	conn   *websocket.Conn
	send   chan protocol.Envelope
	hub    *Hub
	msgCount   atomic.Int64
	msgWindow  int64
	closeOnce  sync.Once
}

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

func (h *Hub) RegisterRoutes(app *fiber.App) {
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"ok": true, "service": "timenotes-collab"})
	})
	app.Post("/api/rooms", h.handleCreateRoom)
	app.Use("/ws/collab", func(c fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/ws/collab", websocket.New(h.handleSocket, websocket.Config{ReadBufferSize: 4096, WriteBufferSize: 4096}))
}

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
	if err := h.store.EnsureRoom(context.Background(), roomID, h.roomKeyHash(roomID, roomKey)); err != nil {
		log.Printf("collab create_room failed room=%s remote=%s err=%v", roomID, c.IP(), err)
		return c.Status(fiber.StatusInternalServerError).JSON(protocol.ErrorPayload{Code: "create_room_failed", Message: "创建协作房间失败"})
	}
	wsURL := ""
	inviteURL := ""
	iceServers := []protocol.ICEServer{}
	if req.ServerURL != "" {
		parsed, err := url.Parse(strings.TrimSpace(req.ServerURL))
		if err == nil && parsed.Scheme != "" && parsed.Host != "" {
			wsURL = fmt.Sprintf("ws://%s/ws/collab", parsed.Host)
			if parsed.Scheme == "https" {
				wsURL = fmt.Sprintf("wss://%s/ws/collab", parsed.Host)
			}
			stun := buildSTUNURL(req.ServerURL)
			if stun != "" {
				iceServers = append(iceServers, protocol.ICEServer{URLs: []string{stun}})
			}
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

func (h *Hub) handleSocket(conn *websocket.Conn) {
	defer conn.Close()
	if h.maxMessageBytes > 0 {
		conn.SetReadLimit(h.maxMessageBytes)
	}
	conn.SetReadDeadline(time.Now().Add(reliableQueueTimeout))
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
	user := authPayload.User
	user.ID = ""
	user.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
	client := &Client{
		id:     authPayload.User.ID,
		roomID: roomID,
		user:   user,
		conn:   conn,
		send:   make(chan protocol.Envelope, clientSendQueueSize),
		hub:    h,
	}
	client.msgWindow = time.Now().Unix()
	go client.writeLoop()
	room, joined := h.joinRoom(client, state)
	if !joined {
		return
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
		room = &Room{id: roomID, clients: make(map[string]*Client), pending: make(map[string]*PendingJoin)}
		h.rooms[roomID] = room
	}
	return room
}

func (h *Hub) joinRoom(client *Client, state storage.RoomState) (*Room, bool) {
	room := h.room(client.roomID)
	room.mu.Lock()
	requestedID := client.id
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
	client.user.Role = "collaborator"
	if requestedID != client.id {
		log.Printf("collab client_id rewritten room=%s requested=%s assigned=%s", client.roomID, requestedID, client.id)
	}
	if room.closed {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: "房间已关闭或房主已离线"}))
		close(client.send)
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
			User:      client.user,
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
		close(client.send)
		return room, false
	}
	if !decision.approved {
		room.mu.Unlock()
		client.queue(protocol.NewEnvelope(protocol.TypeJoinRejected, "server", client.id, protocol.JoinRejectedPayload{Reason: decision.reason}))
		close(client.send)
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
	delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerJoined, "server", "", client.user), client.id)
	log.Printf("collab join_approved room=%s client=%s role=%s host=%s request=%s peers_sent=%d notified=%d total=%d updates=%d compact_bytes=%d", client.roomID, client.id, client.user.Role, room.hostID, pending.requestID, len(peers), delivered, total, len(pending.state.Updates), len(pending.state.CompactState))
	return room, true
}

func (h *Hub) leaveRoom(room *Room, client *Client) {
	client.closeOnce.Do(func() {
		close(client.send)
	})
	var roomClosed bool
	var roomClosedDelivered int
	room.mu.Lock()
	_, existed := room.clients[client.id]
	if existed {
		delete(room.clients, client.id)
	}
	hostLeft := existed && client.id == room.hostID && !room.closed
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
			peer.closeOnce.Do(func() {
				close(peer.send)
			})
		}
		roomClosed = true
	}
	empty := len(room.clients) == 0
	room.mu.Unlock()
	delivered := 0
	if existed && !roomClosed {
		delivered = room.broadcast(protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": client.id}), client.id)
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
	if roomClosed {
		_ = h.store.CloseRoom(context.Background(), client.roomID)
	}
	log.Printf("collab room_gone room=%s host_left=%t empty=%t closed_delivered=%d", client.roomID, hostLeft, empty, roomClosedDelivered)
}

func (room *Room) peersLocked(excludeID string) []protocol.User {
	peers := make([]protocol.User, 0, len(room.clients))
	for id, client := range room.clients {
		if id == excludeID {
			continue
		}
		peers = append(peers, client.user)
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
	select {
	case client.send <- env:
		return true
	case <-timer.C:
		log.Printf("collab drop_blocked_client room=%s client=%s type=%s", client.roomID, client.id, env.Type)
		return false
	}
}

func isTransientEnvelope(env protocol.Envelope) bool {
	return env.Type == protocol.TypePresence
}

func (client *Client) checkRateLimit() bool {
	now := time.Now().Unix()
	window := client.msgWindow
	if now != window {
		if client.msgCount.CompareAndSwap(client.msgCount.Load(), 0) {
			client.msgWindow = now
		}
		return true
	}
	count := client.msgCount.Add(1)
	if count > maxMessagesPerSecond {
		return false
	}
	return true
}

func (client *Client) writeLoop() {
	defer func() {
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
	for {
		_ = client.conn.SetReadDeadline(time.Now().Add(readDeadlineInterval))
		var env protocol.Envelope
		if err := client.conn.ReadJSON(&env); err != nil {
			log.Printf("collab read_closed room=%s client=%s err=%v", client.roomID, client.id, err)
			return
		}
		if !client.checkRateLimit() {
			continue
		}
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
		if len(update) > 0 {
			go func() {
				seq, err := client.hub.store.AppendUpdate(context.Background(), client.roomID, update)
				if err != nil {
					log.Printf("collab append_update failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(update), err)
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
		go func() {
			if err := client.hub.store.SaveSnapshot(context.Background(), client.roomID, state); err != nil {
				log.Printf("collab save_snapshot failed room=%s client=%s bytes=%d err=%v", client.roomID, client.id, len(state), err)
				return
			}
			log.Printf("collab snapshot saved room=%s client=%s bytes=%d", client.roomID, client.id, len(state))
		}()
	case protocol.TypePresence:
		payload, err := protocol.DecodePayload[protocol.PresencePayload](env)
		if err != nil {
			client.sendError("bad_presence", "在线状态格式无效")
			return
		}
		payload.User.ID = client.id
		payload.User.LastSeen = time.Now().UTC().Format(time.RFC3339Nano)
		client.user = payload.User
		env.Payload = mustJSON(payload)
		room.broadcast(env, client.id)
	case protocol.TypeChat:
		payload, err := protocol.DecodePayload[protocol.ChatPayload](env)
		if err != nil {
			client.sendError("bad_chat", "聊天消息格式无效")
			return
		}
		payload.User.ID = client.id
		env.Payload = mustJSON(payload)
		room.broadcast(env, client.id)
	case protocol.TypeSignal:
		_, err := protocol.DecodePayload[protocol.SignalPayload](env)
		if err != nil {
			client.sendError("bad_signal", "信令格式无效")
			return
		}
		room.broadcast(env, client.id)
	case protocol.TypeRelay:
		inner := env.Payload
		var relay protocol.RelayPayload
		if err := json.Unmarshal(inner, &relay); err != nil {
			client.sendError("bad_relay", "中转消息格式无效")
			return
		}
		switch relay.Type {
		case protocol.TypeDocUpdate, protocol.TypeChat:
			delivered := room.broadcast(env, client.id)
			log.Printf("collab relay type=%s room=%s client=%s delivered=%d", relay.Type, client.roomID, client.id, delivered)
		case protocol.TypeSignal:
			room.broadcast(env, client.id)
		default:
			room.broadcast(env, client.id)
		}
	case protocol.TypeJoinDecision:
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
		target.closeOnce.Do(func() { close(target.send) })
		room.mu.Lock()
		delete(room.clients, payload.ClientID)
		room.mu.Unlock()
		delivered := room.broadcast(protocol.NewEnvelope(protocol.TypePeerLeft, "server", "", fiber.Map{"clientId": payload.ClientID}), "")
		log.Printf("collab kicked room=%s target=%s by=%s reason=%s delivered=%d", client.roomID, payload.ClientID, client.id, payload.Reason, delivered)
	default:
		client.sendError("unknown_type", "未知消息类型")
	}
}

func (client *Client) maybeAutoCompact() {
	count, err := client.hub.store.UpdateCount(context.Background(), client.roomID)
	if err != nil || count < autoCompactUpdateThreshold {
		return
	}
	log.Printf("collab auto_compact triggered room=%s update_count=%d threshold=%d", client.roomID, count, autoCompactUpdateThreshold)
}

func (client *Client) sendError(code string, message string) {
	go func() {
		client.queue(protocol.NewEnvelope(protocol.TypeError, "server", client.id, protocol.ErrorPayload{Code: code, Message: message}))
	}()
}

func storedUpdatesForProtocol(updates []storage.RoomUpdate) []protocol.StoredUpdate {
	if len(updates) == 0 {
		return nil
	}
	result := make([]protocol.StoredUpdate, 0, len(updates))
	for _, u := range updates {
		result = append(result, protocol.StoredUpdate{Seq: u.Seq, UpdateBase64: protocol.EncodeBytes(u.Update)})
	}
	return result
}

func mustJSON(value interface{}) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("json marshal: %v", err))
	}
	return raw
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
