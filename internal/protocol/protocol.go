package protocol

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// Version 是协作 envelope 的协议版本。
// 当前仍处于第一阶段，前端和服务端都按 v=1 解析；未来新增破坏性字段时应提高版本并做兼容分支。
const Version = 1

const (
	// TypeAuth 必须是 WebSocket 首帧；没有鉴权的连接不能进入任何房间。
	TypeAuth = "auth"
	// TypeAuthOK 是服务端鉴权成功响应，包含服务端确认的 clientId、在线成员和可恢复的 Yjs 状态。
	TypeAuthOK = "auth_ok"
	// TypeSyncRequest 允许客户端主动重新拉取 SQLite 状态，用于发现丢包或重连后的自愈。
	TypeSyncRequest = "sync_request"
	// TypeDocUpdate 承载 Yjs 二进制增量，服务端会持久化；relay=true 时也会转发。
	TypeDocUpdate = "doc_update"
	// TypeDocSnapshot 承载 Yjs 全量状态，保存后可清理历史增量，降低后加入者重放成本。
	TypeDocSnapshot = "doc_snapshot"
	// TypePresence 是鼠标、当前页面、选中元素、正在编辑元素等在线状态，不落库。
	TypePresence = "presence"
	// TypeSignal 只用于 WebRTC offer/answer/ICE candidate 信令转发。
	TypeSignal = "signal"
	// TypeRelay 是 P2P 失败后的兜底中转通道。
	TypeRelay = "relay"
	// TypeChat 是在线聊天消息，优先 P2P，必要时中转，不落库。
	TypeChat = "chat"
	// TypePing/TypePong 用于客户端显示连接延迟。
	TypePing = "ping"
	TypePong = "pong"
	// TypePeerJoined/TypePeerLeft 用于维护在线成员列表。
	TypePeerJoined = "peer_joined"
	TypePeerLeft   = "peer_left"
	// TypeRoomClosed 表示房主已经离开，房间生命周期结束，其他协作者必须退出当前协作。
	TypeRoomClosed = "room_closed"
	// TypeError 是结构化错误，前端可以直接映射为 toast 或状态栏文案。
	TypeError = "error"
)

// CreateRoomRequest 是“发起联机”按钮调用的 HTTP 请求体。
// serverUrl/appUrl 都由客户端显式传入，服务端只用它们生成可复制邀请链接；
// 房间密钥仍只出现在响应和邀请链接 fragment 中，不写入日志。
type CreateRoomRequest struct {
	ServerURL string `json:"serverUrl,omitempty"`
	AppURL    string `json:"appUrl,omitempty"`
}

// CreateRoomResponse 返回新房间的连接信息。
// roomKey 是敏感值，前端只能用于当前连接和邀请链接，不应持久写入文档。
type CreateRoomResponse struct {
	RoomID    string `json:"roomId"`
	RoomKey   string `json:"roomKey"`
	WSURL     string `json:"wsUrl"`
	InviteURL string `json:"inviteUrl"`
}

// Envelope 是所有 WebSocket/DataChannel 消息的外层协议。
// 好处是 P2P 和服务器中转可以复用同一套消息结构，前端只根据 transport 决定走 DataChannel 还是 relay。
type Envelope struct {
	// Version 用于未来协议升级；当前为 1。
	Version int `json:"v"`
	// Type 决定 Payload 的具体结构。
	Type string `json:"type"`
	// ID 是消息 ID，便于日志关联和客户端去重。
	ID string `json:"id,omitempty"`
	// From 在服务端 readLoop 中被覆盖为真实 clientId，不能信任客户端自报。
	From string `json:"from,omitempty"`
	// To 为空表示广播，非空表示点对点投递。
	To string `json:"to,omitempty"`
	// SentAt 由服务端或本地发送端填充，用于调试和延迟观察。
	SentAt string `json:"sentAt,omitempty"`
	// Payload 保持 RawMessage，避免每次转发都反序列化未知业务内容。
	Payload json.RawMessage `json:"payload,omitempty"`
}

// User 是 presence 中的在线用户快照。
// 服务端只持有最近一次快照；重启后在线状态丢失，但文档内容可由 Yjs 状态恢复。
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
	// PageID 表示用户当前正在查看/编辑的页面，前端用它在页面标签上显示协作者头像。
	PageID string `json:"pageId,omitempty"`
	// SelectedElementID/EditingElementID 不持久化，只用于远端高亮和“正在编辑”提示。
	SelectedElementID string `json:"selectedElementId,omitempty"`
	EditingElementID  string `json:"editingElementId,omitempty"`
	// Cursor 为 nil 表示鼠标不在画布内，前端应隐藏远端光标。
	Cursor *Cursor `json:"cursor"`
	// Transport 表示客户端当前实际使用 p2p/relay/offline，供 UI 和排障使用。
	Transport string `json:"transport,omitempty"`
	LastSeen  string `json:"lastSeen,omitempty"`
	// Role 由服务端分配，host 可以管理页面结构，collaborator 只能编辑当前文档内容。
	Role string `json:"role,omitempty"`
}

// Cursor 使用页面坐标，不使用屏幕像素；这样不同缩放比例下远端光标仍能落在同一画布位置。
type Cursor struct {
	PageID string  `json:"pageId"`
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
}

// AuthPayload 是 WebSocket 首帧 payload。
// RoomKey 是敏感字段，服务端只做 HMAC 校验，不写日志、不回显。
type AuthPayload struct {
	RoomID  string `json:"roomId"`
	RoomKey string `json:"roomKey"`
	User    User   `json:"user"`
}

// AuthOKPayload 是加入房间后的初始化数据。
// CompactStateBase64 + Updates 按顺序应用后应得到房间当前 Yjs 状态。
type AuthOKPayload struct {
	ClientID           string         `json:"clientId"`
	Peers              []User         `json:"peers"`
	CompactStateBase64 string         `json:"compactStateBase64,omitempty"`
	Updates            []StoredUpdate `json:"updates,omitempty"`
	IsHost             bool           `json:"isHost"`
	HostID             string         `json:"hostId,omitempty"`
}

// RoomClosedPayload 告诉客户端房间已经结束。
// 目前关闭原因只有 host_left；未来可以扩展为 server_shutdown、admin_closed 等。
type RoomClosedPayload struct {
	Reason string `json:"reason"`
	HostID string `json:"hostId,omitempty"`
}

type StoredUpdate struct {
	Seq          int64  `json:"seq"`
	UpdateBase64 string `json:"updateBase64"`
}

// DocUpdatePayload 传输 Yjs update。
// UpdateID 由客户端生成用于去重，UpdateBase64 是 update 二进制的 base64 文本。
type DocUpdatePayload struct {
	UpdateID     string `json:"updateId"`
	UpdateBase64 string `json:"updateBase64"`
	Relay        bool   `json:"relay,omitempty"`
}

// DocSnapshotPayload 保存 Yjs 全量状态；服务端不解析其内部结构。
type DocSnapshotPayload struct {
	StateBase64 string `json:"stateBase64"`
}

// PresencePayload 是高频在线状态；relay=false 时通常只用于服务端保存最新快照，不广播。
type PresencePayload struct {
	User  User `json:"user"`
	Relay bool `json:"relay,omitempty"`
}

// SignalPayload 是 WebRTC 信令内容。
// 服务端只按 To 转发，不解析 SDP/Candidate，避免把 P2P 细节耦合进业务层。
type SignalPayload struct {
	Kind      string          `json:"kind"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
	SDP       string          `json:"sdp,omitempty"`
}

// RelayPayload 可以包装需要服务端中转的业务消息。
// 当前客户端主要直接转发 envelope；保留该结构用于后续扩展明确的 relay 子类型。
type RelayPayload struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// ChatPayload 是在线聊天消息；服务端不落库，不记录 Text。
type ChatPayload struct {
	MessageID string `json:"messageId"`
	Text      string `json:"text"`
	User      User   `json:"user"`
	Relay     bool   `json:"relay,omitempty"`
}

type PingPayload struct {
	PingID string `json:"pingId"`
}

type PongPayload struct {
	PingID string `json:"pingId"`
}

type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// NewEnvelope 创建服务端发送或测试中使用的标准 envelope。
// Payload 在这里序列化成 RawMessage，便于后续直接写入 WebSocket。
func NewEnvelope(messageType string, from string, to string, payload any) Envelope {
	raw, _ := json.Marshal(payload)
	return Envelope{Version: Version, Type: messageType, ID: NewID("msg"), From: from, To: to, SentAt: time.Now().UTC().Format(time.RFC3339Nano), Payload: raw}
}

// DecodePayload 将 RawMessage 解成指定 payload 类型。
// 泛型让调用处可以清晰声明自己期望的协议结构。
func DecodePayload[T any](env Envelope) (T, error) {
	var payload T
	if len(env.Payload) == 0 {
		return payload, nil
	}
	err := json.Unmarshal(env.Payload, &payload)
	return payload, err
}

// EncodeBytes 把 Yjs 二进制 update/state 转为 JSON 安全的 base64 字符串。
func EncodeBytes(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeBytes 是 EncodeBytes 的逆操作；空字符串代表没有二进制内容。
func DecodeBytes(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(value)
}
