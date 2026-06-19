package storage

import (
	"context"
	"errors"
	"time"
)

// ErrRoomKeyMismatch 表示 roomId 存在但 roomKey 的 HMAC 不匹配。
// 上层会把它转换成统一的“房间密钥无效”，不要把数据库细节暴露给客户端。
var ErrRoomKeyMismatch = errors.New("room key mismatch")

// ErrRoomClosed 表示房间已经被房主关闭。
// 关闭后的房间不能再通过旧邀请链接加入，避免房主退出后协作者继续使用同一个房间。
var ErrRoomClosed = errors.New("room closed")

// Store 是协作服务唯一依赖的持久化接口。
// 协议层、房间管理和 WebSocket 逻辑都只面向这个接口，SQLite/PostgreSQL 的差异只能留在具体实现里。
type Store interface {
	// Close 释放数据库资源。
	Close() error
	// EnsureRoom 创建房间或校验已有房间的 keyHash。
	EnsureRoom(ctx context.Context, roomID string, keyHash string) error
	// LoadRoomState 返回 compact snapshot 和 snapshot 之后的增量 update。
	LoadRoomState(ctx context.Context, roomID string) (RoomState, error)
	// AppendUpdate 原子追加一条 Yjs update，并返回房间内递增序号。
	AppendUpdate(ctx context.Context, roomID string, update []byte) (int64, error)
	// SaveSnapshot 保存 compact snapshot，baseSeq 为快照覆盖的最大 update seq；
	// 只删除 seq <= baseSeq 的增量，保留快照生成期间其他客户端新 append 的高 seq 增量。
	// baseSeq <= 0 时兼容旧客户端，删除全部增量。
	SaveSnapshot(ctx context.Context, roomID string, state []byte, baseSeq int64) error
	// CloseRoom 标记房间关闭。关闭后的房间不允许旧 roomKey 再次加入。
	CloseRoom(ctx context.Context, roomID string) error
	// UpdateCount 返回指定房间的增量 update 条数，用于触发自动 compact。
	UpdateCount(ctx context.Context, roomID string) (int, error)
	// RoomStorageBytes 计算单房间当前累计存储字节数（compact_state + 所有 update_blob 长度）。
	RoomStorageBytes(ctx context.Context, roomID string) (int64, error)
	// DeleteInactiveRooms 删除 updated_at 早于 cutoff 的已关闭房间及其关联数据，返回删除行数。
	DeleteInactiveRooms(ctx context.Context, cutoff time.Time) (int, error)
}

// RoomState 是后加入者恢复文档所需的最小状态集合。
type RoomState struct {
	CompactState []byte
	Updates      []RoomUpdate
}

// RoomUpdate 是一条按 seq 排序的 Yjs 二进制增量。
type RoomUpdate struct {
	Seq    int64
	Update []byte
}
