package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"timenotesserver/internal/storage"
	"timenotesserver/internal/storage/migrations"

	_ "modernc.org/sqlite"
)

// Store 是 storage.Store 的 SQLite 实现。
// 第一阶段用单文件数据库降低部署成本；协议层只依赖 storage.Store，后续 PostgreSQL 可新增实现替换这里。
type Store struct {
	db *sql.DB
}

// Open 打开 SQLite 数据库并执行内嵌迁移。
// path 为空时使用 data/timenotes-collab.db，方便本地直接运行 go build 后启动。
func Open(path string) (*Store, error) {
	if path == "" {
		path = filepath.Join("data", "timenotes-collab.db")
	}
	// SQLite 文件所在目录需要预先存在；这里创建目录避免首次启动失败。
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// modernc SQLite 在多连接写入时容易遇到锁等待；协作服务写入主要是追加 update，
	// 第一阶段固定单连接能获得更稳定的顺序写入语义。迁移 PostgreSQL 后再放开连接池。
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close 关闭底层数据库连接。主进程退出时调用即可。
func (s *Store) Close() error {
	return s.db.Close()
}

// migrate 按文件名顺序执行 internal/storage/migrations 中的 SQL。
// 迁移文件应写成幂等形式，例如 CREATE TABLE IF NOT EXISTS，避免重复启动失败。
func (s *Store) migrate(ctx context.Context) error {
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// 当前迁移文件很少，直接一次性读入即可；复杂迁移后可增加版本表和事务控制。
		body, err := migrations.Files.ReadFile(entry.Name())
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}
	return s.ensureClosedAtColumn(ctx)
}

// ensureClosedAtColumn 给旧版 SQLite 数据库补 closed_at 字段。
// 当前迁移文件需要重复执行保持幂等，SQLite 的 ALTER TABLE IF NOT EXISTS 兼容性不如手动检查稳定。
func (s *Store) ensureClosedAtColumn(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(rooms)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == "closed_at" {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE rooms ADD COLUMN closed_at TEXT`)
	return err
}

// EnsureRoom 确保房间存在并校验 roomKey 哈希。
// - 新房间：插入 room_id + key_hash；
// - 已存在且 key_hash 相同：刷新 updated_at；
// - 已存在但 key_hash 不同：返回 ErrRoomKeyMismatch，拒绝加入。
func (s *Store) EnsureRoom(ctx context.Context, roomID string, keyHash string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// 使用事务保证“查 key_hash + 更新/插入”是一个一致的操作。
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Commit 成功后 Rollback 会被忽略；出错路径自动回滚，减少遗漏。
	defer func() { _ = tx.Rollback() }()

	var existing string
	var closedAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT key_hash, closed_at FROM rooms WHERE room_id = ?`, roomID).Scan(&existing, &closedAt)
	if err == nil {
		if closedAt.Valid && closedAt.String != "" {
			return storage.ErrRoomClosed
		}
		if existing != keyHash {
			// 这里不返回“房间不存在/密钥不对”的细节，避免外部探测 roomId 是否存在。
			return storage.ErrRoomKeyMismatch
		}
		_, err = tx.ExecContext(ctx, `UPDATE rooms SET updated_at = ? WHERE room_id = ?`, now, roomID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != sql.ErrNoRows {
		return err
	}
	// 房间第一次创建时 update_seq 默认为迁移 SQL 中定义的 0。
	_, err = tx.ExecContext(ctx, `INSERT INTO rooms(room_id, key_hash, created_at, updated_at) VALUES(?, ?, ?, ?)`, roomID, keyHash, now, now)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// LoadRoomState 读取后加入者需要的完整 Yjs 状态：
// 先返回 compact_state，再按 seq 升序返回 compact 之后的所有增量 update。
func (s *Store) LoadRoomState(ctx context.Context, roomID string) (storage.RoomState, error) {
	var state storage.RoomState
	var compact []byte
	err := s.db.QueryRowContext(ctx, `SELECT compact_state FROM rooms WHERE room_id = ?`, roomID).Scan(&compact)
	if err != nil && err != sql.ErrNoRows {
		return state, err
	}
	state.CompactState = compact
	// room_updates 按 seq 排序很重要，Yjs update 必须按保存顺序重放才能便于排障。
	rows, err := s.db.QueryContext(ctx, `SELECT seq, update_blob FROM room_updates WHERE room_id = ? ORDER BY seq ASC`, roomID)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	for rows.Next() {
		var update storage.RoomUpdate
		if err := rows.Scan(&update.Seq, &update.Update); err != nil {
			return state, err
		}
		state.Updates = append(state.Updates, update)
	}
	return state, rows.Err()
}

// AppendUpdate 追加一条 Yjs update，并原子递增房间 update_seq。
// 返回的 seq 用于日志和后续恢复顺序检查。
func (s *Store) AppendUpdate(ctx context.Context, roomID string, update []byte) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var seq int64
	var closedAt sql.NullString
	// 先从 rooms 读取当前 seq；如果房间不存在，说明鉴权流程异常或数据库被外部删除。
	if err := tx.QueryRowContext(ctx, `SELECT update_seq, closed_at FROM rooms WHERE room_id = ?`, roomID).Scan(&seq, &closedAt); err != nil {
		return 0, err
	}
	if closedAt.Valid && closedAt.String != "" {
		return 0, storage.ErrRoomClosed
	}
	seq++
	// update_blob 是 Yjs 原始二进制，不尝试解析或转 JSON，避免破坏 CRDT 状态。
	if _, err := tx.ExecContext(ctx, `INSERT INTO room_updates(room_id, seq, update_blob, created_at) VALUES(?, ?, ?, ?)`, roomID, seq, update, now); err != nil {
		return 0, err
	}
	// rooms.update_seq 是房间级最新序号，便于后续做增量拉取或诊断。
	if _, err := tx.ExecContext(ctx, `UPDATE rooms SET update_seq = ?, updated_at = ? WHERE room_id = ?`, seq, now, roomID); err != nil {
		return 0, err
	}
	return seq, tx.Commit()
}

// SaveSnapshot 保存 Yjs compact_state，并清空已被 compact 覆盖的历史增量。
// 这样后加入者只需要应用一个 snapshot + snapshot 后的新 update，避免长期房间越来越慢。
func (s *Store) SaveSnapshot(ctx context.Context, roomID string, state []byte) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// compact_state 是客户端生成的 Yjs state vector/full state，服务端不理解其内部结构。
	result, err := tx.ExecContext(ctx, `UPDATE rooms SET compact_state = ?, updated_at = ? WHERE room_id = ? AND closed_at IS NULL`, state, now, roomID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return storage.ErrRoomClosed
	}
	// 删除历史 update 必须和保存 snapshot 在同一事务里，否则崩溃可能导致状态丢失或重复。
	if _, err := tx.ExecContext(ctx, `DELETE FROM room_updates WHERE room_id = ?`, roomID); err != nil {
		return err
	}
	return tx.Commit()
}

// CloseRoom 持久关闭房间。关闭后的 roomId/roomKey 不能再通过 EnsureRoom 校验。
func (s *Store) CloseRoom(ctx context.Context, roomID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE rooms SET closed_at = ?, updated_at = ? WHERE room_id = ? AND closed_at IS NULL`, now, now, roomID)
	return err
}
