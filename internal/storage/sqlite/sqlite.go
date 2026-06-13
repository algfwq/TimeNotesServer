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

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if path == "" {
		path = filepath.Join("data", "timenotes-collab.db")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_cache_size=-16000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
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

func (s *Store) EnsureRoom(ctx context.Context, roomID string, keyHash string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var existing string
	var closedAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT key_hash, closed_at FROM rooms WHERE room_id = ?`, roomID).Scan(&existing, &closedAt)
	if err == nil {
		if closedAt.Valid && closedAt.String != "" {
			return storage.ErrRoomClosed
		}
		if existing != keyHash {
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
	_, err = tx.ExecContext(ctx, `INSERT INTO rooms(room_id, key_hash, created_at, updated_at) VALUES(?, ?, ?, ?)`, roomID, keyHash, now, now)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) LoadRoomState(ctx context.Context, roomID string) (storage.RoomState, error) {
	var state storage.RoomState
	var compact []byte
	err := s.db.QueryRowContext(ctx, `SELECT compact_state FROM rooms WHERE room_id = ?`, roomID).Scan(&compact)
	if err != nil && err != sql.ErrNoRows {
		return state, err
	}
	state.CompactState = compact
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

func (s *Store) AppendUpdate(ctx context.Context, roomID string, update []byte) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var seq int64
	var closedAt sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT update_seq, closed_at FROM rooms WHERE room_id = ?`, roomID).Scan(&seq, &closedAt); err != nil {
		return 0, err
	}
	if closedAt.Valid && closedAt.String != "" {
		return 0, storage.ErrRoomClosed
	}
	seq++
	if _, err := tx.ExecContext(ctx, `INSERT INTO room_updates(room_id, seq, update_blob, created_at) VALUES(?, ?, ?, ?)`, roomID, seq, update, now); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE rooms SET update_seq = ?, updated_at = ? WHERE room_id = ?`, seq, now, roomID); err != nil {
		return 0, err
	}
	return seq, tx.Commit()
}

func (s *Store) SaveSnapshot(ctx context.Context, roomID string, state []byte) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE rooms SET compact_state = ?, updated_at = ? WHERE room_id = ? AND closed_at IS NULL`, state, now, roomID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return storage.ErrRoomClosed
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM room_updates WHERE room_id = ?`, roomID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CloseRoom(ctx context.Context, roomID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE rooms SET closed_at = ?, updated_at = ? WHERE room_id = ? AND closed_at IS NULL`, now, now, roomID)
	return err
}

func (s *Store) UpdateCount(ctx context.Context, roomID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM room_updates WHERE room_id = ?`, roomID).Scan(&count)
	return count, err
}
