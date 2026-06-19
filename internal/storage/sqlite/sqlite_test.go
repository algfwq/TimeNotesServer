package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"timenotesserver/internal/storage"
)

func TestRoomLifecycle(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.EnsureRoom(ctx, "room-1", "hash-a"); err != nil {
		t.Fatalf("ensure room: %v", err)
	}
	if err := store.EnsureRoom(ctx, "room-1", "hash-a"); err != nil {
		t.Fatalf("ensure existing room: %v", err)
	}
	if err := store.EnsureRoom(ctx, "room-1", "hash-b"); !errors.Is(err, storage.ErrRoomKeyMismatch) {
		t.Fatalf("expected key mismatch, got %v", err)
	}
	if seq, err := store.AppendUpdate(ctx, "room-1", []byte{1, 2, 3}); err != nil || seq != 1 {
		t.Fatalf("append update seq=%d err=%v", seq, err)
	}
	if seq, err := store.AppendUpdate(ctx, "room-1", []byte{4, 5}); err != nil || seq != 2 {
		t.Fatalf("append second update seq=%d err=%v", seq, err)
	}
	state, err := store.LoadRoomState(ctx, "room-1")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.Updates) != 2 || state.Updates[0].Seq != 1 || state.Updates[1].Seq != 2 {
		t.Fatalf("unexpected updates: %+v", state.Updates)
	}
	if err := store.SaveSnapshot(ctx, "room-1", []byte{9, 9}, 2); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	state, err = store.LoadRoomState(ctx, "room-1")
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if string(state.CompactState) != string([]byte{9, 9}) {
		t.Fatalf("snapshot not saved: %v", state.CompactState)
	}
	if len(state.Updates) != 0 {
		t.Fatalf("updates should be compacted, got %d", len(state.Updates))
	}
	if err := store.CloseRoom(ctx, "room-1"); err != nil {
		t.Fatalf("close room: %v", err)
	}
	if err := store.EnsureRoom(ctx, "room-1", "hash-a"); !errors.Is(err, storage.ErrRoomClosed) {
		t.Fatalf("closed room should reject ensure, got %v", err)
	}
	if _, err := store.AppendUpdate(ctx, "room-1", []byte{6}); !errors.Is(err, storage.ErrRoomClosed) {
		t.Fatalf("closed room should reject append, got %v", err)
	}
	if err := store.SaveSnapshot(ctx, "room-1", []byte{7}, 0); !errors.Is(err, storage.ErrRoomClosed) {
		t.Fatalf("closed room should reject snapshot, got %v", err)
	}
}

// TestOrphanCleanup 验证从未连接过的孤儿房间能被DeleteInactiveRooms清理。
func TestOrphanCleanup(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.EnsureRoom(ctx, "orphan-room", "hash"); err != nil {
		t.Fatal(err)
	}
	// 不进行任何append/join，该房间updated_at停留在创建时间。
	// 清理cutoff设为未来，确认不会被删。
	n, err := store.DeleteInactiveRooms(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("recent orphan should not be deleted, got %d", n)
	}
	// 清理cutoff设为创建时间后+1秒，应删除。
	n, err = store.DeleteInactiveRooms(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("old orphan should be deleted, got %d", n)
	}
}

// TestSnapshotBaseSeq 验证SaveSnapshot只删除seq <= baseSeq的增量。
func TestSnapshotBaseSeq(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.EnsureRoom(ctx, "room", "hash"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := store.AppendUpdate(ctx, "room", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 快照覆盖前3条（seq 1-3），保留后2条（seq 4-5）。
	if err := store.SaveSnapshot(ctx, "room", []byte{99}, 3); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadRoomState(ctx, "room")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Updates) != 2 {
		t.Fatalf("expected 2 remaining updates, got %d", len(state.Updates))
	}
	if state.Updates[0].Seq != 4 || state.Updates[1].Seq != 5 {
		t.Fatalf("expected seq 4 and 5, got %d and %d", state.Updates[0].Seq, state.Updates[1].Seq)
	}
}

// TestStorageCounters 验证计数器列维护正确。
func TestStorageCounters(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.EnsureRoom(ctx, "room", "hash"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendUpdate(ctx, "room", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendUpdate(ctx, "room", []byte{4, 5}); err != nil {
		t.Fatal(err)
	}
	count, err := store.UpdateCount(ctx, "room")
	if err != nil || count != 2 {
		t.Fatalf("expected 2 updates, got count=%d err=%v", count, err)
	}
	bytes, err := store.RoomStorageBytes(ctx, "room")
	if err != nil || bytes != 5 {
		t.Fatalf("expected 5 bytes, got bytes=%d err=%v", bytes, err)
	}
	// 快照后计数器重置。
	if err := store.SaveSnapshot(ctx, "room", []byte{9, 9}, 2); err != nil {
		t.Fatal(err)
	}
	count, err = store.UpdateCount(ctx, "room")
	if err != nil || count != 0 {
		t.Fatalf("expected 0 updates after snapshot, got count=%d err=%v", count, err)
	}
	bytes, err = store.RoomStorageBytes(ctx, "room")
	if err != nil || bytes != 2 {
		t.Fatalf("expected 2 bytes after snapshot, got bytes=%d err=%v", bytes, err)
	}
}
