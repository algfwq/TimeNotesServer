package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

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
	if err := store.SaveSnapshot(ctx, "room-1", []byte{9, 9}); err != nil {
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
	if err := store.SaveSnapshot(ctx, "room-1", []byte{7}); !errors.Is(err, storage.ErrRoomClosed) {
		t.Fatalf("closed room should reject snapshot, got %v", err)
	}
}
