package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"

	"timenotesserver/internal/protocol"
	"timenotesserver/internal/storage"
	"timenotesserver/internal/storage/sqlite"
)

func TestCreateRoomAPI(t *testing.T) {
	store, err := sqlite.Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	app := fiber.New()
	NewHub(store, "test-secret").RegisterRoutes(app)

	body := strings.NewReader(`{"serverUrl":"http://10.0.0.2:8787","appUrl":"http://127.0.0.1:9245/"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/rooms", body)
	req.Host = "127.0.0.1:8787"
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var payload protocol.CreateRoomResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.RoomID == "" || payload.RoomKey == "" {
		t.Fatalf("room id/key should be returned: %+v", payload)
	}
	if payload.WSURL != "ws://10.0.0.2:8787/ws/collab" {
		t.Fatalf("unexpected ws url: %s", payload.WSURL)
	}
	if !strings.Contains(payload.InviteURL, "#") || !strings.Contains(payload.InviteURL, "roomId=") || !strings.Contains(payload.InviteURL, "roomKey=") {
		t.Fatalf("invite url should carry room data in fragment: %s", payload.InviteURL)
	}
	if strings.Contains(payload.InviteURL, "%253A") {
		t.Fatalf("invite fragment should not be double-encoded: %s", payload.InviteURL)
	}
	beforeFragment := strings.SplitN(payload.InviteURL, "#", 2)[0]
	if strings.Contains(beforeFragment, payload.RoomKey) {
		t.Fatalf("room key must not be placed before URL fragment: %s", payload.InviteURL)
	}
}

func TestUniqueClientIDLocked(t *testing.T) {
	room := &Room{id: "room-1", clients: map[string]*Client{
		"user-1": {id: "user-1"},
	}}
	got := room.uniqueClientIDLocked("user-1")
	if got == "" || got == "user-1" {
		t.Fatalf("duplicate client id should be rewritten, got %q", got)
	}
	if got2 := room.uniqueClientIDLocked("user-2"); got2 != "user-2" {
		t.Fatalf("unused client id should be kept, got %q", got2)
	}
}

func TestHostLeaveClosesRoom(t *testing.T) {
	store, err := sqlite.Open(filepath.Join(t.TempDir(), "collab.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hub := NewHub(store, "test-secret")
	roomID := "room-close"
	keyHash := hub.roomKeyHash(roomID, "room-key")
	if err := store.EnsureRoom(context.Background(), roomID, keyHash); err != nil {
		t.Fatalf("ensure room: %v", err)
	}

	host := &Client{id: "host", roomID: roomID, user: protocol.User{ID: "host", Name: "房主"}, send: make(chan protocol.Envelope, 4), hub: hub}
	room := hub.joinRoom(host, storage.RoomState{})
	if host.user.Role != "host" || room.hostID != host.id {
		t.Fatalf("first client should become host, role=%s host=%s", host.user.Role, room.hostID)
	}
	guest := &Client{id: "guest", roomID: roomID, user: protocol.User{ID: "guest", Name: "协作者"}, send: make(chan protocol.Envelope, 4), hub: hub}
	hub.joinRoom(guest, storage.RoomState{})
	if guest.user.Role != "collaborator" {
		t.Fatalf("second client should become collaborator, got %s", guest.user.Role)
	}

	hub.leaveRoom(room, host)
	foundClosed := false
	for env := range guest.send {
		if env.Type == protocol.TypeRoomClosed {
			foundClosed = true
			break
		}
	}
	if !foundClosed {
		t.Fatalf("guest should receive room_closed before channel closes")
	}
	if err := store.EnsureRoom(context.Background(), roomID, keyHash); !errors.Is(err, storage.ErrRoomClosed) {
		t.Fatalf("closed room should reject future joins, got %v", err)
	}
}
