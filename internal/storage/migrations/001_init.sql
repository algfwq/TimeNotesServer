CREATE TABLE IF NOT EXISTS rooms (
  room_id TEXT PRIMARY KEY,
  key_hash TEXT NOT NULL,
  compact_state BLOB,
  update_seq INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  closed_at TEXT
);

CREATE TABLE IF NOT EXISTS room_updates (
  room_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  update_blob BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (room_id, seq),
  FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_room_updates_room_seq ON room_updates(room_id, seq);
