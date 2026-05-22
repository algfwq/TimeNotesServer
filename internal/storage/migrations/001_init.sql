-- rooms 保存房间级元数据和 compact_state。
-- key_hash 是 roomId + roomKey + 服务端 secret 的 HMAC，数据库不保存 roomKey 原文。
CREATE TABLE IF NOT EXISTS rooms (
  room_id TEXT PRIMARY KEY,
  key_hash TEXT NOT NULL,
  -- compact_state 是客户端生成的 Yjs 全量状态，服务端不解析内部 CRDT 结构。
  compact_state BLOB,
  -- update_seq 是 room_updates 的单调递增序号，用于恢复顺序和日志排障。
  update_seq INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  -- closed_at 非空表示房主已经离开，旧邀请链接不能继续加入该房间。
  closed_at TEXT
);

-- room_updates 保存 compact_state 之后的 Yjs 增量。
-- 后加入者按 seq 升序应用 compact_state + updates 即可恢复当前文档。
CREATE TABLE IF NOT EXISTS room_updates (
  room_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  -- update_blob 是 Yjs 原始二进制 update，不做 JSON 化，避免破坏 CRDT payload。
  update_blob BLOB NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (room_id, seq),
  FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE
);

-- 加速 LoadRoomState 的按房间、按序号读取路径。
CREATE INDEX IF NOT EXISTS idx_room_updates_room_seq ON room_updates(room_id, seq);
