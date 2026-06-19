-- 添加 storage_bytes 和 update_count 计数器列，避免每次 doc_update/Snapshot 全表聚合扫描。
-- storage_bytes 跟踪 rooms.compact_state + 所有 room_updates.update_blob 的累计字节数。
-- update_count 跟踪当前未压缩的 room_updates 行数（SaveSnapshot 后重置为 0）。
ALTER TABLE rooms ADD COLUMN storage_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE rooms ADD COLUMN update_count INTEGER NOT NULL DEFAULT 0;

-- 回填已有数据的计数器值（迁移前已存在的房间）。
UPDATE rooms SET
  storage_bytes = COALESCE(LENGTH(compact_state), 0)
    + COALESCE((SELECT SUM(LENGTH(update_blob)) FROM room_updates WHERE room_updates.room_id = rooms.room_id), 0),
  update_count = COALESCE((SELECT COUNT(*) FROM room_updates WHERE room_updates.room_id = rooms.room_id), 0);
