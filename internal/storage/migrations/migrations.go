package migrations

import "embed"

// Files 内嵌 SQLite 迁移脚本，启动时由 sqlite.Store 按文件名顺序执行。
// 迁移 SQL 必须保持幂等，因为当前实现没有单独的 schema_version 表。
//
//go:embed *.sql
var Files embed.FS
