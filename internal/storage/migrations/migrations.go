package migrations

import "embed"

// Files contains SQL migrations for all storage implementations.
//
//go:embed *.sql
var Files embed.FS
