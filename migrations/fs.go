package migrations

import "embed"

// FS embeds all migration SQL scripts.
//
//go:embed *.sql
var FS embed.FS
