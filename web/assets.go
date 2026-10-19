package web

import "embed"

//go:embed index.html styles.css app.mjs
var Files embed.FS
