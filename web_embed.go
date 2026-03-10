package main

import (
	"embed"
	"io/fs"
)

//go:embed all:web
var embeddedWebFS embed.FS

var embeddedWebSubFS fs.FS = mustSubFS(embeddedWebFS, "web")
