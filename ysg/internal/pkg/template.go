package pkg

import "embed"

//go:embed templates/*
var Templates embed.FS
