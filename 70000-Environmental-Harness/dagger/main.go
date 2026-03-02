package main

import (
	"context"
	"olympus.fleet/00SDLC/OlympusForge/70000-Environmental-Harness/dagger/olympusgcp-data/internal/dagger"
)

type OlympusGCPData struct{}

func (m *OlympusGCPData) HelloWorld(ctx context.Context) string {
	return "Hello from OlympusGCP-Data!"
}

func main() {
	dagger.Serve()
}
