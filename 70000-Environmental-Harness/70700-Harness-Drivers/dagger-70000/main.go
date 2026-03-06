package main

import (
	"context"
<<<<<<< HEAD:70000-Environmental-Harness/70700-Harness-Drivers/dagger-70000/main.go
	"dagger/olympusgcp-data/internal/dagger"
=======
	"olympus.fleet/00SDLC/OlympusForge/70000-Environmental-Harness/dagger/olympusgcp-data/internal/dagger"
>>>>>>> origin/development:70000-Environmental-Harness/dagger/main.go
)

type OlympusGCPData struct{}

func (m *OlympusGCPData) HelloWorld(ctx context.Context) string {
	return "Hello from OlympusGCP-Data!"
}

func main() {
	dagger.Serve()
}
