package main

import (
	"context"
	"dagger/olympusgcp-data/internal/dagger"
)

type OlympusGCPData struct{}

func (m *OlympusGCPData) HelloWorld(ctx context.Context) string {
	return "Hello from OlympusGCP-Data!"
}

func main() {
	dagger.Serve()
}
