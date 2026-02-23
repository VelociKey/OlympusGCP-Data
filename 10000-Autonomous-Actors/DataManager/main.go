package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	datav1 "OlympusGCP-Data/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/data/v1"
	"OlympusGCP-Data/40000-Communication-Contracts/430-Protocol-Definitions/000-gen/data/v1/datav1connect"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type DataServer struct {
	baseDir string
}

func (s *DataServer) Upsert(ctx context.Context, req *connect.Request[datav1.UpsertRequest]) (*connect.Response[datav1.UpsertResponse], error) {
	slog.Info("Upsert", "collection", req.Msg.Collection, "id", req.Msg.DocId)
	path := filepath.Join(s.baseDir, req.Msg.Collection, req.Msg.DocId+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to create collection: %v", err))
	}
	if err := os.WriteFile(path, []byte(req.Msg.DataJson), 0644); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write doc: %v", err))
	}
	return connect.NewResponse(&datav1.UpsertResponse{}), nil
}

func (s *DataServer) WriteRow(ctx context.Context, req *connect.Request[datav1.WriteRowRequest]) (*connect.Response[datav1.WriteRowResponse], error) {
	slog.Info("WriteRow", "table", req.Msg.Table, "key", req.Msg.RowKey)
	return connect.NewResponse(&datav1.WriteRowResponse{}), nil
}

func (s *DataServer) Query(ctx context.Context, req *connect.Request[datav1.QueryRequest]) (*connect.Response[datav1.QueryResponse], error) {
	slog.Info("Query", "collection", req.Msg.Collection, "query", req.Msg.Query)
	return connect.NewResponse(&datav1.QueryResponse{ResultsJson: "[]"}), nil
}

func main() {
	server := &DataServer{baseDir: "../../60000-Information-Storage/DataStore"}
	mux := http.NewServeMux()
	path, handler := datav1connect.NewDataServiceHandler(server)
	mux.Handle(path, handler)

	port := "8093" // From genesis.json
	slog.Info("DataManager starting", "port", port)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 3 * time.Second,
	}
	err := srv.ListenAndServe()
	if err != nil {
		slog.Error("Server failed", "error", err)
	}
}
