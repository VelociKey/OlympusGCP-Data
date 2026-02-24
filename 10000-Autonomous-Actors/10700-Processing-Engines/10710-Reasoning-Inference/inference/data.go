package inference

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	datav1 "OlympusGCP-Data/gen/v1/data"
	"connectrpc.com/connect"
)

type DataServer struct {
	baseDir string
}

func NewDataServer(baseDir string) *DataServer {
	return &DataServer{baseDir: baseDir}
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
