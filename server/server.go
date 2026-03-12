package server

import (
	"context"
	"strings"
	"sync"

	pb "github.com/fred/go_index/proto/goindex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GoIndexServer implements the GoIndexService gRPC service.
type GoIndexServer struct {
	pb.UnimplementedGoIndexServiceServer
	mu      sync.RWMutex
	modules []*pb.Module
	// key: "path@version"
	index map[string]int
}

func NewGoIndexServer() *GoIndexServer {
	return &GoIndexServer{
		index: make(map[string]int),
	}
}

func moduleKey(path, version string) string {
	return path + "@" + version
}

func (s *GoIndexServer) AddModule(_ context.Context, req *pb.AddModuleRequest) (*pb.AddModuleResponse, error) {
	if req.Module == nil || req.Module.Path == "" || req.Module.Version == "" {
		return nil, status.Error(codes.InvalidArgument, "module path and version are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := moduleKey(req.Module.Path, req.Module.Version)
	if idx, exists := s.index[key]; exists {
		s.modules[idx] = req.Module
	} else {
		s.index[key] = len(s.modules)
		s.modules = append(s.modules, req.Module)
	}

	return &pb.AddModuleResponse{Module: req.Module}, nil
}

func (s *GoIndexServer) GetModule(_ context.Context, req *pb.GetModuleRequest) (*pb.Module, error) {
	if req.Path == "" || req.Version == "" {
		return nil, status.Error(codes.InvalidArgument, "path and version are required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := moduleKey(req.Path, req.Version)
	idx, exists := s.index[key]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "module %s not found", key)
	}

	return s.modules[idx], nil
}

func (s *GoIndexServer) ListModules(_ context.Context, req *pb.ListModulesRequest) (*pb.ListModulesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*pb.Module
	for _, m := range s.modules {
		if req.PathPrefix != "" && !strings.HasPrefix(m.Path, req.PathPrefix) {
			continue
		}
		result = append(result, m)
		if req.Limit > 0 && int32(len(result)) >= req.Limit {
			break
		}
	}

	return &pb.ListModulesResponse{Modules: result}, nil
}
