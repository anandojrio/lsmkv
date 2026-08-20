package node

import (
	"context"
	"errors"

	"lsmkv/internal/lsm"
	"lsmkv/internal/ring"
	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	lsmkvv1.UnimplementedKVServiceServer
	store *lsm.Store
	rt    *Runtime
}

func NewServer(store *lsm.Store, rt *Runtime) *Server {
	return &Server{
		store: store,
		rt:    rt,
	}
}

func (s *Server) Put(ctx context.Context, req *lsmkvv1.PutRequest) (*lsmkvv1.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		forwarded, err := s.forwardPutIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return &lsmkvv1.PutResponse{}, nil
		}
	}

	if req.Forwarded {
		if err := s.store.Put(req.Key, req.Value); err != nil {
			return nil, toGRPCError(err)
		}
		return &lsmkvv1.PutResponse{}, nil
	}

	if err := s.replicatePut(ctx, req.Key, req.Value); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &lsmkvv1.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *lsmkvv1.GetRequest) (*lsmkvv1.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		resp, forwarded, err := s.forwardGetIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return resp, nil
		}
	}

	if req.Forwarded {
		value, found, err := s.store.Get(req.Key)
		if err != nil {
			return nil, toGRPCError(err)
		}
		if !found {
			return nil, status.Error(codes.NotFound, "key not found")
		}

		return &lsmkvv1.GetResponse{Value: value}, nil
	}

	value, err := s.replicateGet(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &lsmkvv1.GetResponse{Value: value}, nil
}

func (s *Server) Delete(ctx context.Context, req *lsmkvv1.DeleteRequest) (*lsmkvv1.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		forwarded, err := s.forwardDeleteIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return &lsmkvv1.DeleteResponse{}, nil
		}
	}

	if req.Forwarded {
		if err := s.store.Delete(req.Key); err != nil {
			return nil, toGRPCError(err)
		}
		return &lsmkvv1.DeleteResponse{}, nil
	}

	if err := s.replicateDelete(ctx, req.Key); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &lsmkvv1.DeleteResponse{}, nil
}

func (s *Server) forwardPutIfNeeded(ctx context.Context, req *lsmkvv1.PutRequest) (bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return false, err
	}
	if !shouldForward {
		return false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	if err := client.ForwardPut(ctx, req.Key, req.Value); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) forwardGetIfNeeded(ctx context.Context, req *lsmkvv1.GetRequest) (*lsmkvv1.GetResponse, bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return nil, false, err
	}
	if !shouldForward {
		return nil, false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return nil, false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	value, err := client.ForwardGet(ctx, req.Key)
	if err != nil {
		return nil, false, err
	}
	return &lsmkvv1.GetResponse{Value: value}, true, nil
}

func (s *Server) forwardDeleteIfNeeded(ctx context.Context, req *lsmkvv1.DeleteRequest) (bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return false, err
	}
	if !shouldForward {
		return false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	if err := client.ForwardDelete(ctx, req.Key); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) forwardTarget(key []byte) (ring.Node, bool, error) {
	if s.rt == nil || s.rt.Coordinator == nil {
		return ring.Node{}, false, nil
	}

	target, ok := s.rt.Coordinator.CoordinatorNode(key)
	if !ok {
		return ring.Node{}, false, status.Error(codes.Internal, "no coordinator found")
	}

	if target.ID == s.rt.Config.NodeID {
		return target, false, nil
	}
	if target.Addr == "" {
		return ring.Node{}, false, status.Error(codes.Internal, "coordinator address is empty")
	}

	return target, true, nil
}

func toGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, lsm.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, lsm.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, lsm.ErrTooManyImmutables), errors.Is(err, lsm.ErrWriteStall):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, lsm.ErrStoreClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, lsm.ErrCorruptionDetected):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, lsm.ErrNotImplemented):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, lsm.ErrIOFailure):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
