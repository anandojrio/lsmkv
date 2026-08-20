package node

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readReplicaResult struct {
	value []byte
	found bool
	err   error
}

func (s *Server) replicateGet(ctx context.Context, key []byte) ([]byte, error) {
	if s.rt == nil || s.rt.Coordinator == nil {
		value, found, err := s.store.Get(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return value, nil
	}

	prefs := s.rt.Coordinator.PreferenceList(key)
	if len(prefs) == 0 {
		return nil, fmt.Errorf("empty preference list")
	}

	required := s.rt.ReadQuorum
	if required <= 0 {
		required = 1
	}

	results := make(chan readReplicaResult, len(prefs))

	var wg sync.WaitGroup
	for _, replica := range prefs {
		replica := replica
		wg.Add(1)

		go func() {
			defer wg.Done()

			if replica.ID == s.rt.Config.NodeID {
				value, found, err := s.store.Get(key)
				if err != nil {
					results <- readReplicaResult{err: toGRPCError(err)}
					return
				}
				results <- readReplicaResult{
					value: value,
					found: found,
				}
				return
			}

			client, err := Dial(ctx, replica.Addr, DefaultDialTimeout, DefaultRPCTimeout)
			if err != nil {
				results <- readReplicaResult{
					err: status.Errorf(codes.Unavailable, "dial replica %s: %v", replica.Addr, err),
				}
				return
			}
			defer func() { _ = client.Close() }()

			value, err := client.ForwardGet(ctx, key)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					results <- readReplicaResult{found: false}
					return
				}
				results <- readReplicaResult{err: err}
				return
			}

			results <- readReplicaResult{
				value: value,
				found: true,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		success  int
		result   []byte
		firstErr error
	)

	for {
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())

		case res, ok := <-results:
			if !ok {
				if success >= required {
					if result != nil {
						return result, nil
					}
					return nil, status.Error(codes.NotFound, "key not found")
				}

				if firstErr != nil {
					return nil, status.Errorf(codes.Unavailable, "read quorum not reached: %v", firstErr)
				}

				return nil, status.Errorf(
					codes.Unavailable,
					"read quorum not reached: success=%d required=%d",
					success,
					required,
				)
			}

			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				continue
			}

			success++
			if res.found && result == nil {
				result = append([]byte(nil), res.value...)
			}

			if success >= required {
				if result != nil {
					return result, nil
				}
				return nil, status.Error(codes.NotFound, "key not found")
			}
		}
	}
}
