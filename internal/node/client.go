package node

import (
	"context"
	"fmt"
	"time"

	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultDialTimeout = 3 * time.Second
	DefaultRPCTimeout  = 3 * time.Second
)

type Client struct {
	addr       string
	rpcTimeout time.Duration
	conn       *grpc.ClientConn
	client     lsmkvv1.KVServiceClient
}

func Dial(ctx context.Context, addr string, dialTimeout time.Duration, rpcTimeout time.Duration) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr cannot be empty")
	}
	if dialTimeout <= 0 {
		dialTimeout = DefaultDialTimeout
	}
	if rpcTimeout <= 0 {
		rpcTimeout = DefaultRPCTimeout
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return &Client{
		addr:       addr,
		rpcTimeout: rpcTimeout,
		conn:       conn,
		client:     lsmkvv1.NewKVServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.put(ctx, key, value, false)
}

func (c *Client) ForwardPut(ctx context.Context, key, value []byte) error {
	return c.put(ctx, key, value, true)
}

func (c *Client) put(ctx context.Context, key, value []byte, forwarded bool) error {
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	_, err := c.client.Put(callCtx, &lsmkvv1.PutRequest{
		Key:       key,
		Value:     value,
		Forwarded: forwarded,
	})
	return err
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, false)
}

func (c *Client) ForwardGet(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, true)
}

func (c *Client) get(ctx context.Context, key []byte, forwarded bool) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	resp, err := c.client.Get(callCtx, &lsmkvv1.GetRequest{
		Key:       key,
		Forwarded: forwarded,
	})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.delete(ctx, key, false)
}

func (c *Client) ForwardDelete(ctx context.Context, key []byte) error {
	return c.delete(ctx, key, true)
}

func (c *Client) delete(ctx context.Context, key []byte, forwarded bool) error {
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	_, err := c.client.Delete(callCtx, &lsmkvv1.DeleteRequest{
		Key:       key,
		Forwarded: forwarded,
	})
	return err
}
