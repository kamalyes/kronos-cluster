/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 18:09:18
 * @FilePath: \go-distributed\cli\client.go
 * @Description: CLI 客户端 - 结构定义、配置选项、连接创建与关闭
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"
)

// Client CLI 管理客户端 - 通过 gRPC 连接 Master AdminService
type Client struct {
	address         string                          // Master gRPC 地址
	conn            *grpc.ClientConn                // gRPC 连接
	client          pb.AdminServiceClient           // AdminService 客户端
	state           common.ConnectionState          // 当前连接状态
	reconnectPolicy *common.ReconnectPolicy         // 重连策略
	healthInterval  time.Duration                   // 健康检查间隔（0=不检查）
	eventHandlers   []common.ConnectionEventHandler // 事件回调
	dialOpts        []grpc.DialOption               // gRPC 拨号选项
	logger          logger.ILogger                  // 日志

	mu         sync.Mutex
	running    *syncx.Bool
	cancelFunc context.CancelFunc
}

// ClientOption 客户端配置函数
type ClientOption func(*Client)

// WithReconnectPolicy 设置重连策略
func WithReconnectPolicy(policy *common.ReconnectPolicy) ClientOption {
	return func(c *Client) {
		c.reconnectPolicy = policy
	}
}

// WithHealthCheckInterval 设置健康检查间隔
func WithHealthCheckInterval(interval time.Duration) ClientOption {
	return func(c *Client) {
		c.healthInterval = interval
	}
}

// WithEventHandler 添加连接事件回调
func WithEventHandler(handler common.ConnectionEventHandler) ClientOption {
	return func(c *Client) {
		c.eventHandlers = append(c.eventHandlers, handler)
	}
}

// WithLogger 设置日志
func WithLogger(log logger.ILogger) ClientOption {
	return func(c *Client) {
		if log != nil {
			c.logger = log
		}
	}
}

// WithDialOptions 设置额外的 gRPC 拨号选项
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *Client) {
		c.dialOpts = opts
	}
}

// NewClient 创建 CLI 客户端
// address: Master gRPC 地址，格式 "host:port"
func NewClient(address string, opts ...ClientOption) (*Client, error) {
	c := &Client{
		address:         address,
		state:           common.ConnectionStateDisconnected,
		reconnectPolicy: common.DefaultReconnectPolicy(),
		healthInterval:  0,
		logger:          logger.NewEmptyLogger(),
		running:         syncx.NewBool(false),
	}

	for _, opt := range opts {
		opt(c)
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	return c, nil
}

// connect 建立 gRPC 连接
func (c *Client) connect() error {
	c.setState(common.ConnectionStateConnecting)

	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, c.dialOpts...)

	conn, err := grpc.NewClient(c.address, dialOpts...)
	if err != nil {
		c.setState(common.ConnectionStateDisconnected)
		return fmt.Errorf("failed to connect to master at %s: %w", c.address, err)
	}

	c.conn = conn
	c.client = pb.NewAdminServiceClient(conn)
	c.setState(common.ConnectionStateConnected)
	c.setState(common.ConnectionStateReady)

	return nil
}

// Close 关闭客户端连接
func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		c.setState(common.ConnectionStateDisconnected)
		return err
	}
	return nil
}
