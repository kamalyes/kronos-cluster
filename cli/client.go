/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 18:09:18
 * @FilePath: \go-distributed\cli\client.go
 * @Description: CLI 客户端 - 结构定义、配置选项、连接创建与关闭
 *
 * 支持两种连接方式:
 *   1. 直接指定地址: NewClient("localhost:50051", ...)
 *   2. 控制平面配置: NewClientFromControlPlane(config, ...)
 *
 * 当启用安全认证时，CLI 会自动在 gRPC 元数据中携带认证令牌
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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
	controlPlane    *common.ControlPlaneConfig      // 控制平面配置
	authToken       string                          // 认证令牌
	enableAuth      bool                            // 是否启用认证

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

// WithAuthToken 设置认证令牌
func WithAuthToken(token string) ClientOption {
	return func(c *Client) {
		c.authToken = token
	}
}

// WithControlPlane 设置控制平面配置
func WithControlPlane(config *common.ControlPlaneConfig) ClientOption {
	return func(c *Client) {
		c.controlPlane = config
		if config != nil {
			c.enableAuth = config.EnableAuth
			c.authToken = config.Token
		}
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

// NewClientFromControlPlane 从控制平面配置创建 CLI 客户端
// 类似 Kubernetes 的 kubeconfig 方式
func NewClientFromControlPlane(cpConfig *common.ControlPlaneConfig, opts ...ClientOption) (*Client, error) {
	if cpConfig == nil {
		return nil, fmt.Errorf(common.ErrControlPlaneConfigNil)
	}

	opts = append(opts, WithControlPlane(cpConfig))

	return NewClient(cpConfig.ServerAddr, opts...)
}

// NewClientFromConfigFile 从配置文件创建 CLI 客户端
func NewClientFromConfigFile(configPath string, opts ...ClientOption) (*Client, error) {
	cpFile, err := common.LoadControlPlaneConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedLoadConfig, err)
	}

	cpConfig, err := cpFile.ResolveCurrentConfig()
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedResolveConfig, err)
	}

	return NewClientFromControlPlane(cpConfig, opts...)
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
		return fmt.Errorf(common.ErrFailedConnectMasterAt, c.address, err)
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

// Authenticate 认证并获取令牌
func (c *Client) Authenticate(ctx context.Context, secret, clientID string) (string, error) {
	resp, err := c.client.Authenticate(ctx, &pb.AuthRequest{
		Secret:   secret,
		ClientId: clientID,
	})
	if err != nil {
		return "", fmt.Errorf(common.ErrAuthenticationFailed, err)
	}

	if !resp.Success {
		return "", fmt.Errorf(common.ErrAuthenticationMsgFailed, resp.Message)
	}

	c.authToken = resp.Token
	return resp.Token, nil
}

// withAuth 创建带认证信息的上下文
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.authToken != "" {
		md := metadata.Pairs("authorization", c.authToken)
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// IsAuthEnabled 判断是否启用认证
func (c *Client) IsAuthEnabled() bool {
	return c.enableAuth
}

// GetAuthToken 获取当前认证令牌
func (c *Client) GetAuthToken() string {
	return c.authToken
}

// GetControlPlane 获取控制平面配置
func (c *Client) GetControlPlane() *common.ControlPlaneConfig {
	return c.controlPlane
}
