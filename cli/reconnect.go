/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 15:06:01
 * @FilePath: \go-distributed\cli\reconnect.go
 * @Description: CLI 客户端 - 自动重连与健康检查
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

// healthCheckLoop 健康检查循环
func (c *Client) healthCheckLoop(ctx context.Context) {
	syncx.NewEventLoop(ctx).
		OnTicker(c.healthInterval, func() {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := c.Ping(pingCtx); err != nil {
				c.logger.WarnKV("Health check failed", "error", err)
				go c.reconnectLoop(ctx)
			}
			cancel()
		}).
		Run()
}

// reconnectLoop 重连循环
func (c *Client) reconnectLoop(ctx context.Context) {
	c.mu.Lock()
	if c.state == common.ConnectionStateConnected || c.state == common.ConnectionStateReady {
		c.setState(common.ConnectionStateReconnecting)
	}
	currentState := c.state
	c.mu.Unlock()

	if currentState != common.ConnectionStateReconnecting {
		return
	}

	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if c.reconnectPolicy.IsExhausted(attempt) {
			c.logger.ErrorKV("Reconnect exhausted", "attempts", attempt)
			c.setState(common.ConnectionStateDisconnected)
			return
		}

		interval := c.reconnectPolicy.NextInterval(attempt)
		c.logger.InfoKV("Reconnecting", "attempt", attempt+1, "interval", interval)

		time.Sleep(interval)

		if err := c.reconnect(); err != nil {
			c.logger.WarnKV("Reconnect failed", "attempt", attempt+1, "error", err)
			attempt++
			continue
		}

		c.logger.InfoKV("Reconnected successfully")
		return
	}
}

// reconnect 重新建立连接
func (c *Client) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.client = nil
	}

	c.setState(common.ConnectionStateReconnecting)

	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, c.dialOpts...)

	conn, err := grpc.NewClient(c.address, dialOpts...)
	if err != nil {
		return fmt.Errorf("reconnect failed: %w", err)
	}

	c.conn = conn
	c.client = pb.NewAdminServiceClient(conn)
	c.setState(common.ConnectionStateConnected)
	c.setState(common.ConnectionStateReady)

	return nil
}

// handleRPCErr 处理 RPC 错误，检测连接断开
func (c *Client) handleRPCErr(err error) {
	if err != nil && c.running.Load() {
		c.logger.WarnKV("RPC error detected, connection may be lost", "error", err)
		c.setState(common.ConnectionStateReconnecting)
		go c.reconnectLoop(context.Background())
	}
}
