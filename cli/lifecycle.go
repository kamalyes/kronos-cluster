/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 00:00:00
 * @FilePath: \go-distributed\cli\lifecycle.go
 * @Description: CLI 客户端 - 连接生命周期管理（启停、状态查询、健康探测）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"time"
)

// Start 启动客户端生命周期管理（健康检查 + 自动重连）
func (c *Client) Start(ctx context.Context) error {
	if !c.running.CAS(false, true) {
		return fmt.Errorf("client already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	c.cancelFunc = cancel

	if c.healthInterval > 0 {
		go c.healthCheckLoop(ctx)
	}

	c.logger.InfoKV("CLI client started", "address", c.address)
	return nil
}

// Stop 停止客户端
func (c *Client) Stop() error {
	if !c.running.CAS(true, false) {
		return fmt.Errorf("client not running")
	}

	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	c.setState(common.ConnectionStateDraining)
	c.Close()
	c.setState(common.ConnectionStateDisconnected)

	c.logger.InfoKV("CLI client stopped")
	return nil
}

// GetState 获取当前连接状态
func (c *Client) GetState() common.ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// IsReady 判断客户端是否就绪
func (c *Client) IsReady() bool {
	return c.GetState() == common.ConnectionStateReady
}

// WaitReady 等待客户端就绪（带超时）
func (c *Client) WaitReady(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("client not ready: %w", ctx.Err())
		case <-ticker.C:
			if c.IsReady() {
				return nil
			}
		}
	}
}

// Ping 检查与 Master 的连接是否正常
func (c *Client) Ping(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client not connected")
	}

	_, err := c.client.GetClusterStats(ctx, &pb.ClusterStatsRequest{})
	if err != nil {
		c.setState(common.ConnectionStateReconnecting)
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

// setState 设置连接状态（带状态机校验和事件通知）
func (c *Client) setState(newState common.ConnectionState) {
	c.mu.Lock()
	oldState := c.state
	if oldState == newState {
		c.mu.Unlock()
		return
	}

	if err := oldState.ValidateTransition(newState); err != nil {
		c.mu.Unlock()
		c.logger.WarnKV("Invalid state transition", "from", oldState, "to", newState, "error", err)
		return
	}

	c.state = newState
	c.mu.Unlock()

	c.logger.DebugKV("Connection state changed", "from", oldState, "to", newState)

	for _, handler := range c.eventHandlers {
		handler(newState)
	}
}
