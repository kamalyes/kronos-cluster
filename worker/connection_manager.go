/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 10:00:00
 * @FilePath: \go-distributed\worker\connection_manager.go
 * @Description: Worker 连接管理器 - 管理与 Master 的连接生命周期
 *
 * 核心职责:
 *   - 维护连接状态机（Disconnected → Connecting → Connected → Ready → Draining）
 *   - 断线自动重连（指数退避 + 抖动策略）
 *   - 优雅关闭（Drain → 等待任务完成 → Disconnect）
 *   - 连接事件通知
 *
 * 连接状态机:
 *   Disconnected → Connecting → Connected → Ready → Draining → Disconnected
 *                      ↘ Reconnecting → Connected / Disconnected
 *
 * 重连策略:
 *   初始间隔 1s → 2s → 4s → 8s → 16s → 30s（最大）
 *   添加 ±25% 随机抖动防止惊群效应
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-distributed/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"time"
)

// ConnectionManager 连接管理器 - 管理 Worker 与 Master 的连接生命周期
type ConnectionManager struct {
	transport       transport.WorkerTransport       // 传输层
	reconnectPolicy *common.ReconnectPolicy         // 重连策略
	state           common.ConnectionState          // 当前连接状态
	eventHandlers   []common.ConnectionEventHandler // 事件回调列表
	logger          logger.ILogger                  // 日志
	running         *syncx.Bool                     // 运行状态
	cancelFunc      context.CancelFunc              // 取消函数
	mu              *syncx.RWLock                   // 保护状态变更
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager(
	tp transport.WorkerTransport,
	policy *common.ReconnectPolicy,
	log logger.ILogger,
) *ConnectionManager {
	if policy == nil {
		policy = common.DefaultReconnectPolicy()
	}

	return &ConnectionManager{
		transport:       tp,
		reconnectPolicy: policy,
		state:           common.ConnectionStateDisconnected,
		logger:          log,
		running:         syncx.NewBool(false),
		mu:              syncx.NewRWLock(),
	}
}

// Connect 建立与 Master 的连接
func (cm *ConnectionManager) Connect(ctx context.Context) error {
	if err := cm.transitionState(common.ConnectionStateConnecting); err != nil {
		return err
	}

	if err := cm.transport.Connect(ctx); err != nil {
		cm.setState(common.ConnectionStateDisconnected)
		return errorx.WrapError(common.ErrFailedConnectMaster, err)
	}

	cm.setState(common.ConnectionStateConnected)
	cm.emitEvent(cm.state)

	cm.logger.InfoContextKV(ctx, "Connected to master")
	return nil
}

// MarkReady 标记连接为就绪状态（注册成功后调用）
func (cm *ConnectionManager) MarkReady() error {
	if err := cm.transitionState(common.ConnectionStateReady); err != nil {
		return err
	}

	cm.emitEvent(cm.state)
	cm.logger.InfoKV("Connection ready")
	return nil
}

// Disconnect 优雅断开连接
// drainTimeout: 排空等待超时（0=不等待直接断开）
func (cm *ConnectionManager) Disconnect(drainTimeout time.Duration) error {
	if cm.GetState() == common.ConnectionStateDisconnected {
		return nil
	}

	if drainTimeout > 0 {
		if err := cm.startDrain(drainTimeout); err != nil {
			cm.logger.WarnKV("Drain failed, forcing disconnect", "error", err)
		}
	}

	cm.setState(common.ConnectionStateDisconnected)
	cm.emitEvent(cm.state)

	if err := cm.transport.Close(); err != nil {
		cm.logger.WarnKV("Failed to close transport", "error", err)
	}

	cm.logger.InfoKV("Disconnected from master")
	return nil
}

// StartReconnectLoop 启动自动重连循环
func (cm *ConnectionManager) StartReconnectLoop(ctx context.Context) {
	if !cm.running.CAS(false, true) {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	cm.cancelFunc = cancel

	go cm.reconnectLoop(ctx)
}

// StopReconnectLoop 停止自动重连循环
func (cm *ConnectionManager) StopReconnectLoop() {
	if !cm.running.CAS(true, false) {
		return
	}

	if cm.cancelFunc != nil {
		cm.cancelFunc()
	}
}

// OnEvent 注册连接事件回调
func (cm *ConnectionManager) OnEvent(handler common.ConnectionEventHandler) {
	cm.eventHandlers = append(cm.eventHandlers, handler)
}

// GetState 获取当前连接状态
func (cm *ConnectionManager) GetState() common.ConnectionState {
	return syncx.WithRLockReturnValue(cm.mu, func() common.ConnectionState {
		return cm.state
	})
}

// IsReady 判断连接是否就绪
func (cm *ConnectionManager) IsReady() bool {
	return cm.GetState() == common.ConnectionStateReady
}

// IsConnected 判断是否已连接
func (cm *ConnectionManager) IsConnected() bool {
	state := cm.GetState()
	return state == common.ConnectionStateConnected || state == common.ConnectionStateReady
}

// startDrain 开始排空（等待运行中任务完成）
func (cm *ConnectionManager) startDrain(timeout time.Duration) error {
	if err := cm.transitionState(common.ConnectionStateDraining); err != nil {
		return err
	}

	cm.emitEvent(cm.state)
	cm.logger.InfoKV("Starting drain", "timeout", timeout)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cm.IsReady() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	remaining := 0
	return fmt.Errorf(common.ErrDrainTimeout, remaining)
}

// reconnectLoop 重连循环
func (cm *ConnectionManager) reconnectLoop(ctx context.Context) {
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if cm.IsConnected() {
			time.Sleep(1 * time.Second)
			attempt = 0
			continue
		}

		if cm.reconnectPolicy.IsExhausted(attempt) {
			cm.logger.ErrorContextKV(ctx, "Reconnect attempts exhausted",
				"attempts", attempt)
			cm.setState(common.ConnectionStateDisconnected)
			cm.emitEvent(cm.state)
			return
		}

		cm.setState(common.ConnectionStateReconnecting)
		cm.emitEvent(cm.state)

		interval := cm.reconnectPolicy.NextInterval(attempt)
		cm.logger.InfoContextKV(ctx, "Attempting reconnect",
			"attempt", attempt+1,
			"interval", interval)

		time.Sleep(interval)

		if err := cm.Connect(ctx); err != nil {
			cm.logger.WarnContextKV(ctx, "Reconnect failed",
				"attempt", attempt+1,
				"error", err)
			attempt++
			continue
		}

		cm.logger.InfoContextKV(ctx, "Reconnected successfully",
			"attempts", attempt+1)
		attempt = 0
	}
}

// setState 设置连接状态
func (cm *ConnectionManager) setState(state common.ConnectionState) {
	syncx.WithLock(cm.mu, func() {
		cm.state = state
	})
}

// transitionState 安全地转换连接状态
func (cm *ConnectionManager) transitionState(target common.ConnectionState) error {
	current := cm.GetState()
	if err := current.ValidateTransition(target); err != nil {
		return err
	}
	cm.setState(target)
	return nil
}

// emitEvent 触发连接事件
func (cm *ConnectionManager) emitEvent(state common.ConnectionState) {
	for _, handler := range cm.eventHandlers {
		handler(state)
	}
}
