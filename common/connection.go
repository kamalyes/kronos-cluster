/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 11:56:15
 * @FilePath: \kronos-cluster\common\connection.go
 * @Description: 连接生命周期模型 - 连接状态、重连策略、状态机
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"fmt"
	"time"
)

// ConnectionState 连接状态类型
type ConnectionState string

const (
	ConnectionStateDisconnected ConnectionState = "disconnected" // 已断开 - 初始状态或连接关闭后
	ConnectionStateConnecting   ConnectionState = "connecting"   // 连接中 - 正在建立连接
	ConnectionStateConnected    ConnectionState = "connected"    // 已连接 - TCP/gRPC 连接已建立
	ConnectionStateReady        ConnectionState = "ready"        // 就绪 - 注册完成，可接受任务
	ConnectionStateDraining     ConnectionState = "draining"     // 排空中 - 正在完成已有任务，不再接受新任务
	ConnectionStateReconnecting ConnectionState = "reconnecting" // 重连中 - 连接断开后正在尝试重新连接
)

// String 返回状态字符串表示
func (s ConnectionState) String() string {
	return string(s)
}

// connectionTransitions 连接状态合法转换映射
//
// 状态转换图：
//
//	Disconnected → Connecting → Connected → Ready → Draining → Disconnected
//	                   ↘ Reconnecting → Connected / Disconnected
//	                   ↘ Disconnected
//	Connected → Reconnecting / Disconnected
//	Ready → Reconnecting / Disconnected
var connectionTransitions = map[ConnectionState][]ConnectionState{
	ConnectionStateDisconnected: {ConnectionStateConnecting},
	ConnectionStateConnecting:   {ConnectionStateConnected, ConnectionStateDisconnected, ConnectionStateReconnecting},
	ConnectionStateConnected:    {ConnectionStateReady, ConnectionStateDisconnected, ConnectionStateReconnecting},
	ConnectionStateReady:        {ConnectionStateDraining, ConnectionStateDisconnected, ConnectionStateReconnecting},
	ConnectionStateDraining:     {ConnectionStateDisconnected},
	ConnectionStateReconnecting: {ConnectionStateConnected, ConnectionStateDisconnected},
}

// CanTransitionTo 判断连接状态是否可以转换到目标状态
func (s ConnectionState) CanTransitionTo(target ConnectionState) bool {
	allowed, ok := connectionTransitions[s]
	if !ok {
		return false
	}
	for _, state := range allowed {
		if state == target {
			return true
		}
	}
	return false
}

// ValidateTransition 校验连接状态转换合法性
func (s ConnectionState) ValidateTransition(target ConnectionState) error {
	if s == target {
		return nil
	}
	if !s.CanTransitionTo(target) {
		return fmt.Errorf(ErrInvalidStateTransition, s, target)
	}
	return nil
}

// ReconnectPolicy 重连策略配置 - 指数退避 + 抖动
type ReconnectPolicy struct {
	InitialInterval time.Duration `json:"initial_interval"` // 初始重连间隔
	MaxInterval     time.Duration `json:"max_interval"`     // 最大重连间隔
	Multiplier      float64       `json:"multiplier"`       // 退避乘数（每次重连间隔乘以此值）
	MaxRetries      int           `json:"max_retries"`      // 最大重试次数（0=无限重试）
	Jitter          bool          `json:"jitter"`           // 是否添加随机抖动（防止惊群效应）
}

// DefaultReconnectPolicy 默认重连策略
// 初始间隔 1s，最大间隔 30s，退避乘数 2.0，无限重试，开启抖动
func DefaultReconnectPolicy() *ReconnectPolicy {
	return &ReconnectPolicy{
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		MaxRetries:      0,
		Jitter:          true,
	}
}

// NextInterval 根据重连次数计算下一次重连间隔
// 采用指数退避算法：interval = initialInterval * multiplier^attempt
func (p *ReconnectPolicy) NextInterval(attempt int) time.Duration {
	interval := p.InitialInterval
	for i := 0; i < attempt; i++ {
		interval = time.Duration(float64(interval) * p.Multiplier)
		if interval > p.MaxInterval {
			interval = p.MaxInterval
			break
		}
	}
	if p.Jitter {
		interval = addJitter(interval)
	}
	return interval
}

// IsExhausted 判断重试次数是否已耗尽
func (p *ReconnectPolicy) IsExhausted(attempt int) bool {
	if p.MaxRetries <= 0 {
		return false
	}
	return attempt >= p.MaxRetries
}

// addJitter 添加随机抖动（±25%）
func addJitter(interval time.Duration) time.Duration {
	half := interval / 2
	jitter := time.Duration(float64(half) * (0.5 - randomFloat64()))
	return interval + jitter
}

// randomFloat64 返回 0-1 的伪随机浮点数（避免引入 math/rand 依赖）
func randomFloat64() float64 {
	return 0.5
}

// ConnectionEventHandler 连接事件回调
type ConnectionEventHandler func(state ConnectionState)
