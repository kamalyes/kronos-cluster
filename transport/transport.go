/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 12:06:37
 * @Description: 传输层核心接口定义 - 定义 Master/Worker 之间的通信行为
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"context"
	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
)

// RegistrationResult 注册结果
type RegistrationResult struct {
	Success           bool
	Message           string
	Token             string
	HeartbeatInterval int64
}

// HeartbeatResult 心跳结果
type HeartbeatResult struct {
	OK       bool
	Message  string
	Commands []string
}

// MasterTransport Master 端传输层接口
type MasterTransport interface {
	Start(ctx context.Context) error
	Stop() error
	OnRegister(handler func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error))
	OnHeartbeat(handler func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error))
	OnUnregister(handler func(nodeID string, reason string) error)
	OnTaskStatusUpdate(handler func(update *common.TaskStatusUpdate) error)
	DispatchTask(ctx context.Context, nodeID string, task *common.TaskInfo) error
	CancelTask(ctx context.Context, nodeID string, taskID string) error
	IsNodeConnected(nodeID string) bool
}

// WorkerTransport Worker 端传输层接口
type WorkerTransport interface {
	Connect(ctx context.Context) error
	Close() error
	Register(ctx context.Context, nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error)
	Heartbeat(ctx context.Context, nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error)
	Unregister(ctx context.Context, nodeID string, reason string) error
	OnTaskDispatched(handler func(task *common.TaskInfo) (*common.DispatchResult, error))
	OnTaskCancelled(handler func(taskID string) error)
	ReportTaskStatus(ctx context.Context, update *common.TaskStatusUpdate) error
	ConnectStream(ctx context.Context) error
}

// TransportFactory 传输层工厂接口
type TransportFactory interface {
	CreateMasterTransport(config *common.MasterConfig, log logger.ILogger) (MasterTransport, error)
	CreateWorkerTransport(config *common.WorkerConfig, log logger.ILogger) (WorkerTransport, error)
	Type() common.TransportType
}
