/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 16:25:56
 * @FilePath: \go-distributed\common\errors.go
 * @Description: 统一错误信息常量定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

// 节点相关错误
const (
	ErrNodeNotFound          = "node %s not found"          // 节点未找到
	ErrNodeAlreadyRegistered = "node %s already registered" // 节点已注册
)

// 配置相关错误
const (
	ErrConfigCannotNil   = "config cannot be nil"        // 配置不能为空
	ErrNodeConverterNil  = "nodeConverter cannot be nil" // 节点转换器不能为空
	ErrInitInfoCannotNil = "initInfo cannot be nil"      // 初始化函数不能为空
)

// 运行状态错误
const (
	ErrAlreadyRunning   = "%s is already running" // 已在运行中
	ErrNotRunning       = "%s is not running"     // 未在运行
	ErrAlreadyConnected = "%s already connected"  // 已连接
)

// 传输层错误
const (
	ErrUnsupportedTransport  = "unsupported transport type: %s" // 不支持的传输类型
	ErrFailedCreateTransport = "failed to create transport"     // 创建传输层失败
	ErrFailedStartTransport  = "failed to start transport"      // 启动传输层失败
	ErrFailedConnectMaster   = "failed to connect to master"    // 连接 Master 失败
	ErrFailedConnectRedis    = "failed to connect to redis"     // 连接 Redis 失败
	ErrNotConnectedToMaster  = "not connected to master"        // 未连接到 Master
)

// 注册/心跳错误
const (
	ErrRegisterFailed       = "register failed"           // 注册失败
	ErrRegistrationRejected = "registration rejected: %s" // 注册被拒绝
	ErrUnregisterFailed     = "unregister failed"         // 注销失败
	ErrUnregisterRejected   = "unregister rejected: %s"   // 注销被拒绝
	ErrHeartbeatFailed      = "heartbeat failed"          // 心跳失败
)

// 序列化/发布错误
const (
	ErrFailedMarshal = "failed to marshal %s data" // 序列化失败
	ErrFailedPublish = "failed to publish %s"      // 发布失败
	ErrFailedListen  = "failed to listen"          // 监听失败
)

// 令牌错误
const (
	ErrInvalidAlgorithm = "invalid algorithm" // 无效算法
	ErrInvalidToken     = "invalid token"     // 无效令牌
)

// 任务相关错误
const (
	ErrTaskNotFound        = "task %s not found"                              // 任务未找到
	ErrTaskStateTerminal   = "task state %s is terminal"                      // 任务状态为终态
	ErrTaskStateTransition = "task state transition %s to %s"                 // 任务状态转换非法
	ErrTaskDispatchFailed  = "failed to dispatch task %s to %s"               // 任务下发失败
	ErrTaskRejected        = "task %s rejected by worker %s"                  // 任务被 Worker 拒绝
	ErrTaskTimeout         = "task %s timed out"                              // 任务超时
	ErrTaskHandlerNotFound = "task handler not found for type %s"             // 任务处理器未找到
	ErrTaskTypeInvalid     = "invalid task type: %s"                          // 无效的任务类型
	ErrTaskAlreadyExists   = "task %s already exists"                         // 任务已存在
	ErrWorkerConnNotFound  = "worker connection not found for node %s"        // 找不到Worker连接
	ErrWorkerChannelFull   = "failed to send task to worker %s: channel full" // Worker通道已满
)

// 连接生命周期错误
const (
	ErrConnectionNotReady     = "connection not ready"                  // 连接未就绪
	ErrConnectionDisconnected = "connection disconnected"               // 连接已断开
	ErrReconnectExhausted     = "reconnect attempts exhausted for %s"   // 重连次数耗尽
	ErrDrainTimeout           = "drain timeout, %d tasks still running" // 排空超时
)
