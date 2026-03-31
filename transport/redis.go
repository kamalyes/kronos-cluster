/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 18:58:33
 * @Description: Redis Pub/Sub 传输层实现 - Master/Worker 的 Redis 通信（含任务下发、状态回报）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/redis/go-redis/v9"
)

// Redis 键和通道常量
const (
	redisKeyNodes            = "distributed:nodes"          // 节点信息哈希键
	redisKeyNodePrefix       = "distributed:node:"          // 单节点信息键前缀
	redisKeyHeartbeat        = "distributed:heartbeat:"     // 心跳键前缀
	redisKeyTaskPrefix       = "distributed:task:"          // 任务信息键前缀
	redisChannelRegister     = "distributed:register"       // 注册事件通道
	redisChannelHeartbeat    = "distributed:heartbeat"      // 心跳事件通道
	redisChannelUnregister   = "distributed:unregister"     // 注销事件通道
	redisChannelTaskDispatch = "distributed:task:dispatch:" // 任务下发通道前缀（后接 nodeID）
	redisChannelTaskStatus   = "distributed:task:status"    // 任务状态回报通道
	redisChannelTaskCancel   = "distributed:task:cancel:"   // 任务取消通道前缀（后接 nodeID）
)

// =====================================================================
// Master 端 Redis 传输层
// =====================================================================

// RedisMasterTransport Redis Master 端传输层实现 - 通过 Pub/Sub 接收节点事件和任务状态回报
type RedisMasterTransport struct {
	config                    *common.MasterConfig                                                                             // Master 配置
	redisClient               redis.UniversalClient                                                                            // Redis 客户端
	logger                    logger.ILogger                                                                                   // 日志
	running                   *syncx.Bool                                                                                      // 运行状态
	cancelFunc                context.CancelFunc                                                                               // 取消函数
	registerHandler           func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error)                    // 注册回调
	registerWithSecretHandler func(nodeInfo common.NodeInfo, joinSecret string, extension []byte) (*RegistrationResult, error) // 带密钥的注册回调
	heartbeatHandler          func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error)          // 心跳回调
	unregisterHandler         func(nodeID string, reason string) error                                                         // 注销回调
	taskStatusHandler         func(update *common.TaskStatusUpdate) error                                                      // 任务状态更新回调
}

// NewRedisMasterTransport 创建 Redis Master 传输层
func NewRedisMasterTransport(config *common.MasterConfig, log logger.ILogger) *RedisMasterTransport {
	return &RedisMasterTransport{
		config:  config,
		logger:  log,
		running: syncx.NewBool(false),
	}
}

// Start 启动 Redis Master 传输层服务
// 建立 Redis 连接并订阅注册、心跳、注销、任务状态等频道
func (t *RedisMasterTransport) Start(ctx context.Context) error {
	if !t.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyRunning, "redis master transport")
	}

	t.redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{t.config.RedisAddr},
		Password: t.config.RedisPassword,
		DB:       t.config.RedisDB,
	})

	if err := t.redisClient.Ping(ctx).Err(); err != nil {
		t.running.Store(false)
		return errorx.WrapError(common.ErrFailedConnectRedis, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel

	go t.subscribeChannels(ctx)

	t.logger.InfoKV("Redis master transport started", "redis_addr", t.config.RedisAddr)
	return nil
}

// Stop 停止 Redis Master 传输层服务
// 取消订阅并关闭 Redis 连接
func (t *RedisMasterTransport) Stop() error {
	if !t.running.CAS(true, false) {
		return fmt.Errorf(common.ErrNotRunning, "redis master transport")
	}

	if t.cancelFunc != nil {
		t.cancelFunc()
	}

	if t.redisClient != nil {
		t.redisClient.Close()
	}

	t.logger.Info("Redis master transport stopped")
	return nil
}

// OnRegister 设置节点注册回调
// 当收到 Worker 注册消息时触发
func (t *RedisMasterTransport) OnRegister(handler func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error)) {
	t.registerHandler = handler
}

// OnRegisterWithSecret 设置带密钥的节点注册回调
func (t *RedisMasterTransport) OnRegisterWithSecret(handler func(nodeInfo common.NodeInfo, joinSecret string, extension []byte) (*RegistrationResult, error)) {
	t.registerWithSecretHandler = handler
}

// OnHeartbeat 设置节点心跳回调
// 当收到 Worker 心跳消息时触发
func (t *RedisMasterTransport) OnHeartbeat(handler func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error)) {
	t.heartbeatHandler = handler
}

// OnUnregister 设置节点注销回调
// 当收到 Worker 注销消息时触发
func (t *RedisMasterTransport) OnUnregister(handler func(nodeID string, reason string) error) {
	t.unregisterHandler = handler
}

// OnTaskStatusUpdate 设置任务状态更新回调
// 当收到 Worker 任务状态回报时触发
func (t *RedisMasterTransport) OnTaskStatusUpdate(handler func(update *common.TaskStatusUpdate) error) {
	t.taskStatusHandler = handler
}

// DispatchTask 通过 Redis 向指定 Worker 下发任务
// 将任务信息序列化后发布到节点专属的任务下发频道
func (t *RedisMasterTransport) DispatchTask(ctx context.Context, nodeID string, task *common.TaskInfo) error {
	channel := redisChannelTaskDispatch + nodeID
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshal, "task dispatch")
	}

	if err := t.redisClient.Publish(ctx, channel, string(payload)).Err(); err != nil {
		return fmt.Errorf(common.ErrTaskDispatchFailed, task.ID, nodeID)
	}

	// 将任务信息缓存到 Redis，用于故障恢复和状态追踪
	taskData, _ := json.Marshal(task)
	t.redisClient.Set(ctx, redisKeyTaskPrefix+task.ID, string(taskData), 24*time.Hour)

	return nil
}

// CancelTask 通过 Redis 向指定 Worker 发送取消任务命令
// 将取消请求发布到节点专属的任务取消频道
func (t *RedisMasterTransport) CancelTask(ctx context.Context, nodeID string, taskID string) error {
	channel := redisChannelTaskCancel + nodeID
	data := struct {
		TaskID string `json:"task_id"`
		Reason string `json:"reason"`
	}{
		TaskID: taskID,
		Reason: "cancelled by master",
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshal, "task cancel")
	}

	if err := t.redisClient.Publish(ctx, channel, string(payload)).Err(); err != nil {
		return fmt.Errorf(common.ErrTaskDispatchFailed, taskID, nodeID)
	}

	return nil
}

// IsNodeConnected Redis Pub/Sub 模式无需维护流连接，始终返回 true
func (t *RedisMasterTransport) IsNodeConnected(nodeID string) bool {
	return true
}

// subscribeChannels 订阅 Redis 频道，监听节点事件和任务状态
func (t *RedisMasterTransport) subscribeChannels(ctx context.Context) {
	sub := t.redisClient.Subscribe(ctx, redisChannelRegister, redisChannelHeartbeat, redisChannelUnregister, redisChannelTaskStatus)
	defer sub.Close()

	syncx.NewEventLoop(ctx).
		OnChannel(sub.Channel(), func(msg *redis.Message) {
			t.handleMessage(ctx, msg)
		}).
		Run()
}

// handleMessage 根据频道类型分发消息处理
func (t *RedisMasterTransport) handleMessage(ctx context.Context, msg *redis.Message) {
	switch msg.Channel {
	case redisChannelRegister:
		t.handleRegisterMessage(ctx, msg)
	case redisChannelHeartbeat:
		t.handleHeartbeatMessage(ctx, msg)
	case redisChannelUnregister:
		t.handleUnregisterMessage(ctx, msg)
	case redisChannelTaskStatus:
		t.handleTaskStatusMessage(ctx, msg)
	}
}

// handleRegisterMessage 处理节点注册消息
// 反序列化注册数据，调用注册回调，成功后将节点信息写入 Redis 哈希
func (t *RedisMasterTransport) handleRegisterMessage(ctx context.Context, msg *redis.Message) {
	if t.registerHandler == nil {
		return
	}

	var data struct {
		NodeInfo  *common.BaseNodeInfo `json:"node_info"`
		Extension []byte               `json:"extension"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
		t.logger.ErrorContextKV(ctx, "Failed to unmarshal register message", "error", err)
		return
	}

	if data.NodeInfo == nil {
		return
	}

	result, err := t.registerHandler(data.NodeInfo, data.Extension)
	if err != nil {
		t.logger.ErrorContextKV(ctx, "Register handler error", "error", err)
		return
	}

	if result.Success {
		nodeData, _ := json.Marshal(data.NodeInfo)
		t.redisClient.HSet(ctx, redisKeyNodes, data.NodeInfo.ID, string(nodeData))
		t.redisClient.Set(ctx, redisKeyHeartbeat+data.NodeInfo.ID, "1", t.config.HeartbeatTimeout)
	}
}

// handleHeartbeatMessage 处理节点心跳消息
// 反序列化心跳数据，调用心跳回调，成功后刷新心跳键的 TTL
func (t *RedisMasterTransport) handleHeartbeatMessage(ctx context.Context, msg *redis.Message) {
	if t.heartbeatHandler == nil {
		return
	}

	var data struct {
		NodeID    string           `json:"node_id"`
		State     common.NodeState `json:"state"`
		Extension []byte           `json:"extension"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
		t.logger.ErrorContextKV(ctx, "Failed to unmarshal heartbeat message", "error", err)
		return
	}

	result, err := t.heartbeatHandler(data.NodeID, data.State, data.Extension)
	if err != nil {
		t.logger.ErrorContextKV(ctx, "Heartbeat handler error", "error", err)
		return
	}

	if result.OK {
		t.redisClient.Set(ctx, redisKeyHeartbeat+data.NodeID, "1", t.config.HeartbeatTimeout)
	}
}

// handleUnregisterMessage 处理节点注销消息
// 反序列化注销数据，调用注销回调，清理 Redis 中的节点信息和心跳键
func (t *RedisMasterTransport) handleUnregisterMessage(ctx context.Context, msg *redis.Message) {
	if t.unregisterHandler == nil {
		return
	}

	var data struct {
		NodeID string `json:"node_id"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
		t.logger.ErrorContextKV(ctx, "Failed to unmarshal unregister message", "error", err)
		return
	}

	if err := t.unregisterHandler(data.NodeID, data.Reason); err != nil {
		t.logger.ErrorContextKV(ctx, "Unregister handler error", "error", err)
		return
	}

	t.redisClient.HDel(ctx, redisKeyNodes, data.NodeID)
	t.redisClient.Del(ctx, redisKeyHeartbeat+data.NodeID)
}

// handleTaskStatusMessage 处理任务状态回报消息
// 反序列化任务状态数据，调用任务状态回调
func (t *RedisMasterTransport) handleTaskStatusMessage(ctx context.Context, msg *redis.Message) {
	if t.taskStatusHandler == nil {
		return
	}

	var update common.TaskStatusUpdate
	if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
		t.logger.ErrorContextKV(ctx, "Failed to unmarshal task status message", "error", err)
		return
	}

	if err := t.taskStatusHandler(&update); err != nil {
		t.logger.ErrorContextKV(ctx, "Task status handler error", "error", err)
	}
}

// =====================================================================
// Worker 端 Redis 传输层
// =====================================================================

// RedisWorkerTransport Redis Worker 端传输层实现 - 通过 Pub/Sub 发送节点事件并接收任务
type RedisWorkerTransport struct {
	config              *common.WorkerConfig                                        // Worker 配置
	redisClient         redis.UniversalClient                                       // Redis 客户端
	logger              logger.ILogger                                              // 日志
	running             *syncx.Bool                                                 // 运行状态
	cancelFunc          context.CancelFunc                                          // 取消函数
	taskDispatchHandler func(task *common.TaskInfo) (*common.DispatchResult, error) // 任务下发回调
	taskCancelHandler   func(taskID string) error                                   // 任务取消回调
}

// NewRedisWorkerTransport 创建 Redis Worker 传输层
func NewRedisWorkerTransport(config *common.WorkerConfig, log logger.ILogger) *RedisWorkerTransport {
	return &RedisWorkerTransport{
		config:  config,
		logger:  log,
		running: syncx.NewBool(false),
	}
}

// Connect 连接到 Redis 并订阅任务频道
// 建立 Redis 连接后，自动订阅当前节点的任务下发和取消频道
func (t *RedisWorkerTransport) Connect(ctx context.Context) error {
	if !t.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyConnected, "redis worker transport")
	}

	t.redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{t.config.RedisAddr},
		Password: t.config.RedisPassword,
		DB:       t.config.RedisDB,
	})

	if err := t.redisClient.Ping(ctx).Err(); err != nil {
		t.running.Store(false)
		return errorx.WrapError(common.ErrFailedConnectRedis, err)
	}

	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel

	go t.subscribeTaskChannels(ctx)

	t.logger.InfoContextKV(ctx, "Redis worker transport connected", "redis_addr", t.config.RedisAddr)
	return nil
}

// Close 关闭 Redis 连接
// 取消订阅并关闭 Redis 连接
func (t *RedisWorkerTransport) Close() error {
	if !t.running.CAS(true, false) {
		return nil
	}

	if t.cancelFunc != nil {
		t.cancelFunc()
	}

	if t.redisClient != nil {
		t.redisClient.Close()
	}

	t.logger.Info("Redis worker transport closed")
	return nil
}

// Register 通过 Redis Pub/Sub 注册当前 Worker 节点
// 将节点信息发布到注册频道，并写入 Redis 哈希和心跳键
func (t *RedisWorkerTransport) Register(ctx context.Context, nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error) {
	data := struct {
		NodeInfo  common.NodeInfo `json:"node_info"`
		Extension []byte          `json:"extension"`
	}{
		NodeInfo:  nodeInfo,
		Extension: extension,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedMarshal, "register")
	}

	if err := t.redisClient.Publish(ctx, redisChannelRegister, string(payload)).Err(); err != nil {
		return nil, fmt.Errorf(common.ErrFailedPublish, "register")
	}

	nodeData, _ := json.Marshal(nodeInfo)
	t.redisClient.HSet(ctx, redisKeyNodes, nodeInfo.GetID(), string(nodeData))
	t.redisClient.Set(ctx, redisKeyHeartbeat+nodeInfo.GetID(), "1", 30*time.Second)

	return &RegistrationResult{
		Success:           true,
		Message:           "registered via redis",
		HeartbeatInterval: 5,
	}, nil
}

// RegisterWithSecret 通过 Redis Pub/Sub 注册当前 Worker 节点（携带加入密钥）
func (t *RedisWorkerTransport) RegisterWithSecret(ctx context.Context, nodeInfo common.NodeInfo, joinSecret string, extension []byte) (*RegistrationResult, error) {
	data := struct {
		NodeInfo   common.NodeInfo `json:"node_info"`
		JoinSecret string          `json:"join_secret"`
		Extension  []byte          `json:"extension"`
	}{
		NodeInfo:   nodeInfo,
		JoinSecret: joinSecret,
		Extension:  extension,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedMarshal, "register")
	}

	if err := t.redisClient.Publish(ctx, redisChannelRegister, string(payload)).Err(); err != nil {
		return nil, fmt.Errorf(common.ErrFailedPublish, "register")
	}

	nodeData, _ := json.Marshal(nodeInfo)
	t.redisClient.HSet(ctx, redisKeyNodes, nodeInfo.GetID(), string(nodeData))
	t.redisClient.Set(ctx, redisKeyHeartbeat+nodeInfo.GetID(), "1", 30*time.Second)

	return &RegistrationResult{
		Success:           true,
		Message:           "registered via redis",
		HeartbeatInterval: 5,
	}, nil
}

// Heartbeat 通过 Redis Pub/Sub 发送心跳
// 将心跳数据发布到心跳频道，并刷新心跳键的 TTL
func (t *RedisWorkerTransport) Heartbeat(ctx context.Context, nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error) {
	data := struct {
		NodeID    string           `json:"node_id"`
		State     common.NodeState `json:"state"`
		Extension []byte           `json:"extension"`
	}{
		NodeID:    nodeID,
		State:     state,
		Extension: extension,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedMarshal, "heartbeat")
	}

	if err := t.redisClient.Publish(ctx, redisChannelHeartbeat, string(payload)).Err(); err != nil {
		return nil, fmt.Errorf(common.ErrFailedPublish, "heartbeat")
	}

	t.redisClient.Set(ctx, redisKeyHeartbeat+nodeID, "1", 30*time.Second)

	return &HeartbeatResult{
		OK:      true,
		Message: "heartbeat sent via redis",
	}, nil
}

// Unregister 通过 Redis Pub/Sub 注销当前 Worker 节点
// 将注销数据发布到注销频道，并清理 Redis 中的节点信息和心跳键
func (t *RedisWorkerTransport) Unregister(ctx context.Context, nodeID string, reason string) error {
	data := struct {
		NodeID string `json:"node_id"`
		Reason string `json:"reason"`
	}{
		NodeID: nodeID,
		Reason: reason,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshal, "unregister")
	}

	if err := t.redisClient.Publish(ctx, redisChannelUnregister, string(payload)).Err(); err != nil {
		return fmt.Errorf(common.ErrFailedPublish, "unregister")
	}

	t.redisClient.HDel(ctx, redisKeyNodes, nodeID)
	t.redisClient.Del(ctx, redisKeyHeartbeat+nodeID)

	return nil
}

// OnTaskDispatched 设置任务下发回调
// 当收到 Master 通过 Redis 下发的任务时触发
func (t *RedisWorkerTransport) OnTaskDispatched(handler func(task *common.TaskInfo) (*common.DispatchResult, error)) {
	t.taskDispatchHandler = handler
}

// OnTaskCancelled 设置任务取消回调
// 当收到 Master 通过 Redis 发送的取消任务命令时触发
func (t *RedisWorkerTransport) OnTaskCancelled(handler func(taskID string) error) {
	t.taskCancelHandler = handler
}

// ReportTaskStatus 通过 Redis Pub/Sub 上报任务状态
// 将任务状态更新发布到任务状态频道
func (t *RedisWorkerTransport) ReportTaskStatus(ctx context.Context, update *common.TaskStatusUpdate) error {
	payload, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshal, "task status")
	}

	if err := t.redisClient.Publish(ctx, redisChannelTaskStatus, string(payload)).Err(); err != nil {
		return fmt.Errorf(common.ErrFailedPublish, "task status")
	}

	return nil
}

// ConnectStream 打开双向流连接到 Master
// Redis 传输层使用的是 Pub/Sub 模式，不支持双向流，因此返回 nil
func (t *RedisWorkerTransport) ConnectStream(ctx context.Context) error {
	return nil
}

// subscribeTaskChannels 订阅当前节点的任务下发和取消频道
// 频道格式：distributed:task:dispatch:{workerID} 和 distributed:task:cancel:{workerID}
func (t *RedisWorkerTransport) subscribeTaskChannels(ctx context.Context) {
	dispatchChannel := redisChannelTaskDispatch + t.config.WorkerID
	cancelChannel := redisChannelTaskCancel + t.config.WorkerID

	sub := t.redisClient.Subscribe(ctx, dispatchChannel, cancelChannel)
	defer sub.Close()

	syncx.NewEventLoop(ctx).
		OnChannel(sub.Channel(), func(msg *redis.Message) {
			t.handleTaskMessage(msg)
		}).
		Run()
}

// handleTaskMessage 根据频道前缀分发任务消息处理
func (t *RedisWorkerTransport) handleTaskMessage(msg *redis.Message) {
	switch {
	case len(msg.Channel) > len(redisChannelTaskDispatch) && msg.Channel[:len(redisChannelTaskDispatch)] == redisChannelTaskDispatch:
		t.handleTaskDispatchMessage(msg)
	case len(msg.Channel) > len(redisChannelTaskCancel) && msg.Channel[:len(redisChannelTaskCancel)] == redisChannelTaskCancel:
		t.handleTaskCancelMessage(msg)
	}
}

// handleTaskDispatchMessage 处理任务下发消息
// 反序列化任务数据，调用任务下发回调
func (t *RedisWorkerTransport) handleTaskDispatchMessage(msg *redis.Message) {
	if t.taskDispatchHandler == nil {
		return
	}

	var task common.TaskInfo
	if err := json.Unmarshal([]byte(msg.Payload), &task); err != nil {
		t.logger.ErrorKV("Failed to unmarshal task dispatch message", "error", err)
		return
	}

	result, err := t.taskDispatchHandler(&task)
	if err != nil {
		t.logger.ErrorKV("Task dispatch handler error", "error", err)
		return
	}

	t.logger.DebugKV("Task dispatched via Redis", "task_id", task.ID, "accepted", result.Accepted)
}

// handleTaskCancelMessage 处理任务取消消息
// 反序列化取消数据，调用任务取消回调
func (t *RedisWorkerTransport) handleTaskCancelMessage(msg *redis.Message) {
	if t.taskCancelHandler == nil {
		return
	}

	var data struct {
		TaskID string `json:"task_id"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
		t.logger.ErrorKV("Failed to unmarshal task cancel message", "error", err)
		return
	}

	if err := t.taskCancelHandler(data.TaskID); err != nil {
		t.logger.ErrorKV("Task cancel handler error", "error", err)
	}
}

// =====================================================================
// Redis 传输层工厂
// =====================================================================

// RedisTransportFactory Redis 传输层工厂 - 创建 Redis 协议的 Master/Worker 传输层
type RedisTransportFactory struct{}

// NewRedisTransportFactory 创建 Redis 传输层工厂
func NewRedisTransportFactory() *RedisTransportFactory {
	return &RedisTransportFactory{}
}

// CreateMasterTransport 创建 Redis Master 传输层
func (f *RedisTransportFactory) CreateMasterTransport(config *common.MasterConfig, log logger.ILogger) (MasterTransport, error) {
	return NewRedisMasterTransport(config, log), nil
}

// CreateWorkerTransport 创建 Redis Worker 传输层
func (f *RedisTransportFactory) CreateWorkerTransport(config *common.WorkerConfig, log logger.ILogger) (WorkerTransport, error) {
	return NewRedisWorkerTransport(config, log), nil
}

// Type 返回传输协议类型
func (f *RedisTransportFactory) Type() common.TransportType {
	return common.TransportTypeRedis
}
