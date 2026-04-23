/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 15:21:16
 * @FilePath: \kronos-cluster\worker\worker.go
 * @Description: Worker 工作节点 - 负责注册、心跳和资源监控
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/kamalyes/go-toolbox/pkg/netx"
	"github.com/kamalyes/go-toolbox/pkg/osx"
	"github.com/kamalyes/go-toolbox/pkg/retry"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/shirou/gopsutil/v3/mem"
)

var errReconnectInProgress = errors.New("reconnect already in progress")

// Worker 泛型工作节点 - 负责向 Master 注册、发送心跳和上报资源
type Worker[T common.NodeInfo] struct {
	config        *common.WorkerConfig       // Worker 配置
	info          T                          // 节点信息
	transport     transport.WorkerTransport  // 传输层
	monitor       *ResourceMonitor           // 资源监控器
	executor      *TaskExecutor              // 任务执行器
	logger        logger.ILogger             // 日志
	running       *syncx.Bool                // 运行状态
	cancelFunc    context.CancelFunc         // 取消函数
	heartbeatTask *syncx.PeriodicTaskManager // 心跳定时任务管理器
	reconnecting  *syncx.Bool                // 重连状态标志
}

// NewWorker 创建 Worker 实例
// config: Worker 配置
// initInfo: 初始化节点信息的回调函数，由调用方创建具体节点类型实例
// log: 日志实例
func NewWorker[T common.NodeInfo](
	config *common.WorkerConfig,
	initInfo func() T,
	log logger.ILogger,
) (*Worker[T], error) {
	if config == nil {
		return nil, errorx.WrapError(common.ErrConfigCannotNil)
	}
	if initInfo == nil {
		return nil, errorx.WrapError(common.ErrInitInfoCannotNil)
	}

	info := initInfo()

	hostname := osx.SafeGetHostName()
	localIP, err := netx.GetPrivateIP()
	if err != nil {
		localIP = "127.0.0.1"
	}

	populateBaseNode(info, config, hostname, localIP)

	workerTransport, err := createWorkerTransport(config, log)
	if err != nil {
		return nil, errorx.WrapError(common.ErrFailedCreateTransport, err)
	}

	// 使用配置中的资源监控间隔，如果未设置则使用默认值5秒
	monitorInterval := mathx.IfDefaultAndClamp(config.ResourceMonitorInterval, 5*time.Second, 1*time.Second, 300*time.Second)
	monitor := NewResourceMonitor(log, monitorInterval)

	return &Worker[T]{
		config:        config,
		info:          info,
		transport:     workerTransport,
		monitor:       monitor,
		executor:      NewTaskExecutor(workerTransport, monitor, config.MaxConcurrentTasks, log),
		logger:        log,
		running:       syncx.NewBool(false),
		heartbeatTask: syncx.NewPeriodicTaskManager(),
		reconnecting:  syncx.NewBool(false),
	}, nil
}

// populateBaseNode 填充节点基础信息（主机名、IP、CPU、内存等）
func populateBaseNode[T common.NodeInfo](node T, config *common.WorkerConfig, hostname, ip string) {
	node.SetState(common.NodeStateIdle)
	node.SetRole(common.NodeRoleWorker)
	base, ok := any(node).(*common.BaseNodeInfo)
	if ok {
		base.ID = config.WorkerID
		base.Hostname = hostname
		base.IP = ip
		base.GRPCPort = config.GRPCPort
		base.CPUCores = runtime.NumCPU()
		base.Memory = getMemorySize()
		base.Version = "1.0.0"
		base.Region = config.Region
		base.Labels = config.Labels
		base.Role = common.NodeRoleWorker
	}
}

// createWorkerTransport 根据配置创建 Worker 传输层
func createWorkerTransport(config *common.WorkerConfig, log logger.ILogger) (transport.WorkerTransport, error) {
	switch config.TransportType {
	case common.TransportTypeGRPC, "":
		return transport.NewGRPCWorkerTransport(config, log), nil
	case common.TransportTypeRedis:
		return transport.NewRedisWorkerTransport(config, log), nil
	default:
		return nil, fmt.Errorf(common.ErrUnsupportedTransport, config.TransportType)
	}
}

// Start 启动 Worker（连接 Master → 注册 → 启动心跳 → 启动资源监控）
func (w *Worker[T]) Start(ctx context.Context) error {
	if !w.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyRunning, "worker")
	}

	ctx, cancel := context.WithCancel(ctx)
	w.cancelFunc = cancel

	if err := w.transport.Connect(ctx); err != nil {
		return errorx.WrapError(common.ErrFailedConnectMaster, err)
	}

	// 设置任务执行器回调
	w.transport.OnTaskDispatched(w.executor.OnTaskDispatched)
	w.transport.OnTaskCancelled(w.executor.OnTaskCancelled)

	if err := w.register(ctx); err != nil {
		return errorx.WrapError(common.ErrRegisterFailed, err)
	}

	// 打开双向流连接到 Master
	if err := w.transport.ConnectStream(ctx); err != nil {
		w.logger.WarnContextKV(ctx, "Failed to open stream to master, will use unary RPC instead", "error", err)
	}

	w.startHeartbeat(ctx)

	if w.config.ResourceMonitor && w.monitor != nil {
		go w.monitor.Start(ctx)
	}

	w.logger.InfoContextKV(ctx, "Worker started successfully",
		"worker_id", w.info.GetID(),
		"transport", w.config.TransportType,
		"master_addr", w.config.MasterAddr)

	return nil
}

// Stop 停止 Worker（注销 → 取消上下文 → 关闭传输层）
func (w *Worker[T]) Stop() error {
	if !w.running.CAS(true, false) {
		return fmt.Errorf(common.ErrNotRunning, "worker")
	}

	w.logger.Info("Stopping worker...")

	w.unregister()

	if w.cancelFunc != nil {
		w.cancelFunc()
	}

	if err := w.transport.Close(); err != nil {
		w.logger.ErrorKV("Failed to close transport", "error", err)
	}

	w.logger.Info("Worker stopped")
	return nil
}

// register 向 Master 注册当前 Worker 节点
func (w *Worker[T]) register(ctx context.Context) error {
	// 使用配置中的注册重试参数，如果未设置则使用默认值，并限制范围
	maxRetries := mathx.IfDefaultAndClamp(w.config.RegisterMaxRetries, 5, 1, 10)
	retryInterval := mathx.IfDefaultAndClamp(w.config.RegisterRetryInterval, 1*time.Second, 500*time.Millisecond, 120*time.Second)
	// 使用配置中的退避倍数参数，如果未设置则使用默认值，并限制范围
	backoffMultiplier := mathx.IfDefaultAndClamp(w.config.BackoffMultiplier, 1.5, 1.0, 5.0)

	var registrationResult *transport.RegistrationResult

	// 使用retry包实现更专业的重试逻辑
	err := retry.NewRetryWithCtx(ctx).
		SetAttemptCount(maxRetries).
		SetInterval(retryInterval).
		SetBackoffMultiplier(backoffMultiplier). // 指数退避策略
		SetJitter(true).                         // 启用随机抖动
		SetErrCallback(func(nowAttemptCount, remainCount int, err error, funcName ...string) {
			w.logger.WarnContextKV(ctx, "Registration attempt failed, will retry",
				"attempt", nowAttemptCount,
				"remaining", remainCount,
				"max_retries", maxRetries,
				"retry_interval", retryInterval,
				"backoff_multiplier", backoffMultiplier,
				"error", err)
		}).
		SetSuccessCallback(func(funcName ...string) {
			w.logger.InfoContextKV(ctx, "Registration attempt succeeded",
				"worker_id", w.info.GetID())
		}).
		Do(func() error {
			var result *transport.RegistrationResult
			var err error

			if w.config.JoinSecret != "" {
				result, err = w.transport.RegisterWithSecret(ctx, w.info, w.config.JoinSecret, nil)
			} else {
				result, err = w.transport.Register(ctx, w.info, nil)
			}

			if err != nil {
				return err
			}

			if !result.Success {
				return fmt.Errorf(common.ErrRegistrationRejected, result.Message)
			}

			registrationResult = result
			return nil
		})

	if err != nil {
		return err
	}

	w.logger.InfoContextKV(ctx, "Registered successfully",
		"worker_id", w.info.GetID(),
		"token", registrationResult.Token,
		"heartbeat_interval", registrationResult.HeartbeatInterval)

	return nil
}

// unregister 从 Master 注销当前 Worker 节点
func (w *Worker[T]) unregister() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.transport.Unregister(ctx, w.info.GetID(), "shutdown"); err != nil {
		w.logger.WarnKV("Failed to unregister", "error", err)
		return
	}
	w.logger.InfoContextKV(ctx, "Unregistered successfully", "worker_id", w.info.GetID())
}

// startHeartbeat 启动心跳定时任务
func (w *Worker[T]) startHeartbeat(ctx context.Context) {
	interval := mathx.IfEmpty(w.config.ReportInterval, 2*time.Second)

	task := syncx.NewPeriodicTask("heartbeat", interval, func(taskCtx context.Context) error {
		return w.sendHeartbeat(taskCtx)
	}).
		SetPreventOverlap(true).
		SetOnError(func(name string, err error) {
			w.logger.WarnContextKV(ctx, "Heartbeat error", "error", err)
		})

	w.heartbeatTask.AddTask(task)
	w.heartbeatTask.Start()
}

// sendHeartbeat 发送心跳到 Master
func (w *Worker[T]) sendHeartbeat(ctx context.Context) error {
	state := w.info.GetState()

	var extension []byte
	if w.monitor != nil {
		if usage, err := w.monitor.GetResourceUsage(); err == nil {
			w.info.SetResourceUsage(usage)
		}
	}

	w.logger.DebugContextKV(ctx, "Sending heartbeat to master", "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
	result, err := w.transport.Heartbeat(ctx, w.info.GetID(), state, extension)
	if err != nil {
		w.logger.WarnContextKV(ctx, "Heartbeat failed, attempting to reconnect to master", "error", err, "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		// 尝试重新连接和注册
		if reconnectErr := w.reconnect(ctx); reconnectErr != nil {
			if errors.Is(reconnectErr, errReconnectInProgress) {
				w.logger.DebugContextKV(ctx, "Reconnect already in progress after heartbeat failure", "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
				return nil
			}
			w.logger.ErrorContextKV(ctx, "Failed to reconnect to master", "error", reconnectErr, "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		} else {
			w.logger.InfoContextKV(ctx, "Successfully reconnected to master", "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		}
		return nil
	}

	if !result.OK {
		w.logger.InfoContextKV(ctx, "Heartbeat rejected, attempting reconnect", "message", result.Message, "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		if reconnectErr := w.reconnect(ctx); reconnectErr != nil {
			if errors.Is(reconnectErr, errReconnectInProgress) {
				w.logger.DebugContextKV(ctx, "Reconnect already in progress after heartbeat rejection", "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
				return nil
			}
			w.logger.WarnContextKV(ctx, "Reconnect after heartbeat rejection failed", "error", reconnectErr, "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		} else {
			w.logger.InfoContextKV(ctx, "Successfully recovered after heartbeat rejection", "worker_id", w.info.GetID(), "master_addr", w.config.MasterAddr)
		}
		return nil
	}

	w.logger.DebugContextKV(ctx, "Heartbeat sent", "worker_id", w.info.GetID(), "state", state)
	return nil
}

// reconnect 重新连接到 Master
func (w *Worker[T]) reconnect(ctx context.Context) error {
	// 检查是否已经在重连中
	if !w.reconnecting.CAS(false, true) {
		w.logger.InfoContextKV(ctx, "Already reconnecting, skipping")
		return errReconnectInProgress
	}
	defer w.reconnecting.Store(false)

	w.logger.InfoContextKV(ctx, "Attempting to reconnect to master", "master_addr", w.config.MasterAddr)

	// 等待一段时间，给Master足够的启动时间
	time.Sleep(2 * time.Second)

	// 关闭现有连接
	if err := w.transport.Close(); err != nil {
		w.logger.WarnContextKV(ctx, "Failed to close existing transport", "error", err)
	}

	// 重新创建传输层
	workerTransport, err := createWorkerTransport(w.config, w.logger)
	if err != nil {
		return errorx.WrapError(common.ErrFailedCreateTransport, err)
	}
	w.transport = workerTransport

	// 重新设置任务执行器回调
	w.transport.OnTaskDispatched(w.executor.OnTaskDispatched)
	w.transport.OnTaskCancelled(w.executor.OnTaskCancelled)

	// 重新连接到 Master
	if err := w.transport.Connect(ctx); err != nil {
		return errorx.WrapError(common.ErrFailedConnectMaster, err)
	}

	// 重新注册到 Master
	if err := w.register(ctx); err != nil {
		return errorx.WrapError(common.ErrRegisterFailed, err)
	}

	// 重新打开双向流连接到 Master
	if err := w.transport.ConnectStream(ctx); err != nil {
		w.logger.WarnContextKV(ctx, "Failed to open stream to master after reconnection", "error", err)
	}

	w.logger.InfoContextKV(ctx, "Successfully reconnected to master", "master_addr", w.config.MasterAddr)
	return nil
}

// GetInfo 获取节点信息
func (w *Worker[T]) GetInfo() T {
	return w.info
}

// GetMonitor 获取资源监控器
func (w *Worker[T]) GetMonitor() *ResourceMonitor {
	return w.monitor
}

// GetTaskExecutor 获取任务执行器
func (w *Worker[T]) GetTaskExecutor() *TaskExecutor {
	return w.executor
}

// IsRunning 判断 Worker 是否运行中
func (w *Worker[T]) IsRunning() bool {
	return w.running.Load()
}

// getMemorySize 获取系统内存大小
func getMemorySize() int64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 8 * 1024 * 1024 * 1024
	}
	return int64(v.Total)
}
