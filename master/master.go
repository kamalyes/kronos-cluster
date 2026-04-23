/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 10:00:00
 * @FilePath: \kronos-cluster\master\master.go
 * @Description: Master 主控制器 - 管理节点注册、心跳和健康检查
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
)

// Master 泛型主控制器 - 管理分布式节点生命周期和任务调度
type Master[T common.NodeInfo] struct {
	config        *common.MasterConfig             // Master 配置
	pool          *NodePool[T]                     // Worker 节点池
	masterPool    *MasterPool                      // Master 节点池
	health        *HealthChecker[T]                // 健康检查器
	taskManager   *TaskManager                     // 任务管理器
	adminService  *AdminService                    // 管理服务
	transport     transport.MasterTransport        // 传输层
	tokenManager  *common.TokenManager             // 令牌管理器
	authManager   *common.AuthManager              // 安全认证管理器
	nodeConverter func(common.NodeInfo) (T, error) // 节点类型转换器
	logger        logger.ILogger                   // 日志
	running       *syncx.Bool                      // 运行状态
	cancelFunc    context.CancelFunc               // 取消函数
}

// nodePoolAdapter 将 *NodePool[T] 适配为 NodeProvider 接口
// 由于 Go 泛型不支持协变，*NodePool[T] 的方法签名与 NodeProvider 不完全匹配
// 通过适配器桥接类型差异
type nodePoolAdapter[T common.NodeInfo] struct {
	pool *NodePool[T]
}

func (a *nodePoolAdapter[T]) Select(count int) []common.NodeInfo {
	nodes := a.pool.Select(count)
	result := make([]common.NodeInfo, len(nodes))
	for i, n := range nodes {
		result[i] = n
	}
	return result
}

func (a *nodePoolAdapter[T]) Get(nodeID string) (common.NodeInfo, bool) {
	return a.pool.Get(nodeID)
}

func (a *nodePoolAdapter[T]) GetAll() []common.NodeInfo {
	nodes := a.pool.GetAll()
	result := make([]common.NodeInfo, len(nodes))
	for i, n := range nodes {
		result[i] = n
	}
	return result
}

// NewMaster 创建 Master 实例
// config: Master 配置
// nodeConverter: 将通用 NodeInfo 转换为具体业务节点类型的回调函数
// store: 任务持久化存储（MemoryTaskStore 用于测试，RedisTaskStore 用于生产）
// log: 日志实例
func NewMaster[T common.NodeInfo](
	config *common.MasterConfig,
	nodeConverter func(common.NodeInfo) (T, error),
	store TaskStore,
	log logger.ILogger,
) (*Master[T], error) {
	if config == nil {
		return nil, errorx.WrapError(common.ErrConfigCannotNil)
	}
	if nodeConverter == nil {
		return nil, errorx.WrapError(common.ErrNodeConverterNil)
	}
	if store == nil {
		return nil, errorx.WrapError(common.ErrConfigCannotNil)
	}

	config.HeartbeatInterval = mathx.IfDefaultAndClamp(config.HeartbeatInterval, 5*time.Second, 1*time.Second, 60*time.Second)
	config.HeartbeatTimeout = mathx.IfDefaultAndClamp(config.HeartbeatTimeout, 15*time.Second, 5*time.Second, 60*time.Second)
	config.NodeOfflineThreshold = mathx.IfDefaultAndClamp(config.NodeOfflineThreshold, 5*time.Minute, 30*time.Second, 30*time.Minute)
	config.HeartbeatMaxFailures = mathx.IfDefaultAndClamp(config.HeartbeatMaxFailures, 3, 1, 10)
	config.CandidateNodeCount = mathx.IfDefaultAndClamp(config.CandidateNodeCount, 10, 1, 1024)
	config.Secret = mathx.IfEmpty(config.Secret, "kronos-cluster-secret-key")
	config.TokenExpiration = mathx.IfLeZero(config.TokenExpiration, 24*time.Hour)
	config.TokenIssuer = mathx.IfEmpty(config.TokenIssuer, "kronos-cluster-master")
	config.SelectStrategy = mathx.IfEmpty(config.SelectStrategy, common.SelectStrategyRoundRobin)

	selector := NewSelector[T](config.SelectStrategy, nil)

	pool := NewNodePool[T](selector, log, config)
	health := NewHealthChecker[T](pool, config.HeartbeatInterval, config.HeartbeatTimeout, config.HeartbeatMaxFailures, log)
	tokenManager := common.NewTokenManager(config.Secret, config.TokenExpiration, config.TokenIssuer)
	authManager := common.NewAuthManager(config)

	masterTransport, err := createMasterTransport(config, log)
	if err != nil {
		return nil, errorx.WrapError(common.ErrFailedCreateTransport, err)
	}

	adapter := &nodePoolAdapter[T]{pool: pool}

	masterPool := NewMasterPool(log)

	taskManager := NewTaskManager(adapter, masterTransport, store, log, config.CandidateNodeCount)
	adminService := NewAdminService(adapter, masterPool, store, log, authManager)

	if grpcTransport, ok := masterTransport.(*transport.GRPCMasterTransport); ok {
		grpcTransport.RegisterAdminService(adminService)
	}

	return &Master[T]{
		config:        config,
		pool:          pool,
		masterPool:    masterPool,
		health:        health,
		taskManager:   taskManager,
		adminService:  adminService,
		transport:     masterTransport,
		tokenManager:  tokenManager,
		authManager:   authManager,
		nodeConverter: nodeConverter,
		logger:        log,
		running:       syncx.NewBool(false),
	}, nil
}

// createMasterTransport 根据配置创建 Master 传输层
func createMasterTransport(config *common.MasterConfig, log logger.ILogger) (transport.MasterTransport, error) {
	switch config.TransportType {
	case common.TransportTypeGRPC, "":
		return transport.NewGRPCMasterTransport(config, log), nil
	case common.TransportTypeRedis:
		return transport.NewRedisMasterTransport(config, log), nil
	default:
		return nil, fmt.Errorf(common.ErrUnsupportedTransport, config.TransportType)
	}
}

// Start 启动 Master 服务
func (m *Master[T]) Start(ctx context.Context) error {
	if !m.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyRunning, "master")
	}

	ctx, cancel := context.WithCancel(ctx)
	m.cancelFunc = cancel

	// 清空节点池，避免旧节点信息导致Worker无法重新注册
	m.pool.Clear()
	m.masterPool.Clear()

	// 清空 Worker 连接，避免使用旧的连接
	if grpcTransport, ok := m.transport.(*transport.GRPCMasterTransport); ok {
		grpcTransport.ClearWorkerConns()
	}

	if err := m.transport.Start(ctx); err != nil {
		return errorx.WrapError(common.ErrFailedStartTransport, err)
	}

	// 等待 gRPC 服务器完全启动
	time.Sleep(500 * time.Millisecond)

	// Master 自注册到 MasterPool
	masterNode := common.NewMasterNodeInfo(
		m.config.MasterID,
		m.config.Hostname,
		m.config.AdvertiseAddress,
		int32(m.config.GRPCPort),
		m.config.ClusterName,
	)
	masterNode.Version = "1.0.0"
	masterNode.LastHeartbeat = time.Now()
	masterNode.RegisteredAt = time.Now()
	if err := m.masterPool.Register(masterNode); err != nil {
		m.logger.WarnKV("Failed to register master node to master pool", "error", err)
	} else {
		m.logger.InfoKV("Master node self-registered",
			"node_id", masterNode.GetID(),
			"hostname", masterNode.GetHostname(),
			"is_leader", masterNode.IsLeader)
	}

	m.setupTransportHandlers()

	go m.health.Start(ctx)

	if err := m.taskManager.Start(ctx); err != nil {
		m.logger.WarnKV("Failed to start task manager", "error", err)
	}

	m.logger.InfoKV("Master started successfully",
		"transport", m.config.TransportType,
		"grpc_port", m.config.GRPCPort)

	// 自动生成配置文件（如果启用）
	if m.config.GenerateConfigFile {
		serverAddr := m.config.ControlPlane.ServerAddr
		if serverAddr == "" {
			serverAddr = fmt.Sprintf("localhost:%d", m.config.GRPCPort)
		}
		// 使用 MasterConfig.Secret 作为 CLI 认证密钥
		common.GenerateConfigFile(m.config.Secret, serverAddr, m.logger)
	}

	return nil
}

// Stop 停止 Master 服务
func (m *Master[T]) Stop() error {
	if !m.running.CAS(true, false) {
		return fmt.Errorf(common.ErrNotRunning, "master")
	}

	if err := m.taskManager.Stop(); err != nil {
		m.logger.WarnKV("Failed to stop task manager", "error", err)
	}

	if m.cancelFunc != nil {
		m.cancelFunc()
	}

	if err := m.transport.Stop(); err != nil {
		m.logger.ErrorKV("Failed to stop transport", "error", err)
	}

	m.logger.Info("Master stopped")
	return nil
}

// setupTransportHandlers 设置传输层事件回调
func (m *Master[T]) setupTransportHandlers() {
	m.transport.OnRegister(func(nodeInfo common.NodeInfo, extension []byte) (*transport.RegistrationResult, error) {
		node, err := m.nodeConverter(nodeInfo)
		if err != nil {
			return &transport.RegistrationResult{Success: false, Message: err.Error()}, nil
		}

		if err := m.pool.Register(node); err != nil {
			return &transport.RegistrationResult{Success: false, Message: err.Error()}, nil
		}

		token, _ := m.tokenManager.GenerateToken(nodeInfo.GetID())

		return &transport.RegistrationResult{
			Success:           true,
			Message:           "registered successfully",
			Token:             token,
			HeartbeatInterval: int64(m.config.HeartbeatInterval.Seconds()),
		}, nil
	})

	m.transport.OnRegisterWithSecret(func(nodeInfo common.NodeInfo, joinSecret string, extension []byte) (*transport.RegistrationResult, error) {
		if m.authManager.IsAuthEnabled() {
			if err := m.authManager.ValidateJoinSecret(joinSecret); err != nil {
				return &transport.RegistrationResult{Success: false, Message: err.Error()}, nil
			}
			m.authManager.UseJoinSecret(joinSecret)
		}

		node, err := m.nodeConverter(nodeInfo)
		if err != nil {
			return &transport.RegistrationResult{Success: false, Message: err.Error()}, nil
		}

		if err := m.pool.Register(node); err != nil {
			return &transport.RegistrationResult{Success: false, Message: err.Error()}, nil
		}

		token, _ := m.tokenManager.GenerateToken(nodeInfo.GetID())

		return &transport.RegistrationResult{
			Success:           true,
			Message:           "registered successfully",
			Token:             token,
			HeartbeatInterval: int64(m.config.HeartbeatInterval.Seconds()),
		}, nil
	})

	m.transport.OnHeartbeat(func(nodeID string, state common.NodeState, extension []byte) (*transport.HeartbeatResult, error) {
		if err := m.pool.UpdateHeartbeat(nodeID); err != nil {
			return &transport.HeartbeatResult{OK: false, Message: err.Error()}, nil
		}

		if err := m.pool.UpdateNodeState(nodeID, state); err != nil {
			m.logger.WarnKV("Failed to update node state", "node_id", nodeID, "error", err)
		}

		return &transport.HeartbeatResult{OK: true, Message: "OK"}, nil
	})

	m.transport.OnUnregister(func(nodeID string, reason string) error {
		return m.pool.Unregister(nodeID)
	})
}

// GetPool 获取节点池
func (m *Master[T]) GetPool() *NodePool[T] {
	return m.pool
}

// GetMasterPool 获取 Master 节点池
func (m *Master[T]) GetMasterPool() *MasterPool {
	return m.masterPool
}

// GetHealthChecker 获取健康检查器
func (m *Master[T]) GetHealthChecker() *HealthChecker[T] {
	return m.health
}

// GetTaskManager 获取任务管理器
func (m *Master[T]) GetTaskManager() *TaskManager {
	return m.taskManager
}

// GetAdminService 获取管理服务
func (m *Master[T]) GetAdminService() *AdminService {
	return m.adminService
}

// GetConfig 获取 Master 配置
func (m *Master[T]) GetConfig() *common.MasterConfig {
	return m.config
}

// GetAuthManager 获取安全认证管理器
func (m *Master[T]) GetAuthManager() *common.AuthManager {
	return m.authManager
}

// IsRunning 判断 Master 是否运行中
func (m *Master[T]) IsRunning() bool {
	return m.running.Load()
}
