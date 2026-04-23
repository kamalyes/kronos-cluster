/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 16:25:56
 * @FilePath: \kronos-cluster\common\errors.go
 * @Description: 统一错误信息常量定义 - 含错误码
 *
 * 错误码规范:
 *   - 1xxxx: 节点相关
 *   - 2xxxx: 配置相关
 *   - 3xxxx: 运行状态
 *   - 4xxxx: 传输层
 *   - 5xxxx: 注册/心跳
 *   - 6xxxx: 序列化/发布
 *   - 7xxxx: 令牌/认证
 *   - 8xxxx: 任务相关
 *   - 9xxxx: 连接生命周期
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

// ErrorCode 错误码类型
type ErrorCode int

const (
	CodeNodeNotFound          ErrorCode = 10001 // 节点未找到
	CodeNodeAlreadyRegistered ErrorCode = 10002 // 节点已注册
	CodeNodeAlreadyDisabled   ErrorCode = 10003 // 节点已停用
	CodeNodeAlreadyEnabled    ErrorCode = 10004 // 节点已启用
	CodeNodeNotSchedulable    ErrorCode = 10005 // 节点不可调度
	CodeNodeEvictFailed       ErrorCode = 10006 // 驱逐节点失败
)

const (
	CodeConfigCannotNil   ErrorCode = 20001 // 配置不能为空
	CodeNodeConverterNil  ErrorCode = 20002 // 节点转换器不能为空
	CodeInitInfoCannotNil ErrorCode = 20003 // 初始化函数不能为空
)

const (
	CodeAlreadyRunning   ErrorCode = 30001 // 已在运行中
	CodeNotRunning       ErrorCode = 30002 // 未在运行
	CodeAlreadyConnected ErrorCode = 30003 // 已连接
)

const (
	CodeUnsupportedTransport  ErrorCode = 40001 // 不支持的传输类型
	CodeFailedCreateTransport ErrorCode = 40002 // 创建传输层失败
	CodeFailedStartTransport  ErrorCode = 40003 // 启动传输层失败
	CodeFailedConnectMaster   ErrorCode = 40004 // 连接 Master 失败
	CodeFailedConnectRedis    ErrorCode = 40005 // 连接 Redis 失败
	CodeNotConnectedToMaster  ErrorCode = 40006 // 未连接到 Master
)

const (
	CodeRegisterFailed       ErrorCode = 50001 // 注册失败
	CodeRegistrationRejected ErrorCode = 50002 // 注册被拒绝
	CodeUnregisterFailed     ErrorCode = 50003 // 注销失败
	CodeUnregisterRejected   ErrorCode = 50004 // 注销被拒绝
	CodeHeartbeatFailed      ErrorCode = 50005 // 心跳失败
)

const (
	CodeFailedMarshal ErrorCode = 60001 // 序列化失败
	CodeFailedPublish ErrorCode = 60002 // 发布失败
	CodeFailedListen  ErrorCode = 60003 // 监听失败
)

const (
	CodeInvalidAlgorithm   ErrorCode = 70001 // 无效算法
	CodeInvalidToken       ErrorCode = 70002 // 无效令牌
	CodeAuthDisabled       ErrorCode = 70003 // 认证未启用
	CodeAuthRequired       ErrorCode = 70004 // 需要认证
	CodeInvalidJoinSecret  ErrorCode = 70005 // 无效的加入密钥
	CodeJoinSecretExpired  ErrorCode = 70006 // 加入密钥已过期
	CodeJoinSecretRequired ErrorCode = 70007 // 加入密钥必填
	CodeAuthFailed         ErrorCode = 70008 // 认证失败
	CodeTokenExpired       ErrorCode = 70009 // 令牌已过期
	CodeAccessDenied       ErrorCode = 70010 // 访问被拒绝
	CodeInvalidCredentials ErrorCode = 70011 // 无效的凭据
)

const (
	CodeTaskNotFound        ErrorCode = 80001 // 任务未找到
	CodeTaskStateTerminal   ErrorCode = 80002 // 任务状态为终态
	CodeTaskStateTransition ErrorCode = 80003 // 任务状态转换非法
	CodeTaskDispatchFailed  ErrorCode = 80004 // 任务下发失败
	CodeTaskRejected        ErrorCode = 80005 // 任务被 Worker 拒绝
	CodeTaskTimeout         ErrorCode = 80006 // 任务超时
	CodeTaskHandlerNotFound ErrorCode = 80007 // 任务处理器未找到
	CodeTaskTypeInvalid     ErrorCode = 80008 // 无效的任务类型
	CodeTaskAlreadyExists   ErrorCode = 80009 // 任务已存在
	CodeWorkerConnNotFound  ErrorCode = 80010 // 找不到 Worker 连接
	CodeWorkerChannelFull   ErrorCode = 80011 // Worker 通道已满
)

const (
	CodeConnectionNotReady     ErrorCode = 90001 // 连接未就绪
	CodeConnectionDisconnected ErrorCode = 90002 // 连接已断开
	CodeReconnectExhausted     ErrorCode = 90003 // 重连次数耗尽
	CodeDrainTimeout           ErrorCode = 90004 // 排空超时
)

const (
	CodeControlPlaneConfigNil ErrorCode = 21001 // 控制平面配置为空
	CodeFailedReadConfigFile  ErrorCode = 21002 // 读取配置文件失败
	CodeFailedParseYAMLConfig ErrorCode = 21003 // 解析 YAML 配置失败
	CodeFailedParseJSONConfig ErrorCode = 21004 // 解析 JSON 配置失败
	CodeFailedParseConfig     ErrorCode = 21005 // 解析配置失败
	CodeFailedCreateConfigDir ErrorCode = 21006 // 创建配置目录失败
	CodeFailedMarshalConfig   ErrorCode = 21007 // 序列化配置失败
	CodeFailedWriteConfigFile ErrorCode = 21008 // 写入配置文件失败
	CodeNoContextFound        ErrorCode = 21009 // 未找到上下文
	CodeClusterNotFound       ErrorCode = 21010 // 集群未找到
	CodeAuthInfoNotFound      ErrorCode = 21011 // 认证信息未找到
	CodeContextNotFound       ErrorCode = 21012 // 上下文未找到
	CodeFailedLoadConfig      ErrorCode = 21013 // 加载配置失败
	CodeFailedResolveConfig   ErrorCode = 21014 // 解析配置失败
	CodeFailedConnectMasterAt ErrorCode = 21015 // 连接 Master 失败
)

const (
	CodeMetadataNotFound      ErrorCode = 71001 // 上下文中未找到元数据
	CodeAuthTokenNotFound     ErrorCode = 71002 // 未找到认证令牌
	CodeAuthenticationFailed  ErrorCode = 71003 // 认证失败
	CodeListNodesFailed       ErrorCode = 71004 // 列出节点失败
	CodeGetNodeInfoFailed     ErrorCode = 71005 // 获取节点信息失败
	CodeGetClusterStatsFailed ErrorCode = 71006 // 获取集群统计失败
	CodeListTasksFailed       ErrorCode = 71007 // 列出任务失败
	CodeDrainNodeFailed       ErrorCode = 71008 // 排空节点失败
	CodeEvictNodeFailed       ErrorCode = 71009 // 驱逐节点失败
	CodeDisableNodeFailed     ErrorCode = 71010 // 停用节点失败
	CodeEnableNodeFailed      ErrorCode = 71011 // 启用节点失败
	CodeGetNodeTopFailed      ErrorCode = 71012 // 获取节点 Top 失败
	CodeGetNodeLogsFailed     ErrorCode = 71013 // 获取节点日志失败
	CodeFailedListTasksByNode ErrorCode = 71014 // 按节点列出任务失败
)

// ErrorWithCode 带错误码的错误
type ErrorWithCode struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *ErrorWithCode) Error() string {
	return e.Message
}

// NewErrorWithCode 创建带错误码的错误
func NewErrorWithCode(code ErrorCode, message string) *ErrorWithCode {
	return &ErrorWithCode{Code: code, Message: message}
}

// 节点相关错误
const (
	ErrNodeNotFound          = "node %s not found"
	ErrNodeAlreadyRegistered = "node %s already registered"
	ErrNodeAlreadyDisabled   = "node %s already disabled"
	ErrNodeAlreadyEnabled    = "node %s already enabled"
	ErrNodeNotSchedulable    = "node %s is not schedulable"
	ErrNodeEvictFailed       = "failed to evict node %s"
)

// 配置相关错误
const (
	ErrConfigCannotNil   = "config cannot be nil"
	ErrNodeConverterNil  = "nodeConverter cannot be nil"
	ErrInitInfoCannotNil = "initInfo cannot be nil"
)

// 运行状态错误
const (
	ErrAlreadyRunning   = "%s is already running"
	ErrNotRunning       = "%s is not running"
	ErrAlreadyConnected = "%s already connected"
)

// 传输层错误
const (
	ErrUnsupportedTransport  = "unsupported transport type: %s"
	ErrFailedCreateTransport = "failed to create transport"
	ErrFailedStartTransport  = "failed to start transport"
	ErrFailedConnectMaster   = "failed to connect to master"
	ErrFailedConnectRedis    = "failed to connect to redis"
	ErrNotConnectedToMaster  = "not connected to master"
)

// 注册/心跳错误
const (
	ErrRegisterFailed       = "register failed"
	ErrRegistrationRejected = "registration rejected: %s"
	ErrUnregisterFailed     = "unregister failed"
	ErrUnregisterRejected   = "unregister rejected: %s"
	ErrHeartbeatFailed      = "heartbeat failed"
)

// 序列化/发布错误
const (
	ErrFailedMarshal = "failed to marshal %s data"
	ErrFailedPublish = "failed to publish %s"
	ErrFailedListen  = "failed to listen"
)

// 令牌/认证错误
const (
	ErrInvalidAlgorithm   = "invalid algorithm"
	ErrInvalidToken       = "invalid token"
	ErrAuthDisabled       = "authentication is disabled"
	ErrAuthRequired       = "authentication required"
	ErrInvalidJoinSecret  = "invalid join secret"
	ErrJoinSecretExpired  = "join secret expired"
	ErrJoinSecretRequired = "join secret is required"
	ErrAuthFailed         = "authentication failed"
	ErrTokenExpired       = "token expired"
	ErrAccessDenied       = "access denied"
	ErrInvalidCredentials = "invalid credentials"
)

// 任务相关错误
const (
	ErrTaskNotFound        = "task %s not found"
	ErrTaskStateTerminal   = "task state %s is terminal"
	ErrTaskStateTransition = "task state transition %s to %s"
	ErrTaskDispatchFailed  = "failed to dispatch task %s to %s"
	ErrTaskRejected        = "task %s rejected by worker %s"
	ErrTaskTimeout         = "task %s timed out"
	ErrTaskHandlerNotFound = "task handler not found for type %s"
	ErrTaskTypeInvalid     = "invalid task type: %s"
	ErrTaskAlreadyExists   = "task %s already exists"
	ErrWorkerConnNotFound  = "worker connection not found for node %s"
	ErrWorkerChannelFull   = "failed to send task to worker %s: channel full"
)

// 连接生命周期错误
const (
	ErrConnectionNotReady     = "connection not ready"
	ErrConnectionDisconnected = "connection disconnected"
	ErrReconnectExhausted     = "reconnect attempts exhausted for %s"
	ErrDrainTimeout           = "drain timeout, %d tasks still running"
)

// 控制平面配置错误
const (
	ErrControlPlaneConfigNil = "control plane config is nil"
	ErrFailedReadConfigFile  = "failed to read config file: %w"
	ErrFailedParseYAMLConfig = "failed to parse yaml config: %w"
	ErrFailedParseJSONConfig = "failed to parse json config: %w"
	ErrFailedParseConfig     = "failed to parse config: %w"
	ErrFailedCreateConfigDir = "failed to create config directory: %w"
	ErrFailedMarshalConfig   = "failed to marshal config: %w"
	ErrFailedWriteConfigFile = "failed to write config file: %w"
	ErrNoContextFound        = "no context found"
	ErrClusterNotFound       = "cluster %s not found"
	ErrAuthInfoNotFound      = "auth info %s not found"
	ErrContextNotFound       = "context %s not found"
	ErrFailedLoadConfig      = "failed to load config: %w"
	ErrFailedResolveConfig   = "failed to resolve config: %w"
	ErrFailedConnectMasterAt = "failed to connect to master at %s: %w"
)

// CLI 管理操作错误
const (
	ErrMetadataNotFound        = "metadata not found in context"
	ErrAuthTokenNotFound       = "authorization token not found"
	ErrAuthenticationFailed    = "authentication failed: %w"
	ErrAuthenticationMsgFailed = "authentication failed: %s"
	ErrListNodesFailed         = "list nodes failed: %w"
	ErrGetNodeInfoFailed       = "get node info failed: %w"
	ErrGetClusterStatsFailed   = "get cluster stats failed: %w"
	ErrListTasksFailed         = "list tasks failed: %w"
	ErrDrainNodeFailed         = "drain node failed: %w"
	ErrEvictNodeFailed         = "evict node failed: %w"
	ErrDisableNodeFailed       = "disable node failed: %w"
	ErrEnableNodeFailed        = "enable node failed: %w"
	ErrGetNodeTopFailed        = "get node top failed: %w"
	ErrGetNodeLogsFailed       = "get node logs failed: %w"
	ErrFailedListTasksByNode   = "failed to list tasks by node: %w"
	ErrFailedListTasksAll      = "failed to list tasks: %w"
	ErrClientAlreadyRunning    = "client already running"
	ErrClientNotRunning        = "client not running"
	ErrClientNotReady          = "client not ready: %w"
	ErrClientNotConnected      = "client not connected"
	ErrPingFailed              = "ping failed: %w"
	ErrReconnectFailed         = "reconnect failed: %w"
)

// 任务存储错误
const (
	ErrFailedMarshalTask   = "failed to marshal task %s: %w"
	ErrFailedSaveTask      = "failed to save task %s: %w"
	ErrFailedGetTask       = "failed to get task %s: %w"
	ErrFailedUnmarshalTask = "failed to unmarshal task %s: %w"
	ErrFailedEnqueueTask   = "failed to enqueue task %s: %w"
	ErrFailedDequeueTask   = "failed to dequeue pending task: %w"
	ErrFailedScanTasks     = "failed to scan tasks: %w"
)

// 连接状态错误
const (
	ErrInvalidStateTransition = "invalid connection state transition: %s -> %s"
)
