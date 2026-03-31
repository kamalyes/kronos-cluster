/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 13:16:28
 * @FilePath: \go-distributed\common\configs.go
 * @Description: Master 和 Worker 配置定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import "time"

// MasterConfig Master 节点配置
type MasterConfig struct {
	GRPCPort             int                 `json:"grpc_port" yaml:"grpc_port"`                           // gRPC 服务端口
	HTTPPort             int                 `json:"http_port" yaml:"http_port"`                           // HTTP 服务端口
	HeartbeatInterval    time.Duration       `json:"heartbeat_interval" yaml:"heartbeat_interval"`         // 心跳检查间隔
	HeartbeatTimeout     time.Duration       `json:"heartbeat_timeout" yaml:"heartbeat_timeout"`           // 心跳超时时间
	HeartbeatMaxFailures int                 `json:"heartbeat_max_failures" yaml:"heartbeat_max_failures"` // 最大健康检查失败次数
	NodeOfflineThreshold time.Duration       `json:"node_offline_threshold" yaml:"node_offline_threshold"` // 节点离线判断阈值
	CandidateNodeCount   int                 `json:"candidate_node_count" yaml:"candidate_node_count"`     // 调度时候选节点数量
	SelectStrategy       SelectStrategy      `json:"select_strategy" yaml:"select_strategy"`               // 节点选择策略
	NodeFilter           *NodeFilter         `json:"node_filter" yaml:"node_filter"`                       // 节点过滤器
	EnableTLS            bool                `json:"enable_tls" yaml:"enable_tls"`                         // 是否启用 TLS
	CertFile             string              `json:"cert_file" yaml:"cert_file"`                           // TLS 证书文件路径
	KeyFile              string              `json:"key_file" yaml:"key_file"`                             // TLS 密钥文件路径
	Secret               string              `json:"secret" yaml:"secret"`                                 // JWT 签名密钥
	TokenExpiration      time.Duration       `json:"token_expiration" yaml:"token_expiration"`             // 令牌过期时间
	TokenIssuer          string              `json:"token_issuer" yaml:"token_issuer"`                     // 令牌签发者
	TransportType        TransportType       `json:"transport_type" yaml:"transport_type"`                 // 传输协议类型
	RedisAddr            string              `json:"redis_addr" yaml:"redis_addr"`                         // Redis 地址
	RedisPassword        string              `json:"redis_password" yaml:"redis_password"`                 // Redis 密码
	RedisDB              int                 `json:"redis_db" yaml:"redis_db"`                             // Redis 数据库编号
	EnableAuth           bool                `json:"enable_auth" yaml:"enable_auth"`                       // 是否启用安全认证
	JoinSecrets          []*JoinSecretEntry  `json:"join_secrets" yaml:"join_secrets"`                     // Worker 加入集群的预共享密钥列表（类似 K8s Bootstrap Tokens）
	ControlPlane         *ControlPlaneConfig `json:"control_plane" yaml:"control_plane"`                   // 控制平面配置
	GenerateConfigFile   bool                `json:"generate_config_file" yaml:"generate_config_file"`     // 是否自动生成配置文件
}

// WorkerConfig Worker 节点配置
type WorkerConfig struct {
	WorkerID                string            `json:"worker_id" yaml:"worker_id"`                                 // Worker 唯一标识
	MasterAddr              string            `json:"master_addr" yaml:"master_addr"`                             // Master 地址
	GRPCPort                int32             `json:"grpc_port" yaml:"grpc_port"`                                 // gRPC 服务端口
	Region                  string            `json:"region" yaml:"region"`                                       // 所属区域
	Labels                  map[string]string `json:"labels" yaml:"labels"`                                       // 节点标签
	MaxConcurrency          int               `json:"max_concurrency" yaml:"max_concurrency"`                     // 最大并发任务数
	EnableTLS               bool              `json:"enable_tls" yaml:"enable_tls"`                               // 是否启用 TLS
	CertFile                string            `json:"cert_file" yaml:"cert_file"`                                 // TLS 证书文件路径
	ReportBuffer            int               `json:"report_buffer" yaml:"report_buffer"`                         // 上报缓冲区大小
	ReportInterval          time.Duration     `json:"report_interval" yaml:"report_interval"`                     // 上报间隔
	ResourceMonitor         bool              `json:"resource_monitor" yaml:"resource_monitor"`                   // 是否启用资源监控
	ResourceMonitorInterval time.Duration     `json:"resource_monitor_interval" yaml:"resource_monitor_interval"` // 资源监控间隔
	MaxConcurrentTasks      int               `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`           // 最大并发任务数
	RegisterMaxRetries      int               `json:"register_max_retries" yaml:"register_max_retries"`           // 注册最大重试次数
	RegisterRetryInterval   time.Duration     `json:"register_retry_interval" yaml:"register_retry_interval"`     // 注册重试间隔
	ConnectMaxRetries       int               `json:"connect_max_retries" yaml:"connect_max_retries"`             // 连接最大重试次数
	ConnectRetryInterval    time.Duration     `json:"connect_retry_interval" yaml:"connect_retry_interval"`       // 连接重试间隔
	BackoffMultiplier       float64           `json:"backoff_multiplier" yaml:"backoff_multiplier"`               // 退避倍数
	TransportType           TransportType     `json:"transport_type" yaml:"transport_type"`                       // 传输协议类型
	RedisAddr               string            `json:"redis_addr" yaml:"redis_addr"`                               // Redis 地址
	RedisPassword           string            `json:"redis_password" yaml:"redis_password"`                       // Redis 密码
	RedisDB                 int               `json:"redis_db" yaml:"redis_db"`                                   // Redis 数据库编号
	JoinSecret              string            `json:"join_secret" yaml:"join_secret"`                             // 加入集群的预共享密钥
}

// JoinSecretEntry 加入集群的预共享密钥条目
// 类似 Kubernetes Bootstrap Token，支持多个令牌、TTL 过期、用途限制
// 格式: <token-id>.<secret>，如 abcdef.0123456789abcdef
type JoinSecretEntry struct {
	ID          string            `json:"id" yaml:"id"`                   // 令牌 ID（6 位，如 abcdef）
	Secret      string            `json:"secret" yaml:"secret"`           // 令牌密钥（16 位，如 0123456789abcdef）
	Description string            `json:"description" yaml:"description"` // 描述信息（如 "for worker pool A"）
	ExpiresAt   time.Time         `json:"expires_at" yaml:"expires_at"`   // 过期时间
	TTL         time.Duration     `json:"ttl" yaml:"ttl"`                 // 生存时间（创建时设置）
	CreatedAt   time.Time         `json:"created_at" yaml:"created_at"`   // 创建时间
	Usages      int               `json:"usages" yaml:"usages"`           // 剩余可用次数（0 表示无限）
	MaxUsages   int               `json:"max_usages" yaml:"max_usages"`   // 最大可用次数（0 表示无限）
	Labels      map[string]string `json:"labels" yaml:"labels"`           // 标签（可用于区分不同用途）
}

// FullToken 获取完整的令牌字符串（ID.Secret）
func (e *JoinSecretEntry) FullToken() string {
	return e.ID + "." + e.Secret
}

// IsExpired 判断令牌是否已过期
func (e *JoinSecretEntry) IsExpired() bool {
	if e.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// IsUsable 判断令牌是否可用（未过期且有余量）
func (e *JoinSecretEntry) IsUsable() bool {
	if e.IsExpired() {
		return false
	}
	if e.MaxUsages > 0 && e.Usages <= 0 {
		return false
	}
	return true
}

// Use 使用一次令牌，返回是否成功
func (e *JoinSecretEntry) Use() bool {
	if !e.IsUsable() {
		return false
	}
	if e.MaxUsages > 0 {
		e.Usages--
	}
	return true
}

// ControlPlaneConfig 控制平面配置
// 类似 Kubernetes 的 kubeconfig，CLI 通过配置文件连接 Master
type ControlPlaneConfig struct {
	ServerAddr  string            `json:"server_addr" yaml:"server_addr"`   // Master gRPC 地址
	Secret      string            `json:"secret" yaml:"secret"`             // 访问密钥
	EnableAuth  bool              `json:"enable_auth" yaml:"enable_auth"`   // 是否启用认证
	Token       string            `json:"token" yaml:"token"`               // 已获取的认证令牌
	CACertFile  string            `json:"ca_cert_file" yaml:"ca_cert_file"` // CA 证书路径
	Insecure    bool              `json:"insecure" yaml:"insecure"`         // 是否跳过 TLS 验证
	ClusterName string            `json:"cluster_name" yaml:"cluster_name"` // 集群名称
	Namespace   string            `json:"namespace" yaml:"namespace"`       // 命名空间
	Labels      map[string]string `json:"labels" yaml:"labels"`             // 自定义标签
}
