/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 18:15:56
 * @FilePath: \go-distributed\common\node.go
 * @Description: 节点信息模型定义 - 支持多主多从架构和污点(Taint)机制
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import "time"

// NodeRole 节点角色类型
type NodeRole string

const (
	NodeRoleMaster NodeRole = "master" // Master 控制节点
	NodeRoleWorker NodeRole = "worker" // Worker 工作节点
)

// TaintEffect 污点效果（类似 Kubernetes Taint Effect）
type TaintEffect string

const (
	TaintEffectNoSchedule       TaintEffect = "NoSchedule"       // 不调度 - 不会将新任务调度到此节点
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule" // 尽量不调度 - 尽量避免将任务调度到此节点
	TaintEffectNoExecute        TaintEffect = "NoExecute"        // 不执行 - 不调度且驱逐已有任务
)

// Taint 节点污点（类似 Kubernetes Taint）
// 用于限制任务调度到特定节点
type Taint struct {
	Key    string      `json:"key"`    // 污点键
	Value  string      `json:"value"`  // 污点值
	Effect TaintEffect `json:"effect"` // 污点效果
}

// NodeInfo 节点信息接口 - 定义分布式节点的通用行为
type NodeInfo interface {
	// GetID 获取节点唯一标识
	GetID() string
	// GetHostname 获取节点主机名
	GetHostname() string
	// GetIP 获取节点 IP 地址
	GetIP() string
	// GetGRPCPort 获取 gRPC 服务端口
	GetGRPCPort() int32
	// GetCPUCores 获取 CPU 核心数
	GetCPUCores() int
	// GetMemory 获取内存大小（字节）
	GetMemory() int64
	// GetVersion 获取节点版本号
	GetVersion() string
	// GetState 获取节点当前状态
	GetState() NodeState
	// SetState 设置节点状态
	SetState(NodeState)
	// GetLabels 获取节点标签
	GetLabels() map[string]string
	// GetRegion 获取节点所属区域
	GetRegion() string
	// GetLastHeartbeat 获取最后心跳时间
	GetLastHeartbeat() time.Time
	// SetLastHeartbeat 设置最后心跳时间
	SetLastHeartbeat(time.Time)
	// GetResourceUsage 获取资源使用情况
	GetResourceUsage() *ResourceUsage
	// SetResourceUsage 设置资源使用情况
	SetResourceUsage(*ResourceUsage)
	// GetCurrentLoad 获取当前负载（0-1）
	GetCurrentLoad() float64
	// SetCurrentLoad 设置当前负载
	SetCurrentLoad(float64)
	// GetHealthCheckFail 获取健康检查失败次数
	GetHealthCheckFail() int
	// SetHealthCheckFail 设置健康检查失败次数
	SetHealthCheckFail(int)
	// GetRegisteredAt 获取注册时间
	GetRegisteredAt() time.Time
	// SetRegisteredAt 设置注册时间
	SetRegisteredAt(time.Time)
	// IsSchedulable 判断节点是否可调度（未被 cordon）
	IsSchedulable() bool
	// SetSchedulable 设置节点是否可调度
	SetSchedulable(bool)
	// GetDisableReason 获取停用原因
	GetDisableReason() string
	// SetDisableReason 设置停用原因
	SetDisableReason(string)
	// 多主多从架构支持
	GetRole() NodeRole
	SetRole(NodeRole)
	// 污点(Taint)支持
	GetTaints() []Taint
	SetTaints([]Taint)
	AddTaint(Taint)
	RemoveTaint(key string) bool
	HasTaint(key string) bool
}

// BaseNodeInfo 节点基础信息 - 提供 NodeInfo 接口的默认实现
type BaseNodeInfo struct {
	ID              string            `json:"id"`                // 节点唯一标识
	Hostname        string            `json:"hostname"`          // 主机名
	IP              string            `json:"ip"`                // IP 地址
	GRPCPort        int32             `json:"grpc_port"`         // gRPC 服务端口
	CPUCores        int               `json:"cpu_cores"`         // CPU 核心数
	Memory          int64             `json:"memory"`            // 内存大小（字节）
	Version         string            `json:"version"`           // 版本号
	Region          string            `json:"region"`            // 所属区域
	Labels          map[string]string `json:"labels"`            // 节点标签
	State           NodeState         `json:"state"`             // 当前状态
	LastHeartbeat   time.Time         `json:"last_heartbeat"`    // 最后心跳时间
	RegisteredAt    time.Time         `json:"registered_at"`     // 注册时间
	ResourceUsage   *ResourceUsage    `json:"resource_usage"`    // 资源使用情况
	CurrentLoad     float64           `json:"current_load"`      // 当前负载（0-1）
	HealthCheckFail int               `json:"health_check_fail"` // 健康检查失败次数
	Schedulable     bool              `json:"schedulable"`       // 是否可调度（未被 cordon）
	DisableReason   string            `json:"disable_reason"`    // 停用原因
	Role            NodeRole          `json:"role"`              // 角色
	Taints          []Taint           `json:"taints"`            // 污点
}

func (n *BaseNodeInfo) GetID() string                     { return n.ID }
func (n *BaseNodeInfo) GetHostname() string               { return n.Hostname }
func (n *BaseNodeInfo) GetIP() string                     { return n.IP }
func (n *BaseNodeInfo) GetGRPCPort() int32                { return n.GRPCPort }
func (n *BaseNodeInfo) GetCPUCores() int                  { return n.CPUCores }
func (n *BaseNodeInfo) GetMemory() int64                  { return n.Memory }
func (n *BaseNodeInfo) GetVersion() string                { return n.Version }
func (n *BaseNodeInfo) GetState() NodeState               { return n.State }
func (n *BaseNodeInfo) SetState(s NodeState)              { n.State = s }
func (n *BaseNodeInfo) GetLabels() map[string]string      { return n.Labels }
func (n *BaseNodeInfo) SetRegion(s string)                { n.Region = s }
func (n *BaseNodeInfo) GetRegion() string                 { return n.Region }
func (n *BaseNodeInfo) GetLastHeartbeat() time.Time       { return n.LastHeartbeat }
func (n *BaseNodeInfo) SetLastHeartbeat(t time.Time)      { n.LastHeartbeat = t }
func (n *BaseNodeInfo) GetResourceUsage() *ResourceUsage  { return n.ResourceUsage }
func (n *BaseNodeInfo) SetResourceUsage(r *ResourceUsage) { n.ResourceUsage = r }
func (n *BaseNodeInfo) GetCurrentLoad() float64           { return n.CurrentLoad }
func (n *BaseNodeInfo) SetCurrentLoad(l float64)          { n.CurrentLoad = l }
func (n *BaseNodeInfo) GetHealthCheckFail() int           { return n.HealthCheckFail }
func (n *BaseNodeInfo) SetHealthCheckFail(f int)          { n.HealthCheckFail = f }
func (n *BaseNodeInfo) GetRegisteredAt() time.Time        { return n.RegisteredAt }
func (n *BaseNodeInfo) SetRegisteredAt(t time.Time)       { n.RegisteredAt = t }
func (n *BaseNodeInfo) IsSchedulable() bool               { return n.Schedulable }
func (n *BaseNodeInfo) SetSchedulable(s bool)             { n.Schedulable = s }
func (n *BaseNodeInfo) GetDisableReason() string          { return n.DisableReason }
func (n *BaseNodeInfo) SetDisableReason(r string)         { n.DisableReason = r }
func (n *BaseNodeInfo) GetRole() NodeRole                 { return n.Role }
func (n *BaseNodeInfo) SetRole(r NodeRole)                { n.Role = r }
func (n *BaseNodeInfo) GetTaints() []Taint                { return n.Taints }
func (n *BaseNodeInfo) SetTaints(t []Taint)               { n.Taints = t }

func (n *BaseNodeInfo) AddTaint(taint Taint) {
	for i, t := range n.Taints {
		if t.Key == taint.Key {
			n.Taints[i] = taint
			return
		}
	}
	n.Taints = append(n.Taints, taint)
}

func (n *BaseNodeInfo) RemoveTaint(key string) bool {
	for i, t := range n.Taints {
		if t.Key == key {
			n.Taints = append(n.Taints[:i], n.Taints[i+1:]...)
			return true
		}
	}
	return false
}

func (n *BaseNodeInfo) HasTaint(key string) bool {
	for _, t := range n.Taints {
		if t.Key == key {
			return true
		}
	}
	return false
}

// MasterNodeInfo Master 节点信息 - 扩展 BaseNodeInfo，包含 Master 特有字段
type MasterNodeInfo struct {
	BaseNodeInfo
	IsLeader    bool   `json:"is_leader"`    // 是否为 Leader
	ClusterName string `json:"cluster_name"` // 所属集群名称
	PeerAddr    string `json:"peer_addr"`    // 集群内通信地址（用于多 Master 互联）
}

// NewMasterNodeInfo 创建 Master 节点信息
func NewMasterNodeInfo(id, hostname, ip string, grpcPort int32, clusterName string) *MasterNodeInfo {
	return &MasterNodeInfo{
		BaseNodeInfo: BaseNodeInfo{
			ID:          id,
			Hostname:    hostname,
			IP:          ip,
			GRPCPort:    grpcPort,
			Role:        NodeRoleMaster,
			State:       NodeStateRunning,
			Schedulable: false,
			Labels:      map[string]string{"type": "master", "role": "control-plane"},
			Taints:      []Taint{{Key: "node-role.kubernetes.io/master", Value: "", Effect: TaintEffectNoSchedule}},
		},
		IsLeader:    true,
		ClusterName: clusterName,
	}
}
