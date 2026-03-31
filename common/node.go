/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 18:15:56
 * @FilePath: \go-distributed\common\node.go
 * @Description: 节点信息模型定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import "time"

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
