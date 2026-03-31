/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 13:55:35
 * @FilePath: \go-distributed\master\pool.go
 * @Description: 节点池管理 - 线程安全的节点注册、查询和状态管理
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"sort"
	"sync"
	"time"
)

// NodePool 泛型节点池 - 使用 sync.Map 实现线程安全的节点管理
type NodePool[T common.NodeInfo] struct {
	nodes    sync.Map             // 节点存储（线程安全）
	selector NodeSelector[T]      // 节点选择器
	logger   logger.ILogger       // 日志
	config   *common.MasterConfig // Master配置
}

// NewNodePool 创建节点池
func NewNodePool[T common.NodeInfo](selector NodeSelector[T], log logger.ILogger, config *common.MasterConfig) *NodePool[T] {
	return &NodePool[T]{
		selector: selector,
		logger:   log,
		config:   config,
	}
}

// Register 注册节点到池中
func (p *NodePool[T]) Register(node T) error {
	if existingNode, loaded := p.nodes.Load(node.GetID()); loaded {
		// 检查节点是否已经离线
		existing := existingNode.(T)
		// 获取节点离线判断阈值，默认2分钟
		p.config.NodeOfflineThreshold = mathx.IfLeZero(p.config.NodeOfflineThreshold, 2*time.Minute)
		if time.Since(existing.GetLastHeartbeat()) < p.config.NodeOfflineThreshold {
			return fmt.Errorf(common.ErrNodeAlreadyRegistered, node.GetID())
		}
		// 节点已经离线，允许替换
		p.logger.WarnKV("Node ID already exists but appears offline, replacing",
			"node_id", node.GetID(),
			"last_heartbeat", existing.GetLastHeartbeat(),
			"offline_threshold", p.config.NodeOfflineThreshold)
	}

	node.SetRegisteredAt(time.Now())
	node.SetLastHeartbeat(time.Now())
	node.SetState(common.NodeStateIdle)
	p.nodes.Store(node.GetID(), node)

	return nil
}

// Unregister 从池中注销节点
func (p *NodePool[T]) Unregister(nodeID string) error {
	if _, exists := p.nodes.LoadAndDelete(nodeID); !exists {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}

	p.logger.InfoKV("Node unregistered", "node_id", nodeID)
	return nil
}

// Get 根据 ID 获取节点
func (p *NodePool[T]) Get(nodeID string) (T, bool) {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		var zero T
		return zero, false
	}
	return val.(T), true
}

// GetAll 获取所有节点
func (p *NodePool[T]) GetAll() []T {
	nodes := make([]T, 0)
	p.nodes.Range(func(_, val interface{}) bool {
		nodes = append(nodes, val.(T))
		return true
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetID() < nodes[j].GetID()
	})
	return nodes
}

// GetHealthy 获取所有健康节点
func (p *NodePool[T]) GetHealthy() []T {
	nodes := make([]T, 0)
	p.nodes.Range(func(_, val interface{}) bool {
		node := val.(T)
		if common.IsNodeHealthy(node.GetState()) {
			nodes = append(nodes, node)
		}
		return true
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetID() < nodes[j].GetID()
	})
	return nodes
}

// GetIdle 获取所有空闲节点
func (p *NodePool[T]) GetIdle() []T {
	nodes := make([]T, 0)
	p.nodes.Range(func(_, val interface{}) bool {
		node := val.(T)
		if node.GetState() == common.NodeStateIdle {
			nodes = append(nodes, node)
		}
		return true
	})
	return nodes
}

// Select 使用选择器选取指定数量的节点
func (p *NodePool[T]) Select(count int) []T {
	healthy := p.GetHealthy()
	if p.selector != nil {
		return p.selector.Select(healthy, count)
	}
	if count >= len(healthy) {
		return healthy
	}
	return healthy[:count]
}

// SelectWithFilter 使用过滤器选取节点
func (p *NodePool[T]) SelectWithFilter(count int, filter *common.NodeFilter) []T {
	if filter == nil {
		return p.Select(count)
	}

	allNodes := p.GetAll()
	filtered := make([]T, 0)
	for _, node := range allNodes {
		if filter.IsNodeValid(node) {
			filtered = append(filtered, node)
		}
	}

	if filter.PreferIdle {
		idle := make([]T, 0)
		busy := make([]T, 0)
		for _, node := range filtered {
			if node.GetState() == common.NodeStateIdle {
				idle = append(idle, node)
			} else {
				busy = append(busy, node)
			}
		}
		filtered = append(idle, busy...)
	}

	if p.selector != nil {
		return p.selector.Select(filtered, count)
	}

	if count >= len(filtered) {
		return filtered
	}
	return filtered[:count]
}

// UpdateNodeState 更新节点状态
func (p *NodePool[T]) UpdateNodeState(nodeID string, state common.NodeState) error {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	node := val.(T)
	node.SetState(state)
	p.nodes.Store(nodeID, node)
	return nil
}

// UpdateResourceUsage 更新节点资源使用情况，并根据资源自动调整节点状态
func (p *NodePool[T]) UpdateResourceUsage(nodeID string, usage *common.ResourceUsage) error {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	node := val.(T)
	node.SetResourceUsage(usage)
	if usage != nil {
		if usage.ActiveTasks == 0 {
			node.SetState(common.NodeStateIdle)
		} else if usage.CPUPercent > 90 || usage.MemoryPercent > 90 {
			node.SetState(common.NodeStateOverloaded)
		} else if usage.ActiveTasks > 0 {
			node.SetState(common.NodeStateBusy)
		}
	}
	p.nodes.Store(nodeID, node)
	return nil
}

// UpdateHeartbeat 更新节点心跳时间
func (p *NodePool[T]) UpdateHeartbeat(nodeID string) error {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	node := val.(T)
	node.SetLastHeartbeat(time.Now())
	node.SetHealthCheckFail(0)
	p.nodes.Store(nodeID, node)
	return nil
}

// MarkHealthy 将节点标记为健康状态
func (p *NodePool[T]) MarkHealthy(nodeID string) error {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	node := val.(T)
	if node.GetState() == common.NodeStateOffline || node.GetState() == common.NodeStateError {
		node.SetState(common.NodeStateIdle)
	}
	node.SetHealthCheckFail(0)
	p.nodes.Store(nodeID, node)
	return nil
}

// MarkUnhealthy 将节点标记为不健康状态
func (p *NodePool[T]) MarkUnhealthy(nodeID string) error {
	val, ok := p.nodes.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	node := val.(T)
	node.SetState(common.NodeStateError)
	p.nodes.Store(nodeID, node)
	return nil
}

// Count 获取节点总数
func (p *NodePool[T]) Count() int {
	count := 0
	p.nodes.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// Clear 清空节点池
func (p *NodePool[T]) Clear() {
	p.nodes.Range(func(key, _ interface{}) bool {
		p.nodes.Delete(key)
		return true
	})
	p.logger.Info("Node pool cleared")
}

// StartHealthCheck 启动健康检查
func (p *NodePool[T]) StartHealthCheck(ctx context.Context, health *HealthChecker[T]) {
	health.Start(ctx)
}
