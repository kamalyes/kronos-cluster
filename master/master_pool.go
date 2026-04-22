/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-04-21 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-04-21 00:00:00
 * @FilePath: \go-distributed\master\master_pool.go
 * @Description: Master 节点池 - 管理集群中所有 Master 节点信息
 *
 * 支持多主多从架构：
 *   - Master 启动时自注册到 MasterPool
 *   - 其他 Master 节点可通过集群内通信注册
 *   - 提供 Master 节点发现和 Leader 选举基础
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
)

// MasterPool Master 节点池 - 管理集群中所有 Master 节点
type MasterPool struct {
	masters sync.Map
	logger  logger.ILogger
}

// NewMasterPool 创建 Master 节点池
func NewMasterPool(log logger.ILogger) *MasterPool {
	return &MasterPool{
		logger: log,
	}
}

// Register 注册 Master 节点
func (p *MasterPool) Register(master *common.MasterNodeInfo) error {
	if _, loaded := p.masters.Load(master.GetID()); loaded {
		return fmt.Errorf("master node %s already registered", master.GetID())
	}

	master.SetRegisteredAt(time.Now())
	master.SetLastHeartbeat(time.Now())
	p.masters.Store(master.GetID(), master)

	p.logger.InfoKV("Master node registered",
		"node_id", master.GetID(),
		"hostname", master.GetHostname(),
		"is_leader", master.IsLeader)

	return nil
}

// Unregister 注销 Master 节点
func (p *MasterPool) Unregister(nodeID string) error {
	if _, exists := p.masters.LoadAndDelete(nodeID); !exists {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}

	p.logger.InfoKV("Master node unregistered", "node_id", nodeID)
	return nil
}

// Get 获取 Master 节点
func (p *MasterPool) Get(nodeID string) (*common.MasterNodeInfo, bool) {
	val, ok := p.masters.Load(nodeID)
	if !ok {
		return nil, false
	}
	return val.(*common.MasterNodeInfo), true
}

// GetAll 获取所有 Master 节点
func (p *MasterPool) GetAll() []*common.MasterNodeInfo {
	masters := make([]*common.MasterNodeInfo, 0)
	p.masters.Range(func(_, val interface{}) bool {
		masters = append(masters, val.(*common.MasterNodeInfo))
		return true
	})
	sort.Slice(masters, func(i, j int) bool {
		return masters[i].GetID() < masters[j].GetID()
	})
	return masters
}

// GetAllAsNodeInfo 获取所有 Master 节点（作为 NodeInfo 接口）
func (p *MasterPool) GetAllAsNodeInfo() []common.NodeInfo {
	masters := p.GetAll()
	result := make([]common.NodeInfo, len(masters))
	for i, m := range masters {
		result[i] = m
	}
	return result
}

// GetLeader 获取 Leader Master 节点
func (p *MasterPool) GetLeader() (*common.MasterNodeInfo, bool) {
	var leader *common.MasterNodeInfo
	p.masters.Range(func(_, val interface{}) bool {
		m := val.(*common.MasterNodeInfo)
		if m.IsLeader {
			leader = m
			return false
		}
		return true
	})
	return leader, leader != nil
}

// Count 获取 Master 节点总数
func (p *MasterPool) Count() int {
	count := 0
	p.masters.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// UpdateHeartbeat 更新 Master 节点心跳
func (p *MasterPool) UpdateHeartbeat(nodeID string) error {
	val, ok := p.masters.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}
	master := val.(*common.MasterNodeInfo)
	master.SetLastHeartbeat(time.Now())
	p.masters.Store(nodeID, master)
	return nil
}

// Clear 清空 Master 节点池
func (p *MasterPool) Clear() {
	p.masters.Range(func(key, _ interface{}) bool {
		p.masters.Delete(key)
		return true
	})
	p.logger.Info("Master pool cleared")
}
