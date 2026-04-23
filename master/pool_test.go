/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 13:26:15
 * @FilePath: \kronos-cluster\master\pool_test.go
 * @Description: 节点池管理单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	testRegion = "beijing"
	testIP     = "10.0.0.1"
)

func newTestPool() *NodePool[*common.BaseNodeInfo] {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	return NewNodePool(selector, log, config)
}

func newTestBaseNode() *common.BaseNodeInfo {
	return &common.BaseNodeInfo{
		ID:       newTestNodeID(),
		Hostname: "host-" + newTestNodeID(),
		IP:       testIP,
		Region:   testRegion,
		Labels:   map[string]string{"env": "prod"},
	}
}

func newTestNodeID() string {
	return random.UUID()
}

func TestNodePoolRegister(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	err := pool.Register(node)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Count())
}

func TestNodePoolRegisterDuplicate(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	err := pool.Register(node)
	assert.NoError(t, err)

	err = pool.Register(node)
	assert.Error(t, err)
}

func TestNodePoolUnregister(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	pool.Register(node)
	err := pool.Unregister(node.ID)
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Count())
}

func TestNodePoolUnregisterNotFound(t *testing.T) {
	pool := newTestPool()

	err := pool.Unregister(random.UUID())
	assert.Error(t, err)
}

func TestNodePoolGet(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	pool.Register(node)

	found, ok := pool.Get(node.ID)
	assert.True(t, ok)
	assert.Equal(t, node.ID, found.GetID())
}

func TestNodePoolGetNotFound(t *testing.T) {
	pool := newTestPool()

	_, ok := pool.Get(random.UUID())
	assert.False(t, ok)
}

func TestNodePoolGetAll(t *testing.T) {
	pool := newTestPool()
	pool.Register(newTestBaseNode())
	pool.Register(newTestBaseNode())
	pool.Register(newTestBaseNode())

	all := pool.GetAll()
	assert.Len(t, all, 3)
}

func TestNodePoolGetAllEmpty(t *testing.T) {
	pool := newTestPool()

	all := pool.GetAll()
	assert.Empty(t, all)
}

func TestNodePoolGetHealthy(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetState(common.NodeStateRunning)

	node3 := newTestBaseNode()
	pool.Register(node3)
	node3.SetState(common.NodeStateError)

	healthy := pool.GetHealthy()
	assert.Len(t, healthy, 2)
}

func TestNodePoolGetIdle(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetState(common.NodeStateRunning)

	idle := pool.GetIdle()
	assert.Len(t, idle, 1)
	assert.Equal(t, node1.ID, idle[0].GetID())
}

func TestNodePoolUpdateNodeState(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)

	err := pool.UpdateNodeState(node.ID, common.NodeStateRunning)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateRunning, found.GetState())
}

func TestNodePoolUpdateNodeStateNotFound(t *testing.T) {
	pool := newTestPool()

	err := pool.UpdateNodeState(random.UUID(), common.NodeStateRunning)
	assert.Error(t, err)
}

func TestNodePoolUpdateResourceUsage(t *testing.T) {
	pool := newTestPool()

	node := newTestBaseNode()

	pool.Register(node)

	usage := &common.ResourceUsage{CPUPercent: 50, MemoryPercent: 60, ActiveTasks: 5}
	err := pool.UpdateResourceUsage(node.ID, usage)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, 50.0, found.GetResourceUsage().CPUPercent)
	assert.Equal(t, common.NodeStateBusy, found.GetState())
}

func TestNodePoolUpdateResourceUsageOverloaded(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	pool.Register(node)

	usage := &common.ResourceUsage{CPUPercent: 95, MemoryPercent: 60, ActiveTasks: 5}
	err := pool.UpdateResourceUsage(node.ID, usage)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateOverloaded, found.GetState())
}

func TestNodePoolUpdateResourceUsageIdle(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	pool.Register(node)
	node.SetState(common.NodeStateRunning)

	usage := &common.ResourceUsage{CPUPercent: 30, MemoryPercent: 40, ActiveTasks: 0}
	err := pool.UpdateResourceUsage(node.ID, usage)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateIdle, found.GetState())
}

func TestNodePoolUpdateResourceUsageNotFound(t *testing.T) {
	pool := newTestPool()

	usage := &common.ResourceUsage{CPUPercent: 50}
	err := pool.UpdateResourceUsage(random.UUID(), usage)
	assert.Error(t, err)
}

func TestNodePoolUpdateHeartbeat(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)
	node.SetHealthCheckFail(3)

	before := time.Now()
	err := pool.UpdateHeartbeat(node.ID)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.True(t, found.GetLastHeartbeat().After(before) || found.GetLastHeartbeat().Equal(before))
	assert.Equal(t, 0, found.GetHealthCheckFail())
}

func TestNodePoolUpdateHeartbeatNotFound(t *testing.T) {
	pool := newTestPool()

	err := pool.UpdateHeartbeat(random.UUID())
	assert.Error(t, err)
}

func TestNodePoolMarkHealthy(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateError)
	node.SetHealthCheckFail(5)

	err := pool.MarkHealthy(node.ID)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateIdle, found.GetState())
	assert.Equal(t, 0, found.GetHealthCheckFail())
}

func TestNodePoolMarkHealthyFromOffline(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateOffline)

	err := pool.MarkHealthy(node.ID)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateIdle, found.GetState())
}

func TestNodePoolMarkHealthyRunningNode(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateRunning)

	err := pool.MarkHealthy(node.ID)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateRunning, found.GetState())
}

func TestNodePoolMarkHealthyNotFound(t *testing.T) {
	pool := newTestPool()

	err := pool.MarkHealthy(random.UUID())
	assert.Error(t, err)
}

func TestNodePoolMarkUnhealthy(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()
	pool.Register(node)

	err := pool.MarkUnhealthy(node.ID)
	assert.NoError(t, err)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateError, found.GetState())
}

func TestNodePoolMarkUnhealthyNotFound(t *testing.T) {
	pool := newTestPool()

	err := pool.MarkUnhealthy(random.UUID())
	assert.Error(t, err)
}

func TestNodePoolCount(t *testing.T) {
	pool := newTestPool()
	node1 := newTestBaseNode()
	node2 := newTestBaseNode()

	assert.Equal(t, 0, pool.Count())

	pool.Register(node1)
	assert.Equal(t, 1, pool.Count())

	pool.Register(node2)
	assert.Equal(t, 2, pool.Count())

	pool.Unregister(node1.ID)
	assert.Equal(t, 1, pool.Count())
}

func TestNodePoolSelect(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	pool.Register(node1)
	node1.SetCurrentLoad(0.3)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetCurrentLoad(0.7)

	node3 := newTestBaseNode()
	pool.Register(node3)
	node3.SetState(common.NodeStateError)

	selected := pool.Select(1)
	assert.Len(t, selected, 1)
	assert.Equal(t, node1.ID, selected[0].GetID())
}

func TestNodePoolSelectMoreThanAvailable(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	pool.Register(node1)

	selected := pool.Select(5)
	assert.Len(t, selected, 1)
}

func TestNodePoolSelectWithFilter(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	node1.SetRegion(testRegion)
	pool.Register(node1)

	node2 := newTestBaseNode()
	node2.SetRegion("shanghai")
	pool.Register(node2)

	filter := &common.NodeFilter{IncludeRegions: []string{testRegion}}
	selected := pool.SelectWithFilter(5, filter)
	assert.Len(t, selected, 1)
	assert.Equal(t, testRegion, selected[0].GetRegion())
}

func TestNodePoolSelectWithFilterNil(t *testing.T) {
	pool := newTestPool()

	node1 := newTestBaseNode()
	pool.Register(node1)

	selected := pool.SelectWithFilter(5, nil)
	assert.Len(t, selected, 1)
}

func TestNodePoolRegisterSetsDefaults(t *testing.T) {
	pool := newTestPool()
	node := newTestBaseNode()

	pool.Register(node)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateIdle, found.GetState())
	assert.False(t, found.GetLastHeartbeat().IsZero())
	assert.False(t, found.GetRegisteredAt().IsZero())
}
