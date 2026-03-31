/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:15:16
 * @FilePath: \go-distributed\common\cluster.go
 * @Description: 集群统计收集器
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

// ClusterStatsCollector 集群统计收集器 - 遍历节点聚合状态、区域、资源等统计
type ClusterStatsCollector struct {
	HealthyNodes  int32
	OfflineNodes  int32
	DrainingNodes int32
	NodesByRegion map[string]int32
	NodesByState  map[string]int32
	totalCPU      float64
	totalMem      float64
	cpuCount      int
	memCount      int
}

// NewClusterStatsCollector 创建集群统计收集器
func NewClusterStatsCollector() *ClusterStatsCollector {
	return &ClusterStatsCollector{
		NodesByRegion: make(map[string]int32),
		NodesByState:  make(map[string]int32),
	}
}

// Collect 遍历节点列表收集统计
func (c *ClusterStatsCollector) Collect(nodes []NodeInfo, includeResourceStats bool) {
	for _, node := range nodes {
		c.collectNode(node, includeResourceStats)
	}
}

// collectNode 收集单个节点的统计信息
func (c *ClusterStatsCollector) collectNode(node NodeInfo, includeResourceStats bool) {
	state := node.GetState()
	c.NodesByState[string(state)]++
	c.classifyState(state)

	if region := node.GetRegion(); region != "" {
		c.NodesByRegion[region]++
	}

	if includeResourceStats {
		c.accumulateResources(node)
	}
}

// classifyState 按状态分类节点计数
func (c *ClusterStatsCollector) classifyState(state NodeState) {
	switch state {
	case NodeStateRunning, NodeStateIdle, NodeStateBusy:
		c.HealthyNodes++
	case NodeStateOffline:
		c.OfflineNodes++
	case NodeStateDraining:
		c.DrainingNodes++
	}
}

// accumulateResources 累加节点资源统计
func (c *ClusterStatsCollector) accumulateResources(node NodeInfo) {
	usage := node.GetResourceUsage()
	if usage == nil {
		return
	}
	c.totalCPU += usage.CPUPercent
	c.totalMem += usage.MemoryPercent
	c.cpuCount++
	c.memCount++
}

// AvgCPU 返回平均 CPU 使用率
func (c *ClusterStatsCollector) AvgCPU() float64 {
	if c.cpuCount == 0 {
		return 0
	}
	return c.totalCPU / float64(c.cpuCount)
}

// AvgMemory 返回平均内存使用率
func (c *ClusterStatsCollector) AvgMemory() float64 {
	if c.memCount == 0 {
		return 0
	}
	return c.totalMem / float64(c.memCount)
}
