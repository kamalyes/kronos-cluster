/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:39:58
 * @FilePath: \go-distributed\common\filter.go
 * @Description: 节点过滤器 - 支持多维度过滤
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-toolbox/pkg/types"
)

// NodeFilter 节点过滤器 - 支持按 ID、区域、标签、状态、资源等多维度过滤
type NodeFilter struct {
	IncludeIDs       []string          `json:"include_ids" yaml:"include_ids"`               // 包含的节点 ID 列表（白名单）
	ExcludeIDs       []string          `json:"exclude_ids" yaml:"exclude_ids"`               // 排除的节点 ID 列表（黑名单）
	IncludeRegions   []string          `json:"include_regions" yaml:"include_regions"`       // 包含的区域列表
	ExcludeRegions   []string          `json:"exclude_regions" yaml:"exclude_regions"`       // 排除的区域列表
	IncludeLabels    map[string]string `json:"include_labels" yaml:"include_labels"`         // 包含的标签（键值对必须完全匹配）
	ExcludeLabels    map[string]string `json:"exclude_labels" yaml:"exclude_labels"`         // 排除的标签（键值对匹配则排除）
	RequiredStates   []NodeState       `json:"required_states" yaml:"required_states"`       // 要求的节点状态列表
	ExcludedStates   []NodeState       `json:"excluded_states" yaml:"excluded_states"`       // 排除的节点状态列表
	MinCPUCores      int               `json:"min_cpu_cores" yaml:"min_cpu_cores"`           // 最低 CPU 核心数
	MinMemory        int64             `json:"min_memory" yaml:"min_memory"`                 // 最低内存大小（字节）
	MaxCPUPercent    float64           `json:"max_cpu_percent" yaml:"max_cpu_percent"`       // 最大 CPU 使用率（0-100）
	MaxMemoryPercent float64           `json:"max_memory_percent" yaml:"max_memory_percent"` // 最大内存使用率（0-100）
	MaxLoad          float64           `json:"max_load" yaml:"max_load"`                     // 最大负载均值
	MaxActiveTasks   int               `json:"max_active_tasks" yaml:"max_active_tasks"`     // 最大活跃任务数
	PreferIdle       bool              `json:"prefer_idle" yaml:"prefer_idle"`               // 是否优先选择空闲节点
}

// IsNodeValid 判断节点是否通过过滤器（所有条件必须满足）
func (f *NodeFilter) IsNodeValid(node NodeInfo) bool {
	if !f.checkIDFilter(node) {
		return false
	}
	if !f.checkRegionFilter(node) {
		return false
	}
	if !f.checkLabelFilter(node) {
		return false
	}
	if !f.checkStateFilter(node) {
		return false
	}
	return f.checkResourceLimits(node)
}

// checkIDFilter 检查节点 ID 是否通过过滤器
func (f *NodeFilter) checkIDFilter(node NodeInfo) bool {
	if len(f.IncludeIDs) > 0 && !types.Contains(f.IncludeIDs, node.GetID()) {
		return false
	}
	return !types.Contains(f.ExcludeIDs, node.GetID())
}

// checkRegionFilter 检查节点区域是否通过过滤器
func (f *NodeFilter) checkRegionFilter(node NodeInfo) bool {
	if len(f.IncludeRegions) > 0 && !types.Contains(f.IncludeRegions, node.GetRegion()) {
		return false
	}
	return !types.Contains(f.ExcludeRegions, node.GetRegion())
}

// checkLabelFilter 检查节点标签是否通过过滤器
func (f *NodeFilter) checkLabelFilter(node NodeInfo) bool {
	for k, v := range f.IncludeLabels {
		if slaveVal, ok := node.GetLabels()[k]; !ok || slaveVal != v {
			return false
		}
	}
	for k, v := range f.ExcludeLabels {
		if slaveVal, ok := node.GetLabels()[k]; ok && slaveVal == v {
			return false
		}
	}
	return true
}

// checkStateFilter 检查节点状态是否通过过滤器
func (f *NodeFilter) checkStateFilter(node NodeInfo) bool {
	if len(f.RequiredStates) > 0 && !types.Contains(f.RequiredStates, node.GetState()) {
		return false
	}
	return !types.Contains(f.ExcludedStates, node.GetState())
}

// checkResourceLimits 检查节点资源是否满足限制条件
func (f *NodeFilter) checkResourceLimits(node NodeInfo) bool {
	usage := node.GetResourceUsage()
	if usage == nil {
		return true
	}
	if f.MaxCPUPercent > 0 && usage.CPUPercent > f.MaxCPUPercent {
		return false
	}
	if f.MaxMemoryPercent > 0 && usage.MemoryPercent > f.MaxMemoryPercent {
		return false
	}
	if f.MaxLoad > 0 && usage.LoadAvg1m > f.MaxLoad {
		return false
	}
	if f.MaxActiveTasks > 0 && usage.ActiveTasks > f.MaxActiveTasks {
		return false
	}
	return true
}

// NewNodeFilterFromRequest 从 proto ListNodesRequest 构建 NodeFilter
func NewNodeFilterFromRequest(req *pb.ListNodesRequest) *NodeFilter {
	f := &NodeFilter{}

	if !req.IncludeOffline {
		f.ExcludedStates = append(f.ExcludedStates, NodeStateOffline)
	}

	if len(req.FilterStates) > 0 {
		f.RequiredStates = make([]NodeState, 0, len(req.FilterStates))
		for _, state := range req.FilterStates {
			f.RequiredStates = append(f.RequiredStates, ProtoNodeStateToCommon(state))
		}
	}

	if req.RegionFilter != "" {
		f.IncludeRegions = []string{req.RegionFilter}
	}

	if len(req.LabelSelector) > 0 {
		f.IncludeLabels = req.LabelSelector
	}

	return f
}

// FilterNodes 过滤节点列表，返回通过过滤器的节点
func (f *NodeFilter) FilterNodes(nodes []NodeInfo) []NodeInfo {
	filtered := make([]NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		if f.IsNodeValid(node) {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

// TaskFilter 任务过滤器 - 支持按状态和类型过滤任务
type TaskFilter struct {
	RequiredStates []TaskState
	TaskType       string
}

// NewTaskFilterFromRequest 从 proto ListTasksRequest 构建 TaskFilter
func NewTaskFilterFromRequest(req *pb.ListTasksRequest) *TaskFilter {
	f := &TaskFilter{}

	if len(req.FilterStates) > 0 {
		f.RequiredStates = make([]TaskState, 0, len(req.FilterStates))
		for _, state := range req.FilterStates {
			f.RequiredStates = append(f.RequiredStates, ProtoTaskStateToCommon(state))
		}
	}

	if req.TaskTypeFilter != "" {
		f.TaskType = req.TaskTypeFilter
	}

	return f
}

// Apply 对任务列表应用过滤器
func (f *TaskFilter) Apply(tasks []*TaskInfo) []*TaskInfo {
	result := tasks

	if len(f.RequiredStates) > 0 {
		result = f.filterByState(result)
	}

	if f.TaskType != "" {
		result = f.filterByType(result)
	}

	return result
}

// filterByState 按状态过滤任务
func (f *TaskFilter) filterByState(tasks []*TaskInfo) []*TaskInfo {
	stateSet := make(map[TaskState]bool, len(f.RequiredStates))
	for _, s := range f.RequiredStates {
		stateSet[s] = true
	}
	filtered := make([]*TaskInfo, 0, len(tasks))
	for _, t := range tasks {
		if stateSet[t.State] {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

// filterByType 按类型过滤任务
func (f *TaskFilter) filterByType(tasks []*TaskInfo) []*TaskInfo {
	filtered := make([]*TaskInfo, 0, len(tasks))
	for _, t := range tasks {
		if string(t.Type) == f.TaskType {
			filtered = append(filtered, t)
		}
	}
	return filtered
}
