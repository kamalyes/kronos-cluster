/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 10:06:15
 * @FilePath: \go-distributed\common\states.go
 * @Description: 节点状态枚举定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

// NodeState 节点状态类型
type NodeState string

const (
	NodeStateIdle        NodeState = "idle"        // 空闲 - 节点无任务运行
	NodeStateRunning     NodeState = "running"     // 运行中 - 节点正在执行任务
	NodeStateBusy        NodeState = "busy"        // 繁忙 - 节点负载较高但仍可接受任务
	NodeStateStopping    NodeState = "stopping"    // 停止中 - 节点正在优雅关闭
	NodeStateError       NodeState = "error"       // 错误 - 节点发生异常
	NodeStateOffline     NodeState = "offline"     // 离线 - 节点已断开连接
	NodeStateOverloaded  NodeState = "overloaded"  // 过载 - 节点资源超限
	NodeStateUnreachable NodeState = "unreachable" // 不可达 - 节点无法通信
	NodeStateDraining    NodeState = "draining"    // 排空中 - 正在完成已有任务，不再接受新任务
)

// String 返回状态字符串表示
func (s NodeState) String() string {
	return string(s)
}

// IsNodeHealthy 判断节点是否处于健康状态（空闲、运行中、繁忙均视为健康）
func IsNodeHealthy(state NodeState) bool {
	switch state {
	case NodeStateIdle, NodeStateRunning, NodeStateBusy:
		return true
	default:
		return false
	}
}

// IsNodeAvailable 判断节点是否可接受新任务（空闲和繁忙状态可接受）
func IsNodeAvailable(state NodeState) bool {
	switch state {
	case NodeStateIdle, NodeStateBusy:
		return true
	default:
		return false
	}
}
