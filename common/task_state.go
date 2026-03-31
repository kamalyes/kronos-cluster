/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 00:00:00
 * @FilePath: \go-distributed\common\task_state.go
 * @Description: 任务状态枚举与状态机定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"fmt"
)

// TaskState 任务状态类型
type TaskState string

const (
	TaskStatePending    TaskState = "pending"    // 待调度 - 任务已创建，等待 Master 分配节点
	TaskStateScheduled  TaskState = "scheduled"  // 已调度 - Master 已选定目标节点
	TaskStateDispatched TaskState = "dispatched" // 已下发 - 任务已发送到 Worker，等待执行
	TaskStateRunning    TaskState = "running"    // 执行中 - Worker 正在执行任务
	TaskStateSucceeded  TaskState = "succeeded"  // 已成功 - 任务执行成功（终态）
	TaskStateFailed     TaskState = "failed"     // 已失败 - 任务执行失败（终态）
	TaskStateCancelled  TaskState = "cancelled"  // 已取消 - 任务被主动取消（终态）
	TaskStateTimeout    TaskState = "timeout"    // 已超时 - 任务执行超时（终态）
	TaskStateRetrying   TaskState = "retrying"   // 重试中 - 任务失败后进入重试流程
)

// String 返回状态字符串表示
func (s TaskState) String() string {
	return string(s)
}

// IsTerminal 判断任务状态是否为终态（不可再转换）
func (s TaskState) IsTerminal() bool {
	switch s {
	case TaskStateSucceeded, TaskStateFailed, TaskStateCancelled, TaskStateTimeout:
		return true
	default:
		return false
	}
}

// taskTransitions 定义合法的状态转换映射
//
// 状态转换图：
//
//	Pending → Scheduled → Dispatched → Running → Succeeded
//	                                  ↘ Failed → Retrying → Pending
//	                                  ↘ Timeout → Retrying → Pending
//	                                  ↘ Cancelled
var taskTransitions = map[TaskState][]TaskState{
	TaskStatePending:    {TaskStateScheduled, TaskStateCancelled},
	TaskStateScheduled:  {TaskStateDispatched, TaskStateFailed, TaskStateCancelled, TaskStatePending},
	TaskStateDispatched: {TaskStateRunning, TaskStateFailed, TaskStateCancelled, TaskStateTimeout},
	TaskStateRunning:    {TaskStateSucceeded, TaskStateFailed, TaskStateCancelled, TaskStateTimeout},
	TaskStateFailed:     {TaskStateRetrying},
	TaskStateRetrying:   {TaskStatePending, TaskStateCancelled},
	TaskStateTimeout:    {TaskStateRetrying},
	TaskStateSucceeded:  {},
	TaskStateCancelled:  {},
}

// CanTransitionTo 判断当前状态是否可以转换到目标状态
func (s TaskState) CanTransitionTo(target TaskState) bool {
	allowed, ok := taskTransitions[s]
	if !ok {
		return false
	}
	for _, state := range allowed {
		if state == target {
			return true
		}
	}
	return false
}

// ValidateTransition 校验状态转换合法性，不合法则返回错误
func (s TaskState) ValidateTransition(target TaskState) error {
	if s == target {
		return nil
	}
	if s.IsTerminal() {
		return fmt.Errorf(ErrTaskStateTerminal, s)
	}
	if !s.CanTransitionTo(target) {
		return fmt.Errorf(ErrTaskStateTransition, s, target)
	}
	return nil
}
