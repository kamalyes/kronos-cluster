/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 11:18:55
 * @FilePath: \go-distributed\common\task.go
 * @Description: 任务模型定义 - 任务信息、任务类型枚举、任务结果
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"context"
	"time"
)

// TaskType 任务类型枚举
type TaskType string

const (
	TaskTypeCommand   TaskType = "command"   // 命令执行任务
	TaskTypeScript    TaskType = "script"    // 脚本执行任务
	TaskTypeHTTP      TaskType = "http"      // HTTP 请求任务
	TaskTypeScheduler TaskType = "scheduler" // 调度任务
	TaskTypeStress    TaskType = "stress"    // 压测任务
	TaskTypeCustom    TaskType = "custom"    // 自定义任务
)

// String 返回任务类型字符串表示
func (t TaskType) String() string {
	return string(t)
}

// IsValid 判断任务类型是否合法
func (t TaskType) IsValid() bool {
	switch t {
	case TaskTypeCommand, TaskTypeScript, TaskTypeHTTP,
		TaskTypeScheduler, TaskTypeStress, TaskTypeCustom:
		return true
	default:
		return false
	}
}

// TaskInfo 任务信息
type TaskInfo struct {
	ID         string            `json:"id"`          // 任务唯一标识（雪花算法生成）
	Type       TaskType          `json:"type"`        // 任务类型
	Payload    []byte            `json:"payload"`     // 任务载荷数据
	Priority   int32             `json:"priority"`    // 优先级（0=最低，数值越大优先级越高）
	MaxRetries int32             `json:"max_retries"` // 最大重试次数
	RetryCount int32             `json:"retry_count"` // 当前重试次数
	Timeout    time.Duration     `json:"timeout"`     // 任务超时时间
	Metadata   map[string]string `json:"metadata"`    // 任务元数据
	TargetNode string            `json:"target_node"` // 目标节点 ID（空=由 Master 调度）
	State      TaskState         `json:"state"`       // 当前任务状态
	CreatedAt  time.Time         `json:"created_at"`  // 创建时间
	StartedAt  time.Time         `json:"started_at"`  // 开始执行时间
	FinishedAt time.Time         `json:"finished_at"` // 完成时间
	Result     []byte            `json:"result"`      // 执行结果数据
	Error      string            `json:"error"`       // 错误信息
	Progress   float64           `json:"progress"`    // 执行进度（0.0 ~ 1.0）
}

// NewTaskInfo 创建任务信息（仅初始化必填字段）
func NewTaskInfo(id string, taskType TaskType, payload []byte) *TaskInfo {
	return &TaskInfo{
		ID:         id,
		Type:       taskType,
		Payload:    payload,
		Priority:   0,
		MaxRetries: 0,
		RetryCount: 0,
		Timeout:    0,
		Metadata:   make(map[string]string),
		State:      TaskStatePending,
		CreatedAt:  time.Now(),
		Progress:   0,
	}
}

// SetState 安全地设置任务状态（校验状态转换合法性）
func (t *TaskInfo) SetState(state TaskState) error {
	if err := t.State.ValidateTransition(state); err != nil {
		return err
	}
	t.State = state
	return nil
}

// IsRetryable 判断任务是否可以重试
func (t *TaskInfo) IsRetryable() bool {
	return t.RetryCount < t.MaxRetries
}

// IncrementRetry 增加重试计数并重置状态为 Pending
func (t *TaskInfo) IncrementRetry() {
	t.RetryCount++
	t.State = TaskStatePending
}

// IsTimedOut 判断任务是否超时
func (t *TaskInfo) IsTimedOut() bool {
	if t.Timeout <= 0 || t.StartedAt.IsZero() {
		return false
	}
	return time.Since(t.StartedAt) > t.Timeout
}

// Duration 获取任务执行耗时
func (t *TaskInfo) Duration() time.Duration {
	if t.StartedAt.IsZero() {
		return 0
	}
	if t.FinishedAt.IsZero() {
		return time.Since(t.StartedAt)
	}
	return t.FinishedAt.Sub(t.StartedAt)
}

// TaskResult 任务执行结果
type TaskResult struct {
	Data    []byte         `json:"data"`    // 结果数据
	Error   string         `json:"error"`   // 错误信息
	Metrics map[string]any `json:"metrics"` // 执行指标
}

// TaskStatusUpdate 任务状态更新（Worker → Master）
type TaskStatusUpdate struct {
	TaskID       string    `json:"task_id"`       // 任务 ID
	NodeID       string    `json:"node_id"`       // 节点 ID
	State        TaskState `json:"state"`         // 任务状态
	Result       []byte    `json:"result"`        // 结果数据
	ErrorMessage string    `json:"error_message"` // 错误消息
	Progress     float64   `json:"progress"`      // 执行进度（0.0 ~ 1.0）
	Timestamp    time.Time `json:"timestamp"`     // 更新时间戳
}

// DispatchResult 任务下发结果（Worker 接受/拒绝）
type DispatchResult struct {
	Accepted bool   `json:"accepted"` // 是否接受任务
	Message  string `json:"message"`  // 响应描述
}

// TaskHandler 任务处理器接口（Worker 端实现）
type TaskHandler interface {
	// Handle 执行任务，返回任务结果
	Handle(ctx context.Context, task *TaskInfo) (*TaskResult, error)
	// OnCancel 任务取消回调
	OnCancel(taskID string)
}

// TaskHandlerFunc 任务处理函数类型（便捷包装）
type TaskHandlerFunc func(ctx context.Context, task *TaskInfo) (*TaskResult, error)

// Handle 执行任务
func (f TaskHandlerFunc) Handle(ctx context.Context, task *TaskInfo) (*TaskResult, error) {
	return f(ctx, task)
}

// OnCancel 任务取消回调（默认空实现）
func (f TaskHandlerFunc) OnCancel(taskID string) {}

// TaskStats 任务统计信息
type TaskStats struct {
	Total      int `json:"total"`      // 总任务数
	Pending    int `json:"pending"`    // 待调度
	Scheduled  int `json:"scheduled"`  // 已调度
	Dispatched int `json:"dispatched"` // 已下发
	Running    int `json:"running"`    // 执行中
	Succeeded  int `json:"succeeded"`  // 已成功
	Failed     int `json:"failed"`     // 已失败
	Cancelled  int `json:"cancelled"`  // 已取消
	Timeout    int `json:"timeout"`    // 已超时
	Retrying   int `json:"retrying"`   // 重试中
}
