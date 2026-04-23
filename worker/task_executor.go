/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:10:32
 * @FilePath: \kronos-cluster\worker\task_executor.go
 * @Description: Worker 任务执行器 - 负责任务接收、执行、进度上报和取消
 *
 * 核心职责:
 *   - 注册任务类型与处理器的映射关系
 *   - 接收 Master 下发的任务并异步执行
 *   - 实时上报任务进度和状态变更
 *   - 响应任务取消请求
 *   - 管理并发任务数限制
 *
 * 执行流程:
 *   Master.DispatchTask → Worker.OnTaskDispatched → Executor.ExecuteTask
 *     → 启动 goroutine 执行 → 定期上报进度 → 完成/失败上报
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"fmt"
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"sync"
	"time"
)

// runningTask 运行中任务上下文
type runningTask struct {
	cancel context.CancelFunc
	task   *common.TaskInfo
}

// TaskExecutor 任务执行器 - 管理 Worker 端任务的执行生命周期
type TaskExecutor struct {
	transport     transport.WorkerTransport                       // 传输层（用于上报状态）
	monitor       *ResourceMonitor                                // 资源监控器
	handlers      *syncx.Map[common.TaskType, common.TaskHandler] // 任务处理器注册表
	runningTasks  *syncx.Map[string, *runningTask]                // 运行中任务（taskID → runningTask）
	maxConcurrent int                                             // 最大并发任务数
	logger        logger.ILogger                                  // 日志
	mu            sync.Mutex                                      // 保护并发启动
}

// NewTaskExecutor 创建任务执行器
func NewTaskExecutor(
	tp transport.WorkerTransport,
	monitor *ResourceMonitor,
	maxConcurrent int,
	log logger.ILogger,
) *TaskExecutor {
	return &TaskExecutor{
		transport:     tp,
		monitor:       monitor,
		handlers:      syncx.NewMap[common.TaskType, common.TaskHandler](),
		runningTasks:  syncx.NewMap[string, *runningTask](),
		maxConcurrent: maxConcurrent,
		logger:        log,
	}
}

// RegisterHandler 注册任务处理器
func (te *TaskExecutor) RegisterHandler(taskType common.TaskType, handler common.TaskHandler) error {
	if !taskType.IsValid() {
		return fmt.Errorf(common.ErrTaskTypeInvalid, taskType)
	}

	te.handlers.Store(taskType, handler)
	te.logger.InfoKV("Task handler registered", "task_type", taskType)
	return nil
}

// RegisterHandlerFunc 注册任务处理函数（便捷方法）
func (te *TaskExecutor) RegisterHandlerFunc(taskType common.TaskType, fn common.TaskHandlerFunc) error {
	return te.RegisterHandler(taskType, fn)
}

// OnTaskDispatched 处理 Master 下发的任务（作为传输层回调）
func (te *TaskExecutor) OnTaskDispatched(task *common.TaskInfo) (*common.DispatchResult, error) {
	if _, exists := te.runningTasks.Load(task.ID); exists {
		return &common.DispatchResult{
			Accepted: false,
			Message:  fmt.Sprintf(common.ErrTaskAlreadyExists, task.ID),
		}, nil
	}

	currentCount := 0
	te.runningTasks.Range(func(key string, _ *runningTask) bool {
		currentCount++
		return true
	})

	if te.maxConcurrent > 0 && currentCount >= te.maxConcurrent {
		return &common.DispatchResult{
			Accepted: false,
			Message:  "worker at max concurrency",
		}, nil
	}

	handler, ok := te.handlers.Load(task.Type)
	if !ok {
		return &common.DispatchResult{
			Accepted: false,
			Message:  fmt.Sprintf(common.ErrTaskHandlerNotFound, task.Type),
		}, nil
	}

	go te.executeTask(task, handler)

	return &common.DispatchResult{
		Accepted: true,
		Message:  "task accepted",
	}, nil
}

// OnTaskCancelled 处理 Master 取消任务请求（作为传输层回调）
func (te *TaskExecutor) OnTaskCancelled(taskID string) error {
	rt, ok := te.runningTasks.Load(taskID)
	if !ok {
		te.logger.WarnKV("Cancel request for unknown task", "task_id", taskID)
		return nil
	}

	rt.cancel()
	te.runningTasks.Delete(taskID)

	handler, ok := te.handlers.Load(common.TaskTypeCustom)
	if ok {
		handler.OnCancel(taskID)
	}

	te.reportStatus(&common.TaskStatusUpdate{
		TaskID:    taskID,
		State:     common.TaskStateCancelled,
		Timestamp: time.Now(),
	})

	te.logger.InfoKV("Task cancelled", "task_id", taskID)
	return nil
}

// executeTask 执行任务（在独立 goroutine 中运行）
func (te *TaskExecutor) executeTask(task *common.TaskInfo, handler common.TaskHandler) {
	ctx, cancel := context.WithCancel(context.Background())
	te.runningTasks.Store(task.ID, &runningTask{cancel: cancel, task: task})
	defer func() {
		te.runningTasks.Delete(task.ID)
		cancel()
	}()

	if te.monitor != nil {
		te.monitor.IncrementActiveTasks()
		defer te.monitor.DecrementActiveTasks()
	}

	te.reportStatus(&common.TaskStatusUpdate{
		TaskID:    task.ID,
		State:     common.TaskStateRunning,
		Timestamp: time.Now(),
	})

	result, err := handler.Handle(ctx, task)

	if ctx.Err() == context.Canceled {
		return
	}

	if err != nil {
		te.reportStatus(&common.TaskStatusUpdate{
			TaskID:       task.ID,
			State:        common.TaskStateFailed,
			ErrorMessage: err.Error(),
			Timestamp:    time.Now(),
		})
		te.logger.ErrorKV("Task execution failed",
			"task_id", task.ID,
			"error", err)
		return
	}

	state := common.TaskStateSucceeded
	resultData := []byte{}

	if result != nil {
		resultData = result.Data
		if result.Error != "" {
			state = common.TaskStateFailed
		}
	}

	te.reportStatus(&common.TaskStatusUpdate{
		TaskID:    task.ID,
		State:     state,
		Result:    resultData,
		Progress:  1.0,
		Timestamp: time.Now(),
	})

	te.logger.InfoKV("Task completed",
		"task_id", task.ID,
		"state", state)
}

// ReportProgress 上报任务执行进度（供业务代码调用）
func (te *TaskExecutor) ReportProgress(taskID string, progress float64) {
	te.reportStatus(&common.TaskStatusUpdate{
		TaskID:    taskID,
		State:     common.TaskStateRunning,
		Progress:  progress,
		Timestamp: time.Now(),
	})
}

// reportStatus 上报任务状态到 Master
func (te *TaskExecutor) reportStatus(update *common.TaskStatusUpdate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := te.transport.ReportTaskStatus(ctx, update); err != nil {
		te.logger.WarnKV("Failed to report task status",
			"task_id", update.TaskID,
			"state", update.State,
			"error", err)
	}
}

// RunningCount 获取当前运行中的任务数
func (te *TaskExecutor) RunningCount() int {
	count := 0
	te.runningTasks.Range(func(key string, _ *runningTask) bool {
		count++
		return true
	})
	return count
}

// IsIdle 判断执行器是否空闲（无运行中任务）
func (te *TaskExecutor) IsIdle() bool {
	return te.RunningCount() == 0
}

// Drain 排空所有运行中任务（等待完成或超时）
func (te *TaskExecutor) Drain(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if te.IsIdle() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	remaining := te.RunningCount()
	if remaining > 0 {
		return fmt.Errorf(common.ErrDrainTimeout, remaining)
	}

	return nil
}

// ListRunningTasks 列出所有运行中的任务 ID
func (te *TaskExecutor) ListRunningTasks() []string {
	taskIDs := make([]string, 0)
	te.runningTasks.Range(func(key string, _ *runningTask) bool {
		taskIDs = append(taskIDs, key)
		return true
	})
	return taskIDs
}
