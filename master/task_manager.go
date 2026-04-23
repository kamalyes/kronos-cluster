/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 18:52:15
 * @FilePath: \kronos-cluster\master\task_manager.go
 * @Description: Master 任务管理器 - 负责任务创建、调度、追踪、重试和超时管理
 *
 * 核心设计:
 *   - 任务队列通过 TaskStore 持久化，Master 宕机后可恢复
 *   - 使用雪花算法生成全局唯一可排序的任务 ID
 *   - 调度循环从持久化队列取出任务，通过节点选择器分配到最优节点
 *   - 超时检查循环定期扫描运行中任务，超时自动标记并触发重试
 *   - Master 重启时调用 Recover 恢复未完成任务
 *
 * 任务生命周期:
 *   Pending → Scheduled → Dispatched → Running → Succeeded/Failed
 *                                  ↘ Timeout    ↘ Cancelled
 *                      Failed/Timeout → Retrying → Pending
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"fmt"
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/idgen"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"sync"
	"time"
)

// NodeProvider 节点提供者接口 - TaskManager 和 AdminService 依赖此接口而非具体 NodePool
// 这样 Master[T] 可以将 *NodePool[T] 适配为 NodeProvider，避免泛型类型不匹配
type NodeProvider interface {
	// Select 选择指定数量的节点
	Select(count int) []common.NodeInfo
	// Get 获取指定节点
	Get(nodeID string) (common.NodeInfo, bool)
	// GetAll 获取所有节点
	GetAll() []common.NodeInfo
}

// TaskManager 任务管理器 - 管理任务的全生命周期
type TaskManager struct {
	pool               NodeProvider              // 节点提供者（用于调度选节点）
	transport          transport.MasterTransport // 传输层（用于下发任务和取消任务）
	store              TaskStore                 // 持久化存储（任务队列 + 任务详情）
	idGenerator        *idgen.SnowflakeGenerator // 雪花算法 ID 生成器
	logger             logger.ILogger            // 日志
	running            *syncx.Bool               // 运行状态
	cancelFunc         context.CancelFunc        // 取消函数
	mu                 sync.Mutex                // 保护调度操作的互斥锁
	candidateNodeCount int                       // 调度时候选节点数量
}

// NewTaskManager 创建任务管理器
// store: 持久化存储实现（MemoryTaskStore 用于测试，RedisTaskStore 用于生产）
// candidateNodeCount: 调度时候选节点数量，用于跳过没有活跃连接的节点
func NewTaskManager(
	pool NodeProvider,
	tp transport.MasterTransport,
	store TaskStore,
	log logger.ILogger,
	candidateNodeCount int,
) *TaskManager {
	if candidateNodeCount <= 0 {
		candidateNodeCount = 10
	}
	return &TaskManager{
		pool:               pool,
		transport:          tp,
		store:              store,
		idGenerator:        idgen.NewSnowflakeGenerator(1, 1),
		logger:             log,
		running:            syncx.NewBool(false),
		candidateNodeCount: candidateNodeCount,
	}
}

// Start 启动任务管理器（恢复未完成任务 → 启动调度循环 → 启动超时检查）
func (tm *TaskManager) Start(ctx context.Context) error {
	if !tm.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyRunning, "task manager")
	}

	ctx, cancel := context.WithCancel(ctx)
	tm.cancelFunc = cancel

	if err := tm.store.Recover(ctx); err != nil {
		tm.logger.WarnKV("Failed to recover tasks", "error", err)
	}

	go tm.scheduleLoop(ctx)
	go tm.timeoutCheckLoop(ctx)

	tm.logger.Info("Task manager started")
	return nil
}

// Stop 停止任务管理器
func (tm *TaskManager) Stop() error {
	if !tm.running.CAS(true, false) {
		return fmt.Errorf(common.ErrNotRunning, "task manager")
	}

	if tm.cancelFunc != nil {
		tm.cancelFunc()
	}

	if err := tm.store.Close(); err != nil {
		tm.logger.WarnKV("Failed to close task store", "error", err)
	}

	tm.logger.Info("Task manager stopped")
	return nil
}

// SubmitTask 提交新任务
// taskType: 任务类型
// payload: 任务载荷
// opts: 可选配置（优先级、超时、重试、目标节点等）
func (tm *TaskManager) SubmitTask(taskType common.TaskType, payload []byte, opts ...TaskOption) (*common.TaskInfo, error) {
	if !taskType.IsValid() {
		return nil, fmt.Errorf(common.ErrTaskTypeInvalid, taskType)
	}

	taskID := tm.idGenerator.GenerateRequestID()
	task := common.NewTaskInfo(taskID, taskType, payload)

	for _, opt := range opts {
		opt(task)
	}

	ctx := context.Background()

	if err := tm.store.SaveTask(ctx, task); err != nil {
		return nil, errorx.WrapError(common.ErrTaskDispatchFailed, err)
	}

	if task.TargetNode != "" {
		if err := tm.dispatchToNode(ctx, task, task.TargetNode); err != nil {
			_ = tm.store.DeleteTask(ctx, task.ID)
			return nil, err
		}
	} else {
		if err := tm.store.EnqueuePending(ctx, task); err != nil {
			return nil, errorx.WrapError(common.ErrTaskDispatchFailed, err)
		}
	}

	tm.logger.InfoKV("Task submitted",
		"task_id", task.ID,
		"task_type", task.Type,
		"target_node", task.TargetNode)

	return task, nil
}

// CancelTask 取消任务
func (tm *TaskManager) CancelTask(taskID string) error {
	ctx := context.Background()

	task, err := tm.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	if task.State.IsTerminal() {
		return fmt.Errorf(common.ErrTaskStateTerminal, task.State)
	}

	if err := task.SetState(common.TaskStateCancelled); err != nil {
		return err
	}

	task.FinishedAt = time.Now()

	if err := tm.store.SaveTask(ctx, task); err != nil {
		tm.logger.WarnKV("Failed to persist task cancellation", "task_id", taskID, "error", err)
	}

	if task.TargetNode != "" {
		if err := tm.transport.CancelTask(ctx, task.TargetNode, taskID); err != nil {
			tm.logger.WarnKV("Failed to send cancel command to worker",
				"task_id", taskID, "node_id", task.TargetNode, "error", err)
		}
	}

	tm.logger.InfoKV("Task cancelled", "task_id", taskID)
	return nil
}

// GetTask 获取任务信息
func (tm *TaskManager) GetTask(taskID string) (*common.TaskInfo, error) {
	return tm.store.GetTask(context.Background(), taskID)
}

// ListTasks 列出所有任务
func (tm *TaskManager) ListTasks() ([]*common.TaskInfo, error) {
	return tm.store.ListTasks(context.Background())
}

// ListTasksByState 按状态过滤任务
func (tm *TaskManager) ListTasksByState(state common.TaskState) ([]*common.TaskInfo, error) {
	return tm.store.ListTasksByState(context.Background(), state)
}

// ListTasksByNode 按节点过滤任务
func (tm *TaskManager) ListTasksByNode(nodeID string) ([]*common.TaskInfo, error) {
	return tm.store.ListTasksByNode(context.Background(), nodeID)
}

// HandleStatusUpdate 处理 Worker 上报的任务状态更新
func (tm *TaskManager) HandleStatusUpdate(update *common.TaskStatusUpdate) error {
	ctx := context.Background()

	task, err := tm.store.GetTask(ctx, update.TaskID)
	if err != nil {
		tm.logger.WarnKV("Status update for unknown task", "task_id", update.TaskID)
		return err
	}

	if err := task.SetState(update.State); err != nil {
		tm.logger.WarnKV("Invalid task state transition",
			"task_id", update.TaskID,
			"from", task.State,
			"to", update.State,
			"error", err)
		return err
	}

	switch update.State {
	case common.TaskStateRunning:
		task.StartedAt = update.Timestamp
		tm.logger.InfoKV(task.Error,
			"task_id", task.ID,
			"state", update.State)
	case common.TaskStateSucceeded:
		task.FinishedAt = update.Timestamp
		task.Result = update.Result
		task.Progress = 1.0
		tm.logger.InfoKV(task.Error,
			"task_id", task.ID,
			"state", update.State)
	case common.TaskStateFailed:
		task.FinishedAt = update.Timestamp
		task.Error = update.ErrorMessage
		tm.logger.ErrorKV(task.Error,
			"task_id", task.ID,
			"state", update.State,
			"error", update.ErrorMessage)
		if task.IsRetryable() {
			tm.handleRetry(ctx, task)
			return nil
		}
	case common.TaskStateTimeout:
		task.FinishedAt = update.Timestamp
		task.Error = "task timed out"
		tm.logger.ErrorKV(task.Error,
			"task_id", task.ID,
			"state", update.State)
		if task.IsRetryable() {
			tm.handleRetry(ctx, task)
			return nil
		}
	}

	if update.Progress > 0 {
		task.Progress = update.Progress
	}

	if err := tm.store.SaveTask(ctx, task); err != nil {
		tm.logger.WarnKV("Failed to persist task status update",
			"task_id", update.TaskID, "error", err)
	}

	tm.logger.DebugKV("Task status updated",
		"task_id", update.TaskID,
		"state", update.State,
		"progress", update.Progress)

	return nil
}

// GetTaskStats 获取任务统计信息
func (tm *TaskManager) GetTaskStats() (*common.TaskStats, error) {
	return tm.store.TaskStats(context.Background())
}

// handleRetry 处理任务重试
func (tm *TaskManager) handleRetry(ctx context.Context, task *common.TaskInfo) {
	task.IncrementRetry()
	task.StartedAt = time.Time{}
	task.FinishedAt = time.Time{}
	task.Error = ""
	task.Progress = 0

	if err := tm.store.SaveTask(ctx, task); err != nil {
		tm.logger.WarnKV("Failed to persist task retry state", "task_id", task.ID, "error", err)
	}

	if err := tm.store.EnqueuePending(ctx, task); err != nil {
		tm.logger.WarnKV("Failed to enqueue task for retry", "task_id", task.ID, "error", err)
	}

	tm.logger.InfoKV("Task queued for retry",
		"task_id", task.ID,
		"retry_count", task.RetryCount,
		"max_retries", task.MaxRetries)
}

// scheduleLoop 调度循环 - 从持久化队列取出任务并分配节点
func (tm *TaskManager) scheduleLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		task, err := tm.store.DequeuePending(ctx)
		if err != nil || task == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		tm.scheduleTask(ctx, task)
	}
}

// scheduleTask 为任务选择目标节点并下发
func (tm *TaskManager) scheduleTask(ctx context.Context, task *common.TaskInfo) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 选择多个候选节点，跳过没有活跃流连接的节点
	allNodes := tm.pool.Select(tm.candidateNodeCount)
	var connectedNodes []common.NodeInfo
	for _, node := range allNodes {
		if tm.transport.IsNodeConnected(node.GetID()) {
			connectedNodes = append(connectedNodes, node)
		}
	}

	if len(connectedNodes) == 0 {
		if err := tm.store.EnqueuePending(ctx, task); err != nil {
			tm.logger.ErrorKV("Failed to re-enqueue task",
				"task_id", task.ID, "error", err)
		}
		tm.logger.WarnKV("No available nodes for task, re-queued",
			"task_id", task.ID)
		// 当没有可用节点时，等待一段时间再重试，避免日志刷屏
		time.Sleep(1 * time.Second)
		return
	}

	targetNode := connectedNodes[0]
	task.TargetNode = targetNode.GetID()

	if err := tm.dispatchToNode(ctx, task, targetNode.GetID()); err != nil {
		tm.logger.ErrorKV("Failed to dispatch task",
			"task_id", task.ID,
			"node_id", targetNode.GetID(),
			"error", err)
		task.TargetNode = ""
		if err := tm.store.EnqueuePending(ctx, task); err != nil {
			tm.logger.ErrorKV("Failed to re-enqueue task after dispatch failure",
				"task_id", task.ID, "error", err)
		}
		// 当任务分发失败时，等待一段时间再重试，避免日志刷屏
		time.Sleep(1 * time.Second)
	}
}

// setTaskState 设置任务状态并保存
func (tm *TaskManager) setTaskState(ctx context.Context, task *common.TaskInfo, state common.TaskState, logMessage string) error {
	if err := task.SetState(state); err != nil {
		return err
	}

	if err := tm.store.SaveTask(ctx, task); err != nil {
		tm.logger.WarnKV("Failed to persist task state",
			"task_id", task.ID, "state", state, "error", err)
	}

	if logMessage != "" {
		tm.logger.InfoKV(logMessage,
			"task_id", task.ID,
			"state", state)
	}

	return nil
}

// dispatchToNode 向指定节点下发任务
func (tm *TaskManager) dispatchToNode(ctx context.Context, task *common.TaskInfo, nodeID string) error {
	if err := tm.setTaskState(ctx, task, common.TaskStateScheduled, "Task scheduled"); err != nil {
		return err
	}

	if err := tm.transport.DispatchTask(ctx, nodeID, task); err != nil {
		// 记录详细的错误信息
		tm.logger.ErrorKV("Failed to dispatch task",
			"task_id", task.ID,
			"node_id", nodeID,
			"error", err)
		// 尝试将任务状态重置为 pending，以便重新调度
		if resetErr := tm.setTaskState(ctx, task, common.TaskStatePending, "Task reset to pending due to dispatch failure"); resetErr != nil {
			tm.logger.WarnKV("Failed to reset task state to pending",
				"task_id", task.ID, "error", resetErr)
		}
		return err
	}

	if err := tm.setTaskState(ctx, task, common.TaskStateDispatched, "Task dispatched"); err != nil {
		return err
	}

	tm.logger.InfoKV("Task dispatched to node",
		"task_id", task.ID,
		"node_id", nodeID)

	return nil
}

// timeoutCheckLoop 超时检查循环 - 定期扫描运行中的任务是否超时
func (tm *TaskManager) timeoutCheckLoop(ctx context.Context) {
	syncx.NewEventLoop(ctx).
		OnTicker(5*time.Second, func() {
			tm.checkTimeouts(ctx)
		}).
		Run()
}

// checkTimeouts 检查所有运行中任务是否超时
func (tm *TaskManager) checkTimeouts(ctx context.Context) {
	running, err := tm.store.ListTasksByState(ctx, common.TaskStateRunning)
	if err != nil {
		tm.logger.WarnKV("Failed to list running tasks for timeout check", "error", err)
		return
	}

	for _, task := range running {
		if task.IsTimedOut() {
			if err := task.SetState(common.TaskStateTimeout); err != nil {
				continue
			}
			task.FinishedAt = time.Now()
			task.Error = "task timed out"

			if task.IsRetryable() {
				tm.handleRetry(ctx, task)
			} else {
				if err := tm.store.SaveTask(ctx, task); err != nil {
					tm.logger.WarnKV("Failed to persist task timeout",
						"task_id", task.ID, "error", err)
				}
				tm.logger.WarnKV("Task timed out",
					"task_id", task.ID,
					"timeout", task.Timeout)
			}
		}
	}
}

// TaskOption 任务配置函数
type TaskOption func(*common.TaskInfo)

// WithPriority 设置任务优先级
func WithPriority(priority int32) TaskOption {
	return func(t *common.TaskInfo) {
		t.Priority = priority
	}
}

// WithTimeout 设置任务超时时间
func WithTimeout(timeout time.Duration) TaskOption {
	return func(t *common.TaskInfo) {
		t.Timeout = timeout
	}
}

// WithMaxRetries 设置最大重试次数
func WithMaxRetries(maxRetries int32) TaskOption {
	return func(t *common.TaskInfo) {
		t.MaxRetries = maxRetries
	}
}

// WithTargetNode 指定目标节点
func WithTargetNode(nodeID string) TaskOption {
	return func(t *common.TaskInfo) {
		t.TargetNode = nodeID
	}
}

// WithMetadata 设置任务元数据
func WithMetadata(metadata map[string]string) TaskOption {
	return func(t *common.TaskInfo) {
		t.Metadata = metadata
	}
}
