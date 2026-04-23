/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 11:59:57
 * @FilePath: \kronos-cluster\master\task_store.go
 * @Description: 任务持久化存储接口与实现 - 支持可插拔存储后端
 *
 * 设计理念:
 *   任务队列必须持久化，避免 Master 宕机导致任务丢失
 *   通过 TaskStore 接口抽象存储层，支持多种后端实现：
 *   - MemoryTaskStore: 内存实现（仅用于测试，不推荐生产使用）
 *   - RedisTaskStore:  Redis 实现（推荐生产使用，天然支持持久化）
 *
 * Redis 存储架构:
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │  distributed:task:{taskID}        → Hash  (任务详情)         │
 *   │  distributed:task:pending         → List  (待调度队列 FIFO)  │
 *   │  distributed:task:state:{state}   → Set   (按状态索引)       │
 *   │  distributed:task:node:{nodeID}   → Set   (按节点索引)       │
 *   │  distributed:task:stats           → Hash  (统计缓存)         │
 *   └──────────────────────────────────────────────────────────────┘
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/redis/go-redis/v9"
)

const (
	redisTaskKeyPrefix   = "distributed:task:"        // 任务 Hash Key 前缀
	redisPendingQueueKey = "distributed:task:pending" // 待调度队列 Key
	redisStateSetPrefix  = "distributed:task:state:"  // 按状态索引 Key 前缀
	redisNodeSetPrefix   = "distributed:task:node:"   // 按节点索引 Key 前缀
)

// TaskStore 任务持久化存储接口
// 所有方法必须保证数据持久化，Master 重启后任务数据不丢失
type TaskStore interface {
	// SaveTask 保存任务（新建或更新）
	SaveTask(ctx context.Context, task *common.TaskInfo) error

	// GetTask 根据 ID 获取任务
	GetTask(ctx context.Context, taskID string) (*common.TaskInfo, error)

	// DeleteTask 删除任务
	DeleteTask(ctx context.Context, taskID string) error

	// UpdateTaskState 更新任务状态
	UpdateTaskState(ctx context.Context, taskID string, state common.TaskState) error

	// EnqueuePending 将任务加入待调度队列（持久化）
	EnqueuePending(ctx context.Context, task *common.TaskInfo) error

	// DequeuePending 从待调度队列取出一个任务
	DequeuePending(ctx context.Context) (*common.TaskInfo, error)

	// PendingSize 待调度队列长度
	PendingSize(ctx context.Context) (int, error)

	// ListTasks 列出所有任务
	ListTasks(ctx context.Context) ([]*common.TaskInfo, error)

	// ListTasksByState 按状态过滤任务
	ListTasksByState(ctx context.Context, state common.TaskState) ([]*common.TaskInfo, error)

	// ListTasksByNode 按节点过滤任务
	ListTasksByNode(ctx context.Context, nodeID string) ([]*common.TaskInfo, error)

	// TaskStats 获取任务统计
	TaskStats(ctx context.Context) (*common.TaskStats, error)

	// Recover 恢复未完成任务（Master 重启后调用）
	// 将 Running/Dispatched/Scheduled 状态的任务重新入队等待调度
	Recover(ctx context.Context) error

	// Close 关闭存储
	Close() error
}

// =====================================================================
// 内存实现（仅用于测试）
// =====================================================================

// MemoryTaskStore 内存任务存储 - 仅用于测试，宕机数据丢失
type MemoryTaskStore struct {
	tasks   *syncx.Map[string, *common.TaskInfo]
	pending []*common.TaskInfo
	mu      sync.Mutex
	logger  logger.ILogger
}

// NewMemoryTaskStore 创建内存任务存储
func NewMemoryTaskStore(log logger.ILogger) *MemoryTaskStore {
	return &MemoryTaskStore{
		tasks:  syncx.NewMap[string, *common.TaskInfo](),
		logger: log,
	}
}

// SaveTask 保存任务
func (s *MemoryTaskStore) SaveTask(ctx context.Context, task *common.TaskInfo) error {
	s.tasks.Store(task.ID, task)
	return nil
}

// GetTask 获取任务
func (s *MemoryTaskStore) GetTask(ctx context.Context, taskID string) (*common.TaskInfo, error) {
	task, ok := s.tasks.Load(taskID)
	if !ok {
		return nil, fmt.Errorf(common.ErrTaskNotFound, taskID)
	}
	return task, nil
}

// DeleteTask 删除任务
func (s *MemoryTaskStore) DeleteTask(ctx context.Context, taskID string) error {
	s.tasks.Delete(taskID)
	return nil
}

// UpdateTaskState 更新任务状态
func (s *MemoryTaskStore) UpdateTaskState(ctx context.Context, taskID string, state common.TaskState) error {
	task, ok := s.tasks.Load(taskID)
	if !ok {
		return fmt.Errorf(common.ErrTaskNotFound, taskID)
	}
	task.State = state
	s.tasks.Store(taskID, task)
	return nil
}

// EnqueuePending 将任务加入待调度队列
func (s *MemoryTaskStore) EnqueuePending(ctx context.Context, task *common.TaskInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, task)
	s.tasks.Store(task.ID, task)
	return nil
}

// DequeuePending 从待调度队列取出一个任务
func (s *MemoryTaskStore) DequeuePending(ctx context.Context) (*common.TaskInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return nil, nil
	}

	task := s.pending[0]
	s.pending = s.pending[1:]
	return task, nil
}

// PendingSize 待调度队列长度
func (s *MemoryTaskStore) PendingSize(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), nil
}

// ListTasks 列出所有任务
func (s *MemoryTaskStore) ListTasks(ctx context.Context) ([]*common.TaskInfo, error) {
	tasks := make([]*common.TaskInfo, 0)
	s.tasks.Range(func(key string, value *common.TaskInfo) bool {
		tasks = append(tasks, value)
		return true
	})
	return tasks, nil
}

// ListTasksByState 按状态过滤任务
func (s *MemoryTaskStore) ListTasksByState(ctx context.Context, state common.TaskState) ([]*common.TaskInfo, error) {
	tasks := make([]*common.TaskInfo, 0)
	s.tasks.Range(func(key string, value *common.TaskInfo) bool {
		if value.State == state {
			tasks = append(tasks, value)
		}
		return true
	})
	return tasks, nil
}

// ListTasksByNode 按节点过滤任务
func (s *MemoryTaskStore) ListTasksByNode(ctx context.Context, nodeID string) ([]*common.TaskInfo, error) {
	tasks := make([]*common.TaskInfo, 0)
	s.tasks.Range(func(key string, value *common.TaskInfo) bool {
		if value.TargetNode == nodeID {
			tasks = append(tasks, value)
		}
		return true
	})
	return tasks, nil
}

// TaskStats 获取任务统计
func (s *MemoryTaskStore) TaskStats(ctx context.Context) (*common.TaskStats, error) {
	stats := &common.TaskStats{}
	s.tasks.Range(func(key string, value *common.TaskInfo) bool {
		stats.Total++
		switch value.State {
		case common.TaskStatePending:
			stats.Pending++
		case common.TaskStateScheduled:
			stats.Scheduled++
		case common.TaskStateDispatched:
			stats.Dispatched++
		case common.TaskStateRunning:
			stats.Running++
		case common.TaskStateSucceeded:
			stats.Succeeded++
		case common.TaskStateFailed:
			stats.Failed++
		case common.TaskStateCancelled:
			stats.Cancelled++
		case common.TaskStateTimeout:
			stats.Timeout++
		case common.TaskStateRetrying:
			stats.Retrying++
		}
		return true
	})
	return stats, nil
}

// Recover 恢复未完成任务
func (s *MemoryTaskStore) Recover(ctx context.Context) error {
	s.tasks.Range(func(key string, task *common.TaskInfo) bool {
		if taskNeedsRecovery(task) {
			originalState := task.State
			resetTaskForRecovery(task)
			s.pending = append(s.pending, task)
			s.tasks.Store(task.ID, task)
			s.logger.InfoKV("Recovered task after restart",
				"task_id", task.ID,
				"original_state", originalState)
		}
		return true
	})
	return nil
}

// Close 关闭存储
func (s *MemoryTaskStore) Close() error {
	return nil
}

// =====================================================================
// Redis 持久化实现（生产推荐）
// =====================================================================

// RedisTaskStore Redis 任务存储 - 生产推荐，天然持久化
// 使用 Redis Hash 存储任务详情，Redis List 作为待调度队列，Redis Set 作为索引
type RedisTaskStore struct {
	client redis.UniversalClient
	logger logger.ILogger
}

// NewRedisTaskStore 创建 Redis 任务存储
// client: 已初始化的 Redis 客户端（由调用方注入，支持单机/哨兵/集群模式）
func NewRedisTaskStore(client redis.UniversalClient, log logger.ILogger) *RedisTaskStore {
	return &RedisTaskStore{
		client: client,
		logger: log,
	}
}

// SaveTask 保存任务到 Redis（Hash + 索引更新）
func (s *RedisTaskStore) SaveTask(ctx context.Context, task *common.TaskInfo) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshalTask, task.ID, err)
	}

	taskKey := redisTaskKeyPrefix + task.ID
	stateKey := redisStateSetPrefix + string(task.State)

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, taskKey, "data", data)
	pipe.HSet(ctx, taskKey, "state", string(task.State))
	pipe.SAdd(ctx, stateKey, task.ID)

	if task.TargetNode != "" {
		nodeKey := redisNodeSetPrefix + task.TargetNode
		pipe.SAdd(ctx, nodeKey, task.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf(common.ErrFailedSaveTask, task.ID, err)
	}

	return nil
}

// GetTask 从 Redis 获取任务
func (s *RedisTaskStore) GetTask(ctx context.Context, taskID string) (*common.TaskInfo, error) {
	taskKey := redisTaskKeyPrefix + taskID
	result, err := s.client.HGet(ctx, taskKey, "data").Result()
	if err == redis.Nil {
		return nil, fmt.Errorf(common.ErrTaskNotFound, taskID)
	}
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedGetTask, taskID, err)
	}

	var task common.TaskInfo
	if err := json.Unmarshal([]byte(result), &task); err != nil {
		return nil, fmt.Errorf(common.ErrFailedUnmarshalTask, taskID, err)
	}

	return &task, nil
}

// DeleteTask 从 Redis 删除任务（Hash + 索引清理）
func (s *RedisTaskStore) DeleteTask(ctx context.Context, taskID string) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return nil
	}

	taskKey := redisTaskKeyPrefix + taskID
	stateKey := redisStateSetPrefix + string(task.State)

	pipe := s.client.Pipeline()
	pipe.Del(ctx, taskKey)
	pipe.SRem(ctx, stateKey, taskID)

	if task.TargetNode != "" {
		nodeKey := redisNodeSetPrefix + task.TargetNode
		pipe.SRem(ctx, nodeKey, taskID)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// UpdateTaskState 更新 Redis 中任务状态（旧索引移除 + 新索引添加）
func (s *RedisTaskStore) UpdateTaskState(ctx context.Context, taskID string, state common.TaskState) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	oldState := task.State
	task.State = state

	taskKey := redisTaskKeyPrefix + taskID
	oldStateKey := redisStateSetPrefix + string(oldState)
	newStateKey := redisStateSetPrefix + string(state)

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf(common.ErrFailedMarshalTask, taskID, err)
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, taskKey, "data", data)
	pipe.HSet(ctx, taskKey, "state", string(state))
	pipe.SRem(ctx, oldStateKey, taskID)
	pipe.SAdd(ctx, newStateKey, taskID)

	_, err = pipe.Exec(ctx)
	return err
}

// EnqueuePending 将任务加入 Redis 待调度队列（LPUSH + 状态更新）
func (s *RedisTaskStore) EnqueuePending(ctx context.Context, task *common.TaskInfo) error {
	task.State = common.TaskStatePending

	if err := s.SaveTask(ctx, task); err != nil {
		return err
	}

	if err := s.client.LPush(ctx, redisPendingQueueKey, task.ID).Err(); err != nil {
		return fmt.Errorf(common.ErrFailedEnqueueTask, task.ID, err)
	}

	return nil
}

// DequeuePending 从 Redis 待调度队列取出任务（RPOP + 加载详情）
func (s *RedisTaskStore) DequeuePending(ctx context.Context) (*common.TaskInfo, error) {
	taskID, err := s.client.RPop(ctx, redisPendingQueueKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedDequeueTask, err)
	}

	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		s.logger.WarnKV("Dequeued task ID not found in store, skipping",
			"task_id", taskID, "error", err)
		return nil, nil
	}

	return task, nil
}

// PendingSize Redis 待调度队列长度
func (s *RedisTaskStore) PendingSize(ctx context.Context) (int, error) {
	size, err := s.client.LLen(ctx, redisPendingQueueKey).Result()
	if err != nil {
		return 0, err
	}
	return int(size), nil
}

// ListTasks 列出 Redis 中所有任务（SCAN 遍历 Hash Key）
func (s *RedisTaskStore) ListTasks(ctx context.Context) ([]*common.TaskInfo, error) {
	var tasks []*common.TaskInfo
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, redisTaskKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf(common.ErrFailedScanTasks, err)
		}

		for _, key := range keys {
			data, err := s.client.HGet(ctx, key, "data").Result()
			if err != nil {
				s.logger.WarnKV("Failed to read task data", "key", key, "error", err)
				continue
			}

			var task common.TaskInfo
			if err := json.Unmarshal([]byte(data), &task); err != nil {
				s.logger.WarnKV("Failed to unmarshal task", "key", key, "error", err)
				continue
			}
			tasks = append(tasks, &task)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return tasks, nil
}

// ListTasksByState 按状态过滤 Redis 任务（SMEMBERS + 批量加载）
func (s *RedisTaskStore) ListTasksByState(ctx context.Context, state common.TaskState) ([]*common.TaskInfo, error) {
	stateKey := redisStateSetPrefix + string(state)
	taskIDs, err := s.client.SMembers(ctx, stateKey).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*common.TaskInfo, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		task, err := s.GetTask(ctx, taskID)
		if err != nil {
			s.logger.WarnKV("Task in state index not found",
				"task_id", taskID, "state", state, "error", err)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// ListTasksByNode 按节点过滤 Redis 任务（SMEMBERS + 批量加载）
func (s *RedisTaskStore) ListTasksByNode(ctx context.Context, nodeID string) ([]*common.TaskInfo, error) {
	nodeKey := redisNodeSetPrefix + nodeID
	taskIDs, err := s.client.SMembers(ctx, nodeKey).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*common.TaskInfo, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		task, err := s.GetTask(ctx, taskID)
		if err != nil {
			s.logger.WarnKV("Task in node index not found",
				"task_id", taskID, "node_id", nodeID, "error", err)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// TaskStats Redis 任务统计（遍历各状态 Set 的 SCARD）
func (s *RedisTaskStore) TaskStats(ctx context.Context) (*common.TaskStats, error) {
	stats := &common.TaskStats{}

	states := []common.TaskState{
		common.TaskStatePending,
		common.TaskStateScheduled,
		common.TaskStateDispatched,
		common.TaskStateRunning,
		common.TaskStateSucceeded,
		common.TaskStateFailed,
		common.TaskStateCancelled,
		common.TaskStateTimeout,
		common.TaskStateRetrying,
	}

	pipe := s.client.Pipeline()
	cmds := make(map[common.TaskState]*redis.IntCmd, len(states))
	for _, state := range states {
		stateKey := redisStateSetPrefix + string(state)
		cmds[state] = pipe.SCard(ctx, stateKey)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	for _, state := range states {
		count := int(cmds[state].Val())
		stats.Total += count
		switch state {
		case common.TaskStatePending:
			stats.Pending = count
		case common.TaskStateScheduled:
			stats.Scheduled = count
		case common.TaskStateDispatched:
			stats.Dispatched = count
		case common.TaskStateRunning:
			stats.Running = count
		case common.TaskStateSucceeded:
			stats.Succeeded = count
		case common.TaskStateFailed:
			stats.Failed = count
		case common.TaskStateCancelled:
			stats.Cancelled = count
		case common.TaskStateTimeout:
			stats.Timeout = count
		case common.TaskStateRetrying:
			stats.Retrying = count
		}
	}

	return stats, nil
}

// Recover 从 Redis 恢复未完成任务（扫描 Running/Dispatched/Scheduled → 重置为 Pending → 入队）
func (s *RedisTaskStore) Recover(ctx context.Context) error {
	recoverStates := []common.TaskState{
		common.TaskStateDispatched,
		common.TaskStateRunning,
		common.TaskStateScheduled,
	}

	for _, state := range recoverStates {
		tasks, err := s.ListTasksByState(ctx, state)
		if err != nil {
			s.logger.WarnKV("Failed to list tasks for recovery",
				"state", state, "error", err)
			continue
		}

		for _, task := range tasks {
			originalState := task.State
			resetTaskForRecovery(task)

			if err := s.SaveTask(ctx, task); err != nil {
				s.logger.WarnKV("Failed to save recovered task",
					"task_id", task.ID, "error", err)
				continue
			}

			if err := s.client.LPush(ctx, redisPendingQueueKey, task.ID).Err(); err != nil {
				s.logger.WarnKV("Failed to enqueue recovered task",
					"task_id", task.ID, "error", err)
				continue
			}

			s.logger.InfoKV("Recovered task after restart",
				"task_id", task.ID,
				"original_state", originalState)
		}
	}

	return nil
}

// Close 关闭 Redis 存储（不关闭客户端，由调用方管理生命周期）
func (s *RedisTaskStore) Close() error {
	return nil
}

// =====================================================================
// 存储工厂
// =====================================================================

// StoreType 存储类型
type StoreType string

const (
	StoreTypeMemory StoreType = "memory" // 内存存储（测试用）
	StoreTypeRedis  StoreType = "redis"  // Redis 存储（生产推荐）
)

// NewTaskStore 根据类型创建任务存储
func NewTaskStore(storeType StoreType, log logger.ILogger) TaskStore {
	switch storeType {
	case StoreTypeRedis:
		log.WarnKV("RedisTaskStore requires explicit client injection, use NewRedisTaskStore instead")
		return NewMemoryTaskStore(log)
	case StoreTypeMemory, "":
		return NewMemoryTaskStore(log)
	default:
		log.WarnKV("Unknown store type, falling back to memory", "type", storeType)
		return NewMemoryTaskStore(log)
	}
}

// =====================================================================
// 辅助函数
// =====================================================================

// taskNeedsRecovery 判断任务是否需要恢复（Master 重启后重新调度）
func taskNeedsRecovery(task *common.TaskInfo) bool {
	return task.State == common.TaskStateDispatched ||
		task.State == common.TaskStateRunning ||
		task.State == common.TaskStateScheduled
}

// resetTaskForRecovery 重置任务状态以便重新调度
func resetTaskForRecovery(task *common.TaskInfo) {
	task.State = common.TaskStatePending
	task.TargetNode = ""
	task.StartedAt = time.Time{}
	task.Progress = 0
}
