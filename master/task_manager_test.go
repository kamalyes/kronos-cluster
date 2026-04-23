/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 15:33:52
 * @FilePath: \kronos-cluster\master\task_manager_test.go
 * @Description: Master TaskManager测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"testing"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/transport"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
)

type mockMasterTransport struct {
	dispatchedTasks []*common.TaskInfo
	cancelledTasks  []string
}

func newMockMasterTransport() *mockMasterTransport {
	return &mockMasterTransport{}
}

func (m *mockMasterTransport) Start(ctx context.Context) error { return nil }
func (m *mockMasterTransport) Stop() error                     { return nil }
func (m *mockMasterTransport) OnRegister(handler func(common.NodeInfo, []byte) (*transport.RegistrationResult, error)) {
}
func (m *mockMasterTransport) OnRegisterWithSecret(handler func(common.NodeInfo, string, []byte) (*transport.RegistrationResult, error)) {
}
func (m *mockMasterTransport) OnHeartbeat(handler func(string, common.NodeState, []byte) (*transport.HeartbeatResult, error)) {
}
func (m *mockMasterTransport) OnUnregister(handler func(string, string) error) {}
func (m *mockMasterTransport) OnTaskStatusUpdate(handler func(*common.TaskStatusUpdate) error) {
}
func (m *mockMasterTransport) DispatchTask(ctx context.Context, nodeID string, task *common.TaskInfo) error {
	m.dispatchedTasks = append(m.dispatchedTasks, task)
	return nil
}
func (m *mockMasterTransport) CancelTask(ctx context.Context, nodeID string, taskID string) error {
	m.cancelledTasks = append(m.cancelledTasks, taskID)
	return nil
}
func (m *mockMasterTransport) IsNodeConnected(nodeID string) bool { return true }

func newTestTaskManager() (*TaskManager, *NodePool[*common.BaseNodeInfo], *mockMasterTransport, TaskStore) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)
	store := NewMemoryTaskStore(log)
	tp := newMockMasterTransport()
	adapter := &nodePoolAdapter[*common.BaseNodeInfo]{pool: pool}
	tm := NewTaskManager(adapter, tp, store, log, config.CandidateNodeCount)
	return tm, pool, tp, store
}

func TestTaskManagerSubmitTask(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, err := tm.SubmitTask(common.TaskTypeCommand, []byte("echo hello"))
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, common.TaskTypeCommand, task.Type)
	assert.Equal(t, common.TaskStatePending, task.State)
}

func TestTaskManagerSubmitTaskInvalidType(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, err := tm.SubmitTask(common.TaskType("invalid"), []byte("test"))
	assert.Error(t, err)
	assert.Nil(t, task)
}

func TestTaskManagerSubmitTaskWithTargetNode(t *testing.T) {
	tm, pool, tp, _ := newTestTaskManager()

	node := newTestBaseNode()
	node.SetState(common.NodeStateIdle)
	pool.Register(node)

	task, err := tm.SubmitTask(common.TaskTypeCommand, []byte("test"), WithTargetNode(node.ID))
	assert.NoError(t, err)
	assert.Equal(t, node.ID, task.TargetNode)
	assert.Len(t, tp.dispatchedTasks, 1)
}

func TestTaskManagerSubmitTaskWithOptions(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, err := tm.SubmitTask(common.TaskTypeCommand, []byte("test"),
		WithPriority(10),
		WithTimeout(30*time.Second),
		WithMaxRetries(3),
		WithMetadata(map[string]string{"key": "value"}),
	)
	assert.NoError(t, err)
	assert.Equal(t, int32(10), task.Priority)
	assert.Equal(t, 30*time.Second, task.Timeout)
	assert.Equal(t, int32(3), task.MaxRetries)
	assert.Equal(t, "value", task.Metadata["key"])
}

func TestTaskManagerCancelTask(t *testing.T) {
	tm, pool, tp, _ := newTestTaskManager()

	node := newTestBaseNode()
	pool.Register(node)

	task, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"), WithTargetNode(node.ID))

	err := tm.CancelTask(task.ID)
	assert.NoError(t, err)
	assert.Contains(t, tp.cancelledTasks, task.ID)
}

func TestTaskManagerCancelTaskTerminalState(t *testing.T) {
	tm, _, _, store := newTestTaskManager()

	task := newTestTask()
	task.State = common.TaskStateSucceeded
	store.SaveTask(context.Background(), task)

	err := tm.CancelTask(task.ID)
	assert.Error(t, err)
}

func TestTaskManagerCancelTaskNotFound(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	err := tm.CancelTask("nonexistent")
	assert.Error(t, err)
}

func TestTaskManagerGetTask(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	submitted, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"))

	task, err := tm.GetTask(submitted.ID)
	assert.NoError(t, err)
	assert.Equal(t, submitted.ID, task.ID)
}

func TestTaskManagerGetTaskNotFound(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	_, err := tm.GetTask("nonexistent")
	assert.Error(t, err)
}

func TestTaskManagerListTasks(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	tm.SubmitTask(common.TaskTypeCommand, []byte("test1"))
	tm.SubmitTask(common.TaskTypeScript, []byte("test2"))

	tasks, err := tm.ListTasks()
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)
}

func TestTaskManagerHandleStatusUpdateRunning(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"))
	task.State = common.TaskStateDispatched

	err := tm.HandleStatusUpdate(&common.TaskStatusUpdate{
		TaskID:    task.ID,
		State:     common.TaskStateRunning,
		Timestamp: time.Now(),
	})
	assert.NoError(t, err)

	updated, _ := tm.GetTask(task.ID)
	assert.Equal(t, common.TaskStateRunning, updated.State)
	assert.False(t, updated.StartedAt.IsZero())
}

func TestTaskManagerHandleStatusUpdateSucceeded(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"))
	task.State = common.TaskStateRunning

	err := tm.HandleStatusUpdate(&common.TaskStatusUpdate{
		TaskID:    task.ID,
		State:     common.TaskStateSucceeded,
		Result:    []byte("ok"),
		Timestamp: time.Now(),
	})
	assert.NoError(t, err)

	updated, _ := tm.GetTask(task.ID)
	assert.Equal(t, common.TaskStateSucceeded, updated.State)
	assert.Equal(t, 1.0, updated.Progress)
}

func TestTaskManagerHandleStatusUpdateFailedWithRetry(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"), WithMaxRetries(3))
	task.State = common.TaskStateRunning

	err := tm.HandleStatusUpdate(&common.TaskStatusUpdate{
		TaskID:       task.ID,
		State:        common.TaskStateFailed,
		ErrorMessage: "something went wrong",
		Timestamp:    time.Now(),
	})
	assert.NoError(t, err)

	updated, _ := tm.GetTask(task.ID)
	assert.Equal(t, int32(1), updated.RetryCount)
	assert.Equal(t, common.TaskStatePending, updated.State)
}

func TestTaskManagerHandleStatusUpdateInvalidTransition(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	task, _ := tm.SubmitTask(common.TaskTypeCommand, []byte("test"))

	err := tm.HandleStatusUpdate(&common.TaskStatusUpdate{
		TaskID: task.ID,
		State:  common.TaskStateSucceeded,
	})
	assert.Error(t, err)
}

func TestTaskManagerGetTaskStats(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	tm.SubmitTask(common.TaskTypeCommand, []byte("test1"))
	tm.SubmitTask(common.TaskTypeScript, []byte("test2"))

	stats, err := tm.GetTaskStats()
	assert.NoError(t, err)
	assert.Equal(t, 2, stats.Total)
	assert.Equal(t, 2, stats.Pending)
}

func TestTaskManagerStartStop(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	err := tm.Start(context.Background())
	assert.NoError(t, err)
	assert.True(t, tm.running.Load())

	err = tm.Stop()
	assert.NoError(t, err)
}

func TestTaskManagerStartAlreadyRunning(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	tm.Start(context.Background())
	defer tm.Stop()

	err := tm.Start(context.Background())
	assert.Error(t, err)
}

func TestTaskManagerStopNotRunning(t *testing.T) {
	tm, _, _, _ := newTestTaskManager()

	err := tm.Stop()
	assert.Error(t, err)
}
