/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 15:28:11
 * @FilePath: \kronos-cluster\master\task_store_test.go
 * @Description: Master TaskStore测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newTestMemoryStore() *MemoryTaskStore {
	return NewMemoryTaskStore(logger.NewEmptyLogger())
}

func newTestTask() *common.TaskInfo {
	return common.NewTaskInfo(random.UUID(), common.TaskTypeCommand, []byte("test"))
}

func TestMemoryTaskStoreSaveAndGet(t *testing.T) {
	store := newTestMemoryStore()
	task := newTestTask()

	err := store.SaveTask(context.Background(), task)
	assert.NoError(t, err)

	found, err := store.GetTask(context.Background(), task.ID)
	assert.NoError(t, err)
	assert.Equal(t, task.ID, found.ID)
	assert.Equal(t, common.TaskTypeCommand, found.Type)
}

func TestMemoryTaskStoreGetNotFound(t *testing.T) {
	store := newTestMemoryStore()

	_, err := store.GetTask(context.Background(), "nonexistent")
	assert.Error(t, err)
}

func TestMemoryTaskStoreDelete(t *testing.T) {
	store := newTestMemoryStore()
	task := newTestTask()
	store.SaveTask(context.Background(), task)

	err := store.DeleteTask(context.Background(), task.ID)
	assert.NoError(t, err)

	_, err = store.GetTask(context.Background(), task.ID)
	assert.Error(t, err)
}

func TestMemoryTaskStoreUpdateTaskState(t *testing.T) {
	store := newTestMemoryStore()
	task := newTestTask()
	store.SaveTask(context.Background(), task)

	err := store.UpdateTaskState(context.Background(), task.ID, common.TaskStateScheduled)
	assert.NoError(t, err)

	found, _ := store.GetTask(context.Background(), task.ID)
	assert.Equal(t, common.TaskStateScheduled, found.State)
}

func TestMemoryTaskStoreUpdateTaskStateNotFound(t *testing.T) {
	store := newTestMemoryStore()

	err := store.UpdateTaskState(context.Background(), random.UUID(), common.TaskStateRunning)
	assert.Error(t, err)
}

func TestMemoryTaskStoreEnqueueDequeue(t *testing.T) {
	store := newTestMemoryStore()
	task1 := newTestTask()
	task2 := newTestTask()

	store.EnqueuePending(context.Background(), task1)
	store.EnqueuePending(context.Background(), task2)

	size, err := store.PendingSize(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	dequeued, err := store.DequeuePending(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task1.ID, dequeued.ID)

	size, _ = store.PendingSize(context.Background())
	assert.Equal(t, 1, size)
}

func TestMemoryTaskStoreDequeueEmpty(t *testing.T) {
	store := newTestMemoryStore()

	task, err := store.DequeuePending(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, task)
}

func TestMemoryTaskStoreListTasks(t *testing.T) {
	store := newTestMemoryStore()
	store.SaveTask(context.Background(), newTestTask())
	store.SaveTask(context.Background(), newTestTask())
	store.SaveTask(context.Background(), newTestTask())

	tasks, err := store.ListTasks(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tasks, 3)
}

func TestMemoryTaskStoreListTasksEmpty(t *testing.T) {
	store := newTestMemoryStore()

	tasks, err := store.ListTasks(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, tasks)
}

func TestMemoryTaskStoreListTasksByState(t *testing.T) {
	store := newTestMemoryStore()

	task1 := newTestTask()
	store.SaveTask(context.Background(), task1)

	task2 := &common.TaskInfo{ID: newTestNodeID(), Type: common.TaskTypeCommand, State: common.TaskStateRunning, Payload: []byte("test")}
	store.SaveTask(context.Background(), task2)

	tasks, err := store.ListTasksByState(context.Background(), common.TaskStatePending)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.ID, tasks[0].ID)
}

func TestMemoryTaskStoreListTasksByNode(t *testing.T) {
	store := newTestMemoryStore()

	task1 := newTestTask()
	task1.TargetNode = newTestNodeID()
	store.SaveTask(context.Background(), task1)

	task2 := newTestTask()
	task2.TargetNode = newTestNodeID()
	store.SaveTask(context.Background(), task2)

	tasks, err := store.ListTasksByNode(context.Background(), task1.TargetNode)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, task1.ID, tasks[0].ID)
}

func TestMemoryTaskStoreTaskStats(t *testing.T) {
	store := newTestMemoryStore()

	task1 := newTestTask()
	store.SaveTask(context.Background(), task1)

	task2 := &common.TaskInfo{ID: newTestNodeID(), Type: common.TaskTypeCommand, State: common.TaskStateRunning, Payload: []byte("test")}
	store.SaveTask(context.Background(), task2)

	task3 := &common.TaskInfo{ID: newTestNodeID(), Type: common.TaskTypeCommand, State: common.TaskStateSucceeded, Payload: []byte("test")}
	store.SaveTask(context.Background(), task3)

	stats, err := store.TaskStats(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, 1, stats.Pending)
	assert.Equal(t, 1, stats.Running)
	assert.Equal(t, 1, stats.Succeeded)
}

func TestMemoryTaskStoreRecover(t *testing.T) {
	store := newTestMemoryStore()

	task1 := &common.TaskInfo{ID: newTestNodeID(), Type: common.TaskTypeCommand, State: common.TaskStateRunning, TargetNode: newTestNodeID(), Payload: []byte("test")}
	store.SaveTask(context.Background(), task1)

	task2 := &common.TaskInfo{ID: newTestNodeID(), Type: common.TaskTypeCommand, State: common.TaskStateSucceeded, Payload: []byte("test")}
	store.SaveTask(context.Background(), task2)

	err := store.Recover(context.Background())
	assert.NoError(t, err)

	recovered, _ := store.GetTask(context.Background(), task1.ID)
	assert.Equal(t, common.TaskStatePending, recovered.State)
	assert.Equal(t, "", recovered.TargetNode)

	stillSucceeded, _ := store.GetTask(context.Background(), task2.ID)
	assert.Equal(t, common.TaskStateSucceeded, stillSucceeded.State)
}

func TestMemoryTaskStoreClose(t *testing.T) {
	store := newTestMemoryStore()

	err := store.Close()
	assert.NoError(t, err)
}

func TestNewTaskStoreMemory(t *testing.T) {
	log := logger.NewEmptyLogger()
	store := NewTaskStore(StoreTypeMemory, log)
	assert.NotNil(t, store)

	_, ok := store.(*MemoryTaskStore)
	assert.True(t, ok)
}

func TestNewTaskStoreDefault(t *testing.T) {
	log := logger.NewEmptyLogger()
	store := NewTaskStore("", log)
	assert.NotNil(t, store)
}

func TestNewTaskStoreUnknown(t *testing.T) {
	log := logger.NewEmptyLogger()
	store := NewTaskStore("unknown", log)
	assert.NotNil(t, store)
}

func TestTaskNeedsRecovery(t *testing.T) {
	assert.True(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStateRunning}))
	assert.True(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStateDispatched}))
	assert.True(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStateScheduled}))
	assert.False(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStatePending}))
	assert.False(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStateSucceeded}))
	assert.False(t, taskNeedsRecovery(&common.TaskInfo{State: common.TaskStateFailed}))
}

func TestResetTaskForRecovery(t *testing.T) {
	task := &common.TaskInfo{
		State:      common.TaskStateRunning,
		TargetNode: newTestNodeID(),
		Progress:   0.5,
	}
	resetTaskForRecovery(task)

	assert.Equal(t, common.TaskStatePending, task.State)
	assert.Equal(t, "", task.TargetNode)
	assert.Equal(t, float64(0), task.Progress)
	assert.True(t, task.StartedAt.IsZero())
}
