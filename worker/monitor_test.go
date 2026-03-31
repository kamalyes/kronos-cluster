/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 09:13:07
 * @FilePath: \go-distributed\worker\monitor_test.go
 * @Description: 资源监控器单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"testing"
	"time"

	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
)

func TestNewResourceMonitor(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)
	assert.NotNil(t, rm)
}

func TestResourceMonitorGetResourceUsage(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.NotNil(t, usage)
	assert.False(t, usage.Timestamp.IsZero())
}

func TestResourceMonitorSetActiveTasks(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	rm.SetActiveTasks(5)
	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.Equal(t, 5, usage.ActiveTasks)
}

func TestResourceMonitorIncrementActiveTasks(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	rm.SetActiveTasks(3)
	rm.IncrementActiveTasks()

	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.Equal(t, 4, usage.ActiveTasks)
}

func TestResourceMonitorDecrementActiveTasks(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	rm.SetActiveTasks(3)
	rm.DecrementActiveTasks()

	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.Equal(t, 2, usage.ActiveTasks)
}

func TestResourceMonitorDecrementActiveTasksNotBelowZero(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	rm.SetActiveTasks(0)
	rm.DecrementActiveTasks()

	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.Equal(t, 0, usage.ActiveTasks)
}

func TestResourceMonitorSetQueuedTasks(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	rm.SetQueuedTasks(10)
	usage, err := rm.GetResourceUsage()
	assert.NoError(t, err)
	assert.Equal(t, 10, usage.QueuedTasks)
}

func TestResourceMonitorIsHealthy(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	healthy, reason := rm.IsHealthy()
	assert.True(t, healthy || !healthy)
	if !healthy {
		assert.NotEmpty(t, reason)
	}
}

func TestResourceMonitorGetGoRuntimeStats(t *testing.T) {
	log := logger.NewEmptyLogger()
	rm := NewResourceMonitor(log, 5*time.Second)

	stats := rm.GetGoRuntimeStats()
	assert.NotNil(t, stats)
	assert.Contains(t, stats, "goroutines")
	assert.Contains(t, stats, "heap_alloc_mb")
	assert.Contains(t, stats, "heap_sys_mb")
	assert.Contains(t, stats, "gc_num")
}
