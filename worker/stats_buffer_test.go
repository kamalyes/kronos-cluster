/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 09:56:58
 * @FilePath: \go-distributed\worker\stats_buffer_test.go
 * @Description: 统计缓冲区单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

type testReportable struct {
	NodeID    string
	TaskID    string
	Timestamp time.Time
}

func (r testReportable) GetNodeID() string       { return r.NodeID }
func (r testReportable) GetTaskID() string       { return r.TaskID }
func (r testReportable) GetTimestamp() time.Time { return r.Timestamp }

func newTestReportable(nodeID string) testReportable {
	return testReportable{NodeID: nodeID, TaskID: random.UUID(), Timestamp: time.Now()}
}

func TestNewStatsBuffer(t *testing.T) {
	log := logger.NewEmptyLogger()
	var flushed int32
	nodeID := random.UUID()

	sb := NewStatsBuffer(nodeID, 10, func(data []testReportable) error {
		atomic.AddInt32(&flushed, int32(len(data)))
		return nil
	}, log)

	assert.NotNil(t, sb)
	assert.Equal(t, nodeID, sb.nodeID)
}

func TestStatsBufferAdd(t *testing.T) {
	log := logger.NewEmptyLogger()
	var flushedCount int32
	nodeID := random.UUID()

	sb := NewStatsBuffer(nodeID, 3, func(data []testReportable) error {
		atomic.AddInt32(&flushedCount, int32(len(data)))
		return nil
	}, log)

	sb.Add(newTestReportable(nodeID))
	sb.Add(newTestReportable(nodeID))

	assert.Equal(t, int32(0), atomic.LoadInt32(&flushedCount))
}

func TestStatsBufferAutoFlush(t *testing.T) {
	log := logger.NewEmptyLogger()
	var flushedCount int32
	nodeID := random.UUID()

	sb := NewStatsBuffer(nodeID, 3, func(data []testReportable) error {
		atomic.AddInt32(&flushedCount, int32(len(data)))
		return nil
	}, log)

	sb.Add(newTestReportable(nodeID))
	sb.Add(newTestReportable(nodeID))
	sb.Add(newTestReportable(nodeID))

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(3), atomic.LoadInt32(&flushedCount))
}

func TestStatsBufferFlush(t *testing.T) {
	log := logger.NewEmptyLogger()
	var flushedData []testReportable
	nodeID := random.UUID()

	sb := NewStatsBuffer(nodeID, 100, func(data []testReportable) error {
		flushedData = append(flushedData, data...)
		return nil
	}, log)

	sb.Add(newTestReportable(nodeID))
	sb.Add(newTestReportable(nodeID))

	err := sb.Flush()
	assert.NoError(t, err)
	assert.Len(t, flushedData, 2)
}

func TestStatsBufferFlushEmpty(t *testing.T) {
	log := logger.NewEmptyLogger()
	called := false
	nodeID := random.UUID()

	sb := NewStatsBuffer(nodeID, 100, func(data []testReportable) error {
		called = true
		return nil
	}, log)

	err := sb.Flush()
	assert.NoError(t, err)
	assert.False(t, called)
}

func TestStatsBufferSetTaskID(t *testing.T) {
	log := logger.NewEmptyLogger()
	nodeID := random.UUID()
	sb := NewStatsBuffer[testReportable](nodeID, 100, nil, log)

	taskID := random.UUID()
	sb.SetTaskID(taskID)
	assert.Equal(t, taskID, sb.GetTaskID())
}

func TestStatsBufferGetTaskID(t *testing.T) {
	log := logger.NewEmptyLogger()
	nodeID := random.UUID()
	sb := NewStatsBuffer[testReportable](nodeID, 100, nil, log)

	assert.Equal(t, "", sb.GetTaskID())
}

func TestAggregateLatenciesEmpty(t *testing.T) {
	stats := AggregateLatencies([]float64{})
	assert.NotNil(t, stats)
	assert.Equal(t, 0.0, stats.Min)
	assert.Equal(t, 0.0, stats.Max)
	assert.Equal(t, 0.0, stats.Avg)
	assert.Equal(t, 0, stats.Count)
}

func TestAggregateLatenciesSingleValue(t *testing.T) {
	stats := AggregateLatencies([]float64{100})
	assert.Equal(t, 100.0, stats.Min)
	assert.Equal(t, 100.0, stats.Max)
	assert.Equal(t, 100.0, stats.Avg)
	assert.Equal(t, 1, stats.Count)
}

func TestAggregateLatenciesMultipleValues(t *testing.T) {
	latencies := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	stats := AggregateLatencies(latencies)

	assert.Equal(t, 10.0, stats.Min)
	assert.Equal(t, 100.0, stats.Max)
	assert.Equal(t, 10, stats.Count)
	assert.Greater(t, stats.Avg, 0.0)
	assert.Greater(t, stats.P50, 0.0)
	assert.Greater(t, stats.P90, 0.0)
	assert.Greater(t, stats.P95, 0.0)
	assert.Greater(t, stats.P99, 0.0)
}

func TestAggregateLatenciesPercentileOrder(t *testing.T) {
	latencies := make([]float64, 100)
	for i := 0; i < 100; i++ {
		latencies[i] = float64(i + 1)
	}
	stats := AggregateLatencies(latencies)

	assert.LessOrEqual(t, stats.P50, stats.P90)
	assert.LessOrEqual(t, stats.P90, stats.P95)
	assert.LessOrEqual(t, stats.P95, stats.P99)
}

func TestFormatStatusCode(t *testing.T) {
	assert.Equal(t, "200", FormatStatusCode(200))
	assert.Equal(t, "404", FormatStatusCode(404))
	assert.Equal(t, "500", FormatStatusCode(500))
	assert.Equal(t, "0", FormatStatusCode(0))
}
