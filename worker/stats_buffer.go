/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \kronos-cluster\worker\stats_buffer.go
 * @Description: 统计缓冲区 - 批量收集和刷写统计数据
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"sort"
	"time"
)

// Reportable 可上报数据接口 - 定义统计数据的基本字段
type Reportable interface {
	GetNodeID() string       // 获取节点 ID
	GetTaskID() string       // 获取任务 ID
	GetTimestamp() time.Time // 获取时间戳
}

// StatsBuffer 泛型统计缓冲区 - 批量收集数据，达到阈值或定时刷写
type StatsBuffer[T Reportable] struct {
	nodeID      string                     // 节点 ID
	taskID      string                     // 当前任务 ID
	buffer      []T                        // 数据缓冲区
	bufferMu    syncx.Locker               // 缓冲区锁
	maxSize     int                        // 缓冲区最大容量
	flushFunc   func([]T) error            // 刷写回调函数
	logger      logger.ILogger             // 日志
	taskManager *syncx.PeriodicTaskManager // 定时任务管理器
}

// NewStatsBuffer 创建统计缓冲区
func NewStatsBuffer[T Reportable](nodeID string, maxSize int, flushFunc func([]T) error, log logger.ILogger) *StatsBuffer[T] {
	return &StatsBuffer[T]{
		nodeID:      nodeID,
		buffer:      make([]T, 0, maxSize),
		bufferMu:    syncx.NewLock(),
		maxSize:     maxSize,
		flushFunc:   flushFunc,
		logger:      log,
		taskManager: syncx.NewPeriodicTaskManager(),
	}
}

// Add 添加统计数据到缓冲区，达到最大容量时自动刷写
func (sb *StatsBuffer[T]) Add(result T) {
	sb.bufferMu.Lock()
	defer sb.bufferMu.Unlock()

	sb.buffer = append(sb.buffer, result)

	if len(sb.buffer) >= sb.maxSize {
		syncx.Go().Exec(func() {
			sb.Flush()
		})
	}
}

// Start 启动定时刷写任务
func (sb *StatsBuffer[T]) Start(ctx context.Context) {
	task := syncx.NewPeriodicTask("stats-flush", 1*time.Second, func(taskCtx context.Context) error {
		return sb.Flush()
	}).
		SetOnError(func(name string, err error) {
			sb.logger.WarnContextKV(ctx, "Stats flush error", "error", err)
		}).
		SetOnStart(func(name string) {
			sb.logger.InfoContextKV(ctx, "Stats buffer started", "node_id", sb.nodeID)
		}).
		SetOnStop(func(name string) {
			sb.Flush()
			sb.logger.InfoContextKV(ctx, "Stats buffer stopped", "node_id", sb.nodeID)
		})

	sb.taskManager.AddTask(task)
	sb.taskManager.Start()
}

// Stop 停止统计缓冲区（会先刷写剩余数据）
func (sb *StatsBuffer[T]) Stop() {
	sb.taskManager.Stop()
}

// Flush 刷写缓冲区数据到回调函数
func (sb *StatsBuffer[T]) Flush() error {
	sb.bufferMu.Lock()
	if len(sb.buffer) == 0 {
		sb.bufferMu.Unlock()
		return nil
	}

	toSend := make([]T, len(sb.buffer))
	copy(toSend, sb.buffer)
	sb.buffer = sb.buffer[:0]
	sb.bufferMu.Unlock()

	if sb.flushFunc != nil {
		if err := sb.flushFunc(toSend); err != nil {
			sb.logger.WarnKV("Failed to flush stats", "error", err)
			return err
		}
	}

	sb.logger.DebugKV("Stats flushed",
		"node_id", sb.nodeID,
		"records", len(toSend))

	return nil
}

// SetTaskID 设置当前任务 ID
func (sb *StatsBuffer[T]) SetTaskID(taskID string) {
	sb.bufferMu.Lock()
	defer sb.bufferMu.Unlock()
	sb.taskID = taskID
}

// GetTaskID 获取当前任务 ID
func (sb *StatsBuffer[T]) GetTaskID() string {
	sb.bufferMu.Lock()
	defer sb.bufferMu.Unlock()
	return sb.taskID
}

// AggregateLatencies 聚合延迟统计数据（Min/Max/Avg/P50/P90/P95/P99）
func AggregateLatencies(latencies []float64) *LatencyStats {
	if len(latencies) == 0 {
		return &LatencyStats{}
	}

	sort.Float64s(latencies)

	return &LatencyStats{
		Min:   latencies[0],
		Max:   latencies[len(latencies)-1],
		Avg:   mathx.Mean(latencies),
		P50:   mathx.Percentile(latencies, 50),
		P90:   mathx.Percentile(latencies, 90),
		P95:   mathx.Percentile(latencies, 95),
		P99:   mathx.Percentile(latencies, 99),
		Count: len(latencies),
	}
}

// LatencyStats 延迟统计结果
type LatencyStats struct {
	Min   float64 // 最小延迟
	Max   float64 // 最大延迟
	Avg   float64 // 平均延迟
	P50   float64 // P50 延迟
	P90   float64 // P90 延迟
	P95   float64 // P95 延迟
	P99   float64 // P99 延迟
	Count int     // 样本数量
}

// FormatStatusCode 格式化状态码为字符串
func FormatStatusCode(code int) string {
	return fmt.Sprintf("%d", code)
}
