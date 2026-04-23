/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 18:22:02
 * @FilePath: \kronos-cluster\worker\monitor.go
 * @Description: 资源监控器 - 采集 CPU、内存、网络、负载等系统指标
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"runtime"
	"time"
)

// ResourceMonitor 资源监控器 - 定期采集系统资源使用情况
type ResourceMonitor struct {
	mu             *syncx.RWLock       // 读写锁
	logger         logger.ILogger      // 日志
	updateInterval time.Duration       // 更新间隔
	activeTasks    int                 // 活跃任务数
	queuedTasks    int                 // 队列中任务数
	lastNetIO      *net.IOCountersStat // 上次网络 IO 数据（用于计算增量）
	lastNetIOTime  time.Time           // 上次网络 IO 采集时间
}

// NewResourceMonitor 创建资源监控器
func NewResourceMonitor(log logger.ILogger, interval time.Duration) *ResourceMonitor {
	return &ResourceMonitor{
		mu:             syncx.NewRWLock(),
		logger:         log,
		updateInterval: interval,
		lastNetIOTime:  time.Now(),
	}
}

// Start 启动资源监控定时采集
func (rm *ResourceMonitor) Start(ctx context.Context) {
	syncx.NewEventLoop(ctx).
		OnTicker(rm.updateInterval, func() {
			usage, err := rm.GetResourceUsage()
			if err != nil {
				rm.logger.ErrorContextKV(ctx, "Failed to get resource usage", map[string]interface{}{
					"error": err.Error(),
				})
			} else {
				rm.logger.DebugContextKV(ctx, "Resource usage updated", map[string]interface{}{
					"cpu_percent":    usage.CPUPercent,
					"memory_percent": usage.MemoryPercent,
					"active_tasks":   usage.ActiveTasks,
					"load_avg_1m":    usage.LoadAvg1m,
				})
			}
		}).
		Run()
}

// GetResourceUsage 采集当前系统资源使用情况
func (rm *ResourceMonitor) GetResourceUsage() (*common.ResourceUsage, error) {
	usage := &common.ResourceUsage{
		Timestamp: time.Now(),
	}

	// 采集 CPU 使用率
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err == nil && len(cpuPercent) > 0 {
		usage.CPUPercent = cpuPercent[0]
	}

	// 采集内存使用情况
	vmStat, err := mem.VirtualMemory()
	if err == nil {
		usage.MemoryPercent = vmStat.UsedPercent
		usage.MemoryUsed = int64(vmStat.Used)
		usage.MemoryTotal = int64(vmStat.Total)
	}

	// 采集系统负载
	loadAvg, err := load.Avg()
	if err == nil {
		usage.LoadAvg1m = loadAvg.Load1
		usage.LoadAvg5m = loadAvg.Load5
		usage.LoadAvg15m = loadAvg.Load15
	}

	// 读取任务计数
	syncx.WithRLock(rm.mu, func() {
		usage.ActiveTasks = rm.activeTasks
		usage.QueuedTasks = rm.queuedTasks
	})

	// 采集网络 IO（计算增量带宽）
	netIO, err := net.IOCounters(false)
	if err == nil && len(netIO) > 0 {
		currentIO := &netIO[0]
		currentTime := time.Now()

		if rm.lastNetIO != nil {
			duration := currentTime.Sub(rm.lastNetIOTime).Seconds()
			if duration > 0 {
				bytesInDiff := float64(currentIO.BytesRecv - rm.lastNetIO.BytesRecv)
				bytesOutDiff := float64(currentIO.BytesSent - rm.lastNetIO.BytesSent)
				usage.NetworkInMbps = (bytesInDiff * 8) / (1024 * 1024 * duration)
				usage.NetworkOutMbps = (bytesOutDiff * 8) / (1024 * 1024 * duration)
			}
		}

		rm.lastNetIO = currentIO
		rm.lastNetIOTime = currentTime
	}

	usage.DiskIOUtil = 0

	return usage, nil
}

// SetActiveTasks 设置活跃任务数
func (rm *ResourceMonitor) SetActiveTasks(count int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.activeTasks = count
}

// IncrementActiveTasks 活跃任务数加一
func (rm *ResourceMonitor) IncrementActiveTasks() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.activeTasks++
}

// DecrementActiveTasks 活跃任务数减一
func (rm *ResourceMonitor) DecrementActiveTasks() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.activeTasks > 0 {
		rm.activeTasks--
	}
}

// SetQueuedTasks 设置队列中任务数
func (rm *ResourceMonitor) SetQueuedTasks(count int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.queuedTasks = count
}

// IsHealthy 判断系统资源是否健康（CPU < 95%、内存 < 95%、负载 < 2倍 CPU 核数）
func (rm *ResourceMonitor) IsHealthy() (bool, string) {
	usage, err := rm.GetResourceUsage()
	if err != nil {
		return false, "Failed to get resource usage: " + err.Error()
	}

	if usage.CPUPercent > 95 {
		return false, "CPU usage too high"
	}

	if usage.MemoryPercent > 95 {
		return false, "Memory usage too high"
	}

	numCPU := runtime.NumCPU()
	if usage.LoadAvg1m > float64(numCPU*2) {
		return false, "System load too high"
	}

	return true, ""
}

// GetGoRuntimeStats 获取 Go 运行时统计信息
func (rm *ResourceMonitor) GetGoRuntimeStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"goroutines":    runtime.NumGoroutine(),
		"heap_alloc_mb": float64(m.Alloc) / 1024 / 1024,
		"heap_sys_mb":   float64(m.HeapSys) / 1024 / 1024,
		"heap_inuse_mb": float64(m.HeapInuse) / 1024 / 1024,
		"heap_idle_mb":  float64(m.HeapIdle) / 1024 / 1024,
		"gc_num":        m.NumGC,
		"last_gc_time":  time.Unix(0, int64(m.LastGC)).Format(time.RFC3339),
		"next_gc_mb":    float64(m.NextGC) / 1024 / 1024,
	}
}
