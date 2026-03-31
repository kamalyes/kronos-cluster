/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 11:30:08
 * @FilePath: \go-distributed\common\resource.go
 * @Description: 资源使用率模型定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import "time"

// ResourceUsage 资源使用情况
type ResourceUsage struct {
	CPUPercent     float64   `json:"cpu_percent"`      // CPU 使用率（0-100）
	MemoryPercent  float64   `json:"memory_percent"`   // 内存使用率（0-100）
	MemoryUsed     int64     `json:"memory_used"`      // 已用内存（字节）
	MemoryTotal    int64     `json:"memory_total"`     // 总内存（字节）
	ActiveTasks    int       `json:"active_tasks"`     // 活跃任务数
	QueuedTasks    int       `json:"queued_tasks"`     // 队列中任务数
	LoadAvg1m      float64   `json:"load_avg_1m"`      // 系统负载均值（1分钟）
	LoadAvg5m      float64   `json:"load_avg_5m"`      // 系统负载均值（5分钟）
	LoadAvg15m     float64   `json:"load_avg_15m"`     // 系统负载均值（15分钟）
	NetworkInMbps  float64   `json:"network_in_mbps"`  // 入站网络带宽（Mbps）
	NetworkOutMbps float64   `json:"network_out_mbps"` // 出站网络带宽（Mbps）
	DiskIOUtil     float64   `json:"disk_io_util"`     // 磁盘 IO 利用率（0-100）
	Timestamp      time.Time `json:"timestamp"`        // 采集时间戳
}

// IsOverloaded 判断资源是否过载（CPU 或内存超过阈值）
func (r *ResourceUsage) IsOverloaded(cpuThreshold, memThreshold float64) bool {
	return r.CPUPercent > cpuThreshold || r.MemoryPercent > memThreshold
}
