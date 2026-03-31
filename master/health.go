/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \go-distributed\master\health.go
 * @Description: 健康检查器 - 定期检测节点心跳超时并标记不健康/恢复
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"time"
)

// HealthChecker 泛型健康检查器 - 定期检测节点心跳，自动标记不健康/恢复
type HealthChecker[T common.NodeInfo] struct {
	pool         *NodePool[T]               // 节点池
	interval     time.Duration              // 检查间隔
	timeout      time.Duration              // 心跳超时时间
	maxFailures  int                        // 最大失败次数
	failureCount *syncx.Map[string, int32]  // 失败计数器
	logger       logger.ILogger             // 日志
	taskManager  *syncx.PeriodicTaskManager // 定时任务管理器
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker[T common.NodeInfo](pool *NodePool[T], interval, timeout time.Duration, maxFailures int, log logger.ILogger) *HealthChecker[T] {
	return &HealthChecker[T]{
		pool:         pool,
		interval:     interval,
		timeout:      timeout,
		maxFailures:  maxFailures,
		failureCount: syncx.NewMap[string, int32](),
		logger:       log,
		taskManager:  syncx.NewPeriodicTaskManager(),
	}
}

// Start 启动健康检查定时任务
func (hc *HealthChecker[T]) Start(ctx context.Context) {
	task := syncx.NewPeriodicTask("health-check", hc.interval, func(taskCtx context.Context) error {
		hc.checkAll()
		return nil
	}).
		SetOnError(func(name string, err error) {
			hc.logger.ErrorContextKV(ctx, "Health check task error", "task", name, "error", err)
		}).
		SetOnStart(func(name string) {
			hc.logger.InfoContextKV(ctx, "Health checker started", "interval", hc.interval)
		}).
		SetOnStop(func(name string) {
			hc.logger.InfoContextKV(ctx, "Health checker stopped")
		})

	hc.taskManager.AddTask(task)
	hc.taskManager.Start()
}

// Stop 停止健康检查
func (hc *HealthChecker[T]) Stop() {
	hc.taskManager.Stop()
}

// checkAll 检查所有节点的心跳状态
func (hc *HealthChecker[T]) checkAll() {
	nodes := hc.pool.GetAll()
	syncx.ParallelForEachSlice(nodes, func(idx int, node T) {
		hc.checkNode(node)
	})
}

// checkNode 检查单个节点的心跳状态
func (hc *HealthChecker[T]) checkNode(node T) {
	if time.Since(node.GetLastHeartbeat()) > hc.timeout {
		hc.handleFailure(node)
	} else {
		hc.handleSuccess(node)
	}
}

// handleFailure 处理心跳超时 - 增加失败计数，达到阈值标记为不健康并从节点池中移除
func (hc *HealthChecker[T]) handleFailure(node T) {
	count, _ := hc.failureCount.Load(node.GetID())
	count++
	hc.failureCount.Store(node.GetID(), count)

	if int(count) >= hc.maxFailures {
		if err := hc.pool.MarkUnhealthy(node.GetID()); err != nil {
			hc.logger.ErrorKV("Failed to mark node as unhealthy",
				"node_id", node.GetID(), "error", err)
			return
		}

		// 从节点池中移除不健康的节点
		if err := hc.pool.Unregister(node.GetID()); err != nil {
			hc.logger.ErrorKV("Failed to unregister unhealthy node",
				"node_id", node.GetID(), "error", err)
			return
		}

		hc.logger.WarnKV("Node marked as unhealthy and removed from pool",
			"node_id", node.GetID(),
			"hostname", node.GetHostname(),
			"failures", count)
	}
}

// handleSuccess 处理心跳恢复 - 清除失败计数，标记为健康
func (hc *HealthChecker[T]) handleSuccess(node T) {
	if count, loaded := hc.failureCount.Load(node.GetID()); loaded && count > 0 {
		hc.failureCount.Delete(node.GetID())

		if err := hc.pool.MarkHealthy(node.GetID()); err == nil {
			hc.logger.InfoKV("Node recovered to healthy",
				"node_id", node.GetID(),
				"hostname", node.GetHostname())
		}
	}
}

// SetInterval 设置检查间隔
func (hc *HealthChecker[T]) SetInterval(interval time.Duration) {
	hc.interval = interval
}

// SetTimeout 设置心跳超时时间
func (hc *HealthChecker[T]) SetTimeout(timeout time.Duration) {
	hc.timeout = timeout
}

// SetMaxFailures 设置最大失败次数
func (hc *HealthChecker[T]) SetMaxFailures(max int) {
	hc.maxFailures = max
}
