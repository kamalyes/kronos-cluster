/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 19:05:28
 * @FilePath: \kronos-cluster\examples\worker.go
 * @Description: 主程序 - 启动 Worker 工作控制器
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/logger"
	"github.com/kamalyes/kronos-cluster/worker"
	"github.com/kamalyes/go-toolbox/pkg/random"
)

func main() {
	log := logger.NewDistributedLogger("worker")

	w, err := worker.NewWorker[*common.BaseNodeInfo](&common.WorkerConfig{
		WorkerID:           fmt.Sprintf("worker-%s", random.UUID()),
		MasterAddr:         "localhost:9001",
		TransportType:      common.TransportTypeGRPC,
		ResourceMonitor:    true,
		MaxConcurrentTasks: 10,
		JoinSecret:         "REPLACE_WITH_REAL_TOKEN",
	}, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{
			Labels: map[string]string{
				"type":   "worker",
				"region": "beijing",
			},
		}
	}, log)
	if err != nil {
		log.Fatal(err.Error())
	}

	executor := w.GetTaskExecutor()

	err = executor.RegisterHandlerFunc(common.TaskTypeCommand, func(ctx context.Context, task *common.TaskInfo) (*common.TaskResult, error) {
		log.InfoKV("Executing command task", "task_id", task.ID, "command", string(task.Payload))

		time.Sleep(2 * time.Second)

		executor.ReportProgress(task.ID, 0.5)

		time.Sleep(2 * time.Second)

		if ctx.Err() == context.Canceled {
			log.InfoKV("Task cancelled", "task_id", task.ID)
			return nil, ctx.Err()
		}

		result := fmt.Sprintf("Command executed: %s", string(task.Payload))
		log.InfoKV("Command executed successfully", "task_id", task.ID, "result", result)

		return &common.TaskResult{
			Data:  []byte(result),
			Error: "",
		}, nil
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	err = executor.RegisterHandlerFunc(common.TaskTypeHTTP, func(ctx context.Context, task *common.TaskInfo) (*common.TaskResult, error) {
		log.InfoKV("Executing HTTP task", "task_id", task.ID, "url", string(task.Payload))

		time.Sleep(3 * time.Second)

		result := fmt.Sprintf("HTTP request completed: %s", string(task.Payload))
		log.InfoKV("HTTP task completed", "task_id", task.ID, "result", result)

		return &common.TaskResult{
			Data:  []byte(result),
			Error: "",
		}, nil
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	err = executor.RegisterHandlerFunc(common.TaskTypeCustom, func(ctx context.Context, task *common.TaskInfo) (*common.TaskResult, error) {
		log.InfoKV("Executing custom task", "task_id", task.ID, "payload", string(task.Payload))

		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				log.InfoKV("Custom task cancelled", "task_id", task.ID)
				return nil, ctx.Err()
			default:
				progress := float64(i+1) / 10.0
				executor.ReportProgress(task.ID, progress)
				time.Sleep(500 * time.Millisecond)
			}
		}

		result := fmt.Sprintf("Custom task completed: %s", string(task.Payload))
		log.InfoKV("Custom task completed", "task_id", task.ID, "result", result)

		return &common.TaskResult{
			Data:  []byte(result),
			Error: "",
		}, nil
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := w.Start(context.Background()); err != nil {
		log.Fatal(err.Error())
	}
	defer w.Stop()

	log.Info("Worker started and ready to receive tasks")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			runningTasks := executor.ListRunningTasks()
			log.InfoKV("Worker status",
				"running_tasks", len(runningTasks),
				"is_idle", executor.IsIdle(),
				"tasks", runningTasks,
			)
		}
	}
}
