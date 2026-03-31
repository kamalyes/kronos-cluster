/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 19:09:16
 * @FilePath: \go-distributed\examples\main.go
 * @Description: 主程序 - 启动 Master 主控制器
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-distributed/logger"
	"github.com/kamalyes/go-distributed/master"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/random"
)

func main() {
	log := logger.NewDistributedLogger("master")

	converter := func(info common.NodeInfo) (*common.BaseNodeInfo, error) {
		base, ok := info.(*common.BaseNodeInfo)
		if !ok {
			return nil, errorx.WrapError("invalid node type")
		}
		return base, nil
	}

	m, err := master.NewMaster[*common.BaseNodeInfo](&common.MasterConfig{
		GRPCPort:             9001,
		TransportType:        common.TransportTypeGRPC,
		HeartbeatInterval:    5 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		HeartbeatMaxFailures: 3,
		EnableAuth:           true,
		Secret:               "my-jwt-signing-secret-key",
		TokenExpiration:      24 * time.Hour,
		GenerateConfigFile:   true,
		ControlPlane: &common.ControlPlaneConfig{
			ServerAddr: "localhost:9001",
			EnableAuth: true,
		},
	}, converter, master.NewMemoryTaskStore(log), log)
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := m.Start(context.Background()); err != nil {
		log.Fatal(err.Error())
	}
	defer m.Stop()

	log.Info("Master started on port 9000")

	entry := m.GetAuthManager().CreateJoinSecret(2*time.Hour, "example worker token", 0, map[string]string{
		"purpose": "example",
		"env":     "development",
	})
	log.InfoKV("Join secret created",
		"token", entry.ID+"."+entry.Secret,
		"expires_at", entry.ExpiresAt,
		"description", entry.Description)

	// 自动生成配置文件
	log.Info("Config file generation is now controlled by MasterConfig.GenerateConfigFile option")

	// 启动一个协程，定期提交新任务
	go func() {
		taskCounter := 3
		taskTicker := time.NewTicker(5 * time.Second)
		defer taskTicker.Stop()

		for {
			<-taskTicker.C

			// 提交命令任务
			cmdTask, err := m.GetTaskManager().SubmitTask(
				common.TaskTypeCommand,
				[]byte("echo 'Hello from task "+fmt.Sprintf("%d", taskCounter)+"'"),
				master.WithPriority(10),
				master.WithTimeout(30*time.Second),
				master.WithMaxRetries(3),
				master.WithMetadata(map[string]string{
					"owner":    random.UUID(),
					"env":      "production",
					"task_num": string(rune(taskCounter + '0')),
					"type":     "command",
				}),
			)
			if err != nil {
				log.ErrorKV("Failed to submit task", "error", err)
				continue
			}

			log.InfoKV("Task submitted", "task_id", cmdTask.ID, "type", cmdTask.Type, "task_num", taskCounter)

			// 每提交3个任务，提交一个HTTP任务
			if taskCounter%3 == 0 {
				httpTask, err := m.GetTaskManager().SubmitTask(
					common.TaskTypeHTTP,
					[]byte(`{"url": "http://example.com", "method": "GET", "timeout": 10}`),
					master.WithPriority(5),
					master.WithTimeout(15*time.Second),
					master.WithMaxRetries(2),
					master.WithMetadata(map[string]string{
						"owner":    random.UUID(),
						"env":      "production",
						"task_num": string(rune(taskCounter + '0')),
						"type":     "http",
					}),
				)
				if err != nil {
					log.ErrorKV("Failed to submit HTTP task", "error", err)
				} else {
					log.InfoKV("HTTP task submitted", "task_id", httpTask.ID, "type", httpTask.Type, "task_num", taskCounter)
				}
			}

			taskCounter++
			if taskCounter > 100 {
				taskCounter = 1
			}
		}
	}()

	// 主协程，定期打印所有任务的状态
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 这里可以添加代码来获取和打印所有任务的状态
			// 为了简洁，我们只打印一条信息表示系统正在运行
			log.Info("Master is running and generating tasks...")
		}
	}
}
