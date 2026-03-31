/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-31 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 00:00:00
 * @FilePath: \go-distributed\examples\cli.go
 * @Description: 命令行工具 - 使用配置文件与分布式系统交互
 *
 * 配置文件格式 (YAML):
 * current_context: default
 * contexts:
 *   - name: default
 *     cluster_name: my-cluster
 *     auth_info: my-auth
 * clusters:
 *   - name: my-cluster
 *     server: localhost:9001
 *     insecure: true
 * auth_infos:
 *   - name: my-auth
 *     secret: my-secret
 *     enable_auth: true
 *
 * Usage:
 *   cli get nodes
 *   cli get tasks
 *   cli get stats
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kamalyes/go-distributed/cli"
	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-distributed/logger"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-toolbox/pkg/random"
)

var (
	configPath string
)

func main() {
	log := logger.NewDistributedLogger("cli")

	flag.StringVar(&configPath, "config", "", "配置文件路径")
	flag.Parse()

	if configPath == "" {
		homeDir, _ := os.UserHomeDir()
		configPath = fmt.Sprintf("%s/.go-distributed/config.yaml", homeDir)
	}

	cpFile, err := common.LoadControlPlaneConfig(configPath)
	if err != nil {
		log.ErrorKV("Failed to load config", "path", configPath, "error", err.Error())
		os.Exit(1)
	}

	cpConfig, err := cpFile.ResolveCurrentConfig()
	if err != nil {
		log.ErrorKV("Failed to resolve config", "error", err.Error())
		os.Exit(1)
	}

	client, err := cli.NewClientFromControlPlane(cpConfig, cli.WithLogger(log))
	if err != nil {
		log.ErrorKV("Failed to create client", "error", err.Error())
		os.Exit(1)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := authenticateClient(ctx, client, cpConfig, log); err != nil {
		log.ErrorKV("Failed to authenticate", "error", err.Error())
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	resource := args[1]

	switch command {
	case "get":
		handleGetCommand(ctx, client, log, resource)
	default:
		printUsage()
		os.Exit(1)
	}
}

func authenticateClient(ctx context.Context, client *cli.Client, config *common.ControlPlaneConfig, log logger.ILogger) error {
	if config.EnableAuth && config.Token == "" {
		token, err := client.Authenticate(ctx, config.Secret, fmt.Sprintf("cli-%s", random.UUID()))
		if err != nil {
			return fmt.Errorf("authentication failed: %w", err)
		}
		log.InfoKV("Authenticated successfully", "token", token)
	}
	return nil
}

func handleGetCommand(ctx context.Context, client *cli.Client, log logger.ILogger, resource string) {
	switch resource {
	case "nodes":
		handleGetNodes(ctx, client, log)
	case "tasks":
		handleGetTasks(ctx, client, log)
	case "stats":
		handleNodeTop(ctx, client, log)
	default:
		log.ErrorKV("Unknown resource", "resource", resource)
		printUsage()
	}
}

func handleGetNodes(ctx context.Context, client *cli.Client, log logger.ILogger) {
	resp, err := client.ListNodes(ctx, &pb.ListNodesRequest{
		IncludeOffline: true,
	})
	if err != nil {
		log.ErrorKV("Failed to list nodes", "error", err.Error())
		return
	}
	log.InfoKV("Nodes", "count", len(resp.Nodes))
	for _, node := range resp.Nodes {
		var nodeId string
		if node.NodeInfo != nil {
			nodeId = node.NodeInfo.NodeId
		}
		log.InfoKV("Node",
			"node_id", nodeId,
			"state", node.State.String(),
			"schedulable", node.Schedulable,
			"active_tasks", node.ActiveTaskCount)
	}
}

func handleGetTasks(ctx context.Context, client *cli.Client, log logger.ILogger) {
	resp, err := client.ListTasks(ctx, &pb.ListTasksRequest{})
	if err != nil {
		log.ErrorKV("Failed to list tasks", "error", err.Error())
		return
	}
	log.InfoKV("Tasks", "count", len(resp.Tasks))
	for _, task := range resp.Tasks {
		log.InfoKV("Task",
			"task_id", task.TaskId,
			"task_type", task.TaskType,
			"state", task.State.String(),
			"priority", task.Priority)
	}
}

func handleNodeTop(ctx context.Context, client *cli.Client, log logger.ILogger) {
	resp, err := client.GetClusterStats(ctx, &pb.ClusterStatsRequest{})
	if err != nil {
		log.ErrorKV("Failed to get cluster stats", "error", err.Error())
		return
	}
	log.InfoKV("Cluster stats",
		"total_nodes", resp.TotalNodes,
		"healthy_nodes", resp.HealthyNodes,
		"offline_nodes", resp.OfflineNodes,
		"running_tasks", resp.TotalRunningTasks,
		"pending_tasks", resp.TotalPendingTasks,
		"avg_cpu_usage", resp.AvgCpuUsage,
		"avg_memory_usage", resp.AvgMemoryUsage)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  cli get nodes         List all nodes in the cluster")
	fmt.Println("  cli get tasks         List all tasks")
	fmt.Println("  cli get stats         Get cluster statistics")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  --config PATH         Path to config file (default: ~/.go-distributed/config.yaml)")
}
