/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-31 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 00:00:00
 * @FilePath: \kronos-cluster\examples\cli.go
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

	"github.com/kamalyes/kronos-cluster/cli"
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/kronos-cluster/logger"
	pb "github.com/kamalyes/kronos-cluster/proto"
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
		configPath = fmt.Sprintf("%s/.kronos-cluster/config.yaml", homeDir)
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
	case "workers":
		handleGetWorkers(ctx, client, log)
	case "masters":
		handleGetMasters(ctx, client, log)
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
		role := node.Role.String()
		taints := formatTaints(node.Taints)
		log.InfoKV("Node",
			"node_id", nodeId,
			"role", role,
			"state", node.State.String(),
			"schedulable", node.Schedulable,
			"taints", taints,
			"active_tasks", node.ActiveTaskCount)
	}
}

func handleGetMasters(ctx context.Context, client *cli.Client, log logger.ILogger) {
	resp, err := client.ListMasters(ctx, &pb.ListMastersRequest{
		IncludeOffline: true,
	})
	if err != nil {
		log.ErrorKV("Failed to list masters", "error", err.Error())
		return
	}
	log.InfoKV("Masters", "count", len(resp.Masters), "leaders", resp.LeaderCount)
	for _, master := range resp.Masters {
		var nodeId string
		if master.NodeInfo != nil {
			nodeId = master.NodeInfo.NodeId
		}
		taints := formatTaints(master.Taints)
		log.InfoKV("Master",
			"node_id", nodeId,
			"is_leader", master.IsLeader,
			"cluster", master.ClusterName,
			"state", master.State.String(),
			"schedulable", master.Schedulable,
			"taints", taints)
	}
}

func handleGetWorkers(ctx context.Context, client *cli.Client, log logger.ILogger) {
	resp, err := client.ListWorkers(ctx, &pb.ListWorkersRequest{
		IncludeOffline: true,
	})
	if err != nil {
		log.ErrorKV("Failed to list workers", "error", err.Error())
		return
	}
	log.InfoKV("Workers", "count", len(resp.Workers), "healthy", resp.HealthyCount)
	for _, worker := range resp.Workers {
		var nodeId string
		if worker.NodeInfo != nil {
			nodeId = worker.NodeInfo.NodeId
		}
		taints := formatTaints(worker.Taints)
		log.InfoKV("Worker",
			"node_id", nodeId,
			"state", worker.State.String(),
			"schedulable", worker.Schedulable,
			"taints", taints,
			"active_tasks", worker.ActiveTaskCount)
	}
}

func formatTaints(taints []*pb.Taint) string {
	if len(taints) == 0 {
		return "<none>"
	}
	result := ""
	for i, t := range taints {
		if i > 0 {
			result += ", "
		}
		effect := t.Effect.String()
		if t.Value != "" {
			result += fmt.Sprintf("%s=%s:%s", t.Key, t.Value, effect)
		} else {
			result += fmt.Sprintf("%s:%s", t.Key, effect)
		}
	}
	return result
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
	fmt.Println("  cli get nodes         List all nodes (masters + workers)")
	fmt.Println("  cli get masters       List master nodes only")
	fmt.Println("  cli get workers       List worker nodes only")
	fmt.Println("  cli get tasks         List all tasks")
	fmt.Println("  cli get stats         Get cluster statistics")
	fmt.Println("")
	fmt.Println("Options:")
	fmt.Println("  --config PATH         Path to config file (default: ~/.kronos-cluster/config.yaml)")
}
