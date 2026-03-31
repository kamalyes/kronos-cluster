/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 00:00:00
 * @FilePath: \go-distributed\cli\admin.go
 * @Description: CLI 客户端 - AdminService API 调用
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	pb "github.com/kamalyes/go-distributed/proto"
)

// ListNodes 列出节点（类似 kubectl get nodes）
func (c *Client) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	resp, err := c.client.ListNodes(ctx, req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("list nodes failed: %w", err)
	}
	return resp, nil
}

// GetNodeInfo 获取节点详情
func (c *Client) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	resp, err := c.client.GetNodeInfo(ctx, req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("get node info failed: %w", err)
	}
	return resp, nil
}

// GetClusterStats 获取集群统计（类似 kubectl top nodes）
func (c *Client) GetClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error) {
	resp, err := c.client.GetClusterStats(ctx, req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("get cluster stats failed: %w", err)
	}
	return resp, nil
}

// ListTasks 列出任务（类似 kubectl get jobs）
func (c *Client) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	resp, err := c.client.ListTasks(ctx, req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("list tasks failed: %w", err)
	}
	return resp, nil
}

// DrainNode 排空节点（类似 kubectl drain）
func (c *Client) DrainNode(ctx context.Context, req *pb.DrainNodeRequest) (*pb.DrainNodeResponse, error) {
	resp, err := c.client.DrainNode(ctx, req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("drain node failed: %w", err)
	}
	return resp, nil
}
