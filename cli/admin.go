/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 00:00:00
 * @FilePath: \go-distributed\cli\admin.go
 * @Description: CLI 客户端 - AdminService API 调用
 *
 * 提供类似 kubectl 的集群管理 API:
 *   - ListNodes:      类似 kubectl get nodes
 *   - GetNodeInfo:    获取节点详情
 *   - GetClusterStats: 类似 kubectl top nodes
 *   - ListTasks:      类似 kubectl get jobs
 *   - DrainNode:      类似 kubectl drain
 *   - EvictNode:      类似 kubectl delete node
 *   - DisableNode:    类似 kubectl cordon
 *   - EnableNode:     类似 kubectl uncordon
 *   - GetNodeTop:     类似 kubectl top node
 *   - GetNodeLogs:    类似 kubectl logs
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"

	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
)

// ListNodes 列出节点（类似 kubectl get nodes）
func (c *Client) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	resp, err := c.client.ListNodes(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrListNodesFailed, err)
	}
	return resp, nil
}

// GetNodeInfo 获取节点详情
func (c *Client) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	resp, err := c.client.GetNodeInfo(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrGetNodeInfoFailed, err)
	}
	return resp, nil
}

// GetClusterStats 获取集群统计（类似 kubectl top nodes）
func (c *Client) GetClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error) {
	resp, err := c.client.GetClusterStats(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrGetClusterStatsFailed, err)
	}
	return resp, nil
}

// ListTasks 列出任务（类似 kubectl get jobs）
func (c *Client) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	resp, err := c.client.ListTasks(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrListTasksFailed, err)
	}
	return resp, nil
}

// DrainNode 排空节点（类似 kubectl drain）
func (c *Client) DrainNode(ctx context.Context, req *pb.DrainNodeRequest) (*pb.DrainNodeResponse, error) {
	resp, err := c.client.DrainNode(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrDrainNodeFailed, err)
	}
	return resp, nil
}

// EvictNode 驱逐节点（类似 kubectl delete node）
func (c *Client) EvictNode(ctx context.Context, req *pb.EvictNodeRequest) (*pb.EvictNodeResponse, error) {
	resp, err := c.client.EvictNode(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrEvictNodeFailed, err)
	}
	return resp, nil
}

// DisableNode 停用节点（类似 kubectl cordon）
func (c *Client) DisableNode(ctx context.Context, req *pb.DisableNodeRequest) (*pb.DisableNodeResponse, error) {
	resp, err := c.client.DisableNode(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrDisableNodeFailed, err)
	}
	return resp, nil
}

// EnableNode 启用节点（类似 kubectl uncordon）
func (c *Client) EnableNode(ctx context.Context, req *pb.EnableNodeRequest) (*pb.EnableNodeResponse, error) {
	resp, err := c.client.EnableNode(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrEnableNodeFailed, err)
	}
	return resp, nil
}

// GetNodeTop 获取节点资源 Top（类似 kubectl top node）
func (c *Client) GetNodeTop(ctx context.Context, req *pb.GetNodeTopRequest) (*pb.GetNodeTopResponse, error) {
	resp, err := c.client.GetNodeTop(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrGetNodeTopFailed, err)
	}
	return resp, nil
}

// GetNodeLogs 获取节点日志（类似 kubectl logs）
func (c *Client) GetNodeLogs(ctx context.Context, req *pb.GetNodeLogsRequest) (*pb.GetNodeLogsResponse, error) {
	resp, err := c.client.GetNodeLogs(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf(common.ErrGetNodeLogsFailed, err)
	}
	return resp, nil
}

// ListMasters 列出 Master 节点（类似 kubectl get nodes -l role=master）
func (c *Client) ListMasters(ctx context.Context, req *pb.ListMastersRequest) (*pb.ListMastersResponse, error) {
	resp, err := c.client.ListMasters(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("failed to list masters: %w", err)
	}
	return resp, nil
}

// ListWorkers 列出 Worker 节点（类似 kubectl get nodes -l role=worker）
func (c *Client) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	resp, err := c.client.ListWorkers(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("failed to list workers: %w", err)
	}
	return resp, nil
}

// AddTaint 给节点添加污点（类似 kubectl taint）
func (c *Client) AddTaint(ctx context.Context, req *pb.AddTaintRequest) (*pb.AddTaintResponse, error) {
	resp, err := c.client.AddTaint(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("failed to add taint: %w", err)
	}
	return resp, nil
}

// RemoveTaint 移除节点污点
func (c *Client) RemoveTaint(ctx context.Context, req *pb.RemoveTaintRequest) (*pb.RemoveTaintResponse, error) {
	resp, err := c.client.RemoveTaint(c.withAuth(ctx), req)
	if err != nil {
		c.handleRPCErr(err)
		return nil, fmt.Errorf("failed to remove taint: %w", err)
	}
	return resp, nil
}
