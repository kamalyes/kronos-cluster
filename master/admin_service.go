/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:15:55
 * @FilePath: \go-distributed\master\admin_service.go
 * @Description: Master AdminService gRPC 服务端实现
 *
 * 实现 proto AdminServiceServer 接口，提供集群管理 API：
 *   - ListNodes:      类似 kubectl get nodes
 *   - GetNodeInfo:    获取节点详情
 *   - GetClusterStats: 类似 kubectl top nodes
 *   - ListTasks:      类似 kubectl get jobs
 *   - DrainNode:      类似 kubectl drain
 *
 * CLI/Dashboard 通过 gRPC 客户端调用这些接口获取集群信息
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
)

// AdminService 管理服务 - 实现 pb.AdminServiceServer 接口
type AdminService struct {
	pb.UnimplementedAdminServiceServer
	pool   NodeProvider
	store  TaskStore
	logger logger.ILogger
}

// NewAdminService 创建管理服务
func NewAdminService(
	pool NodeProvider,
	store TaskStore,
	log logger.ILogger,
) *AdminService {
	return &AdminService{
		pool:   pool,
		store:  store,
		logger: log,
	}
}

// ListNodes 列出节点（类似 kubectl get nodes）
func (s *AdminService) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodeFilter := common.NewNodeFilterFromRequest(req)
	filtered := nodeFilter.FilterNodes(s.pool.GetAll())

	pbNodes := make([]*pb.NodeDetail, 0, len(filtered))
	for _, node := range filtered {
		pbNodes = append(pbNodes, common.CommonNodeToNodeDetail(node))
	}

	return &pb.ListNodesResponse{
		Nodes:      pbNodes,
		TotalCount: int32(len(pbNodes)),
	}, nil
}

// GetNodeInfo 获取节点详情
func (s *AdminService) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	return &pb.GetNodeInfoResponse{
		Node: common.CommonNodeToNodeDetail(node),
	}, nil
}

// GetClusterStats 获取集群统计（类似 kubectl top nodes）
func (s *AdminService) GetClusterStats(ctx context.Context, req *pb.ClusterStatsRequest) (*pb.ClusterStatsResponse, error) {
	nodes := s.pool.GetAll()

	collector := common.NewClusterStatsCollector()
	collector.Collect(nodes, req.IncludeResourceStats)

	resp := &pb.ClusterStatsResponse{
		TotalNodes:     int32(len(nodes)),
		HealthyNodes:   collector.HealthyNodes,
		OfflineNodes:   collector.OfflineNodes,
		DrainingNodes:  collector.DrainingNodes,
		NodesByRegion:  collector.NodesByRegion,
		NodesByState:   collector.NodesByState,
		AvgCpuUsage:    collector.AvgCPU(),
		AvgMemoryUsage: collector.AvgMemory(),
	}

	if req.IncludeTaskStats {
		s.appendTaskStats(ctx, resp)
	}

	return resp, nil
}

// appendTaskStats 追加任务统计到响应
func (s *AdminService) appendTaskStats(ctx context.Context, resp *pb.ClusterStatsResponse) {
	stats, err := s.store.TaskStats(ctx)
	if err != nil {
		s.logger.WarnKV("Failed to get task stats", "error", err)
		return
	}
	if stats == nil {
		return
	}
	resp.TotalRunningTasks = int32(stats.Running)
	resp.TotalPendingTasks = int32(stats.Pending)
}

// ListTasks 列出任务（类似 kubectl get jobs）
func (s *AdminService) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	tasks, err := s.loadTasks(ctx, req)
	if err != nil {
		return nil, err
	}

	taskFilter := common.NewTaskFilterFromRequest(req)
	tasks = taskFilter.Apply(tasks)

	pbTasks := make([]*pb.TaskInfo, 0, len(tasks))
	for _, t := range tasks {
		pbTasks = append(pbTasks, common.CommonTaskToProto(t))
	}

	return &pb.ListTasksResponse{
		Tasks:      pbTasks,
		TotalCount: int32(len(pbTasks)),
	}, nil
}

// loadTasks 根据请求条件加载任务列表
func (s *AdminService) loadTasks(ctx context.Context, req *pb.ListTasksRequest) ([]*common.TaskInfo, error) {
	if req.NodeIdFilter != "" {
		tasks, err := s.store.ListTasksByNode(ctx, req.NodeIdFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to list tasks by node: %w", err)
		}
		return tasks, nil
	}

	tasks, err := s.store.ListTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	return tasks, nil
}

// DrainNode 排空节点（类似 kubectl drain）
func (s *AdminService) DrainNode(ctx context.Context, req *pb.DrainNodeRequest) (*pb.DrainNodeResponse, error) {
	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	node.SetState(common.NodeStateDraining)
	s.logger.InfoKV("Node draining", "nodeID", req.NodeId, "force", req.Force, "reason", req.Reason)

	return &pb.DrainNodeResponse{
		Accepted: true,
		Message:  fmt.Sprintf("node %s is now draining", req.NodeId),
	}, nil
}
