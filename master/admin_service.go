/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:15:55
 * @FilePath: \kronos-cluster\master\admin_service.go
 * @Description: Master AdminService gRPC 服务端实现
 *
 * 实现 proto AdminServiceServer 接口，提供集群管理 API：
 * CLI/Dashboard 通过 gRPC 客户端调用这些接口获取集群信息
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
 *   - Authenticate:   CLI 客户端认证
 *   - ClusterOverview: 集群概览
 *   - UpdateNodeLabels: 类似 kubectl label
 *   - AdminCancelTask: 管理端取消任务
 *   - AdminRetryTask:  管理端重试任务
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"fmt"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	pb "github.com/kamalyes/kronos-cluster/proto"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
)

// AdminService 管理服务 - 实现 pb.AdminServiceServer 接口
type AdminService struct {
	pb.UnimplementedAdminServiceServer
	pool        NodeProvider
	masterPool  *MasterPool
	store       TaskStore
	logger      logger.ILogger
	authManager *common.AuthManager
	startTime   time.Time
}

// NewAdminService 创建管理服务
func NewAdminService(
	pool NodeProvider,
	masterPool *MasterPool,
	store TaskStore,
	log logger.ILogger,
	authManager *common.AuthManager,
) *AdminService {
	return &AdminService{
		pool:        pool,
		masterPool:  masterPool,
		store:       store,
		logger:      log,
		authManager: authManager,
		startTime:   time.Now(),
	}
}

// validateAuth 验证请求中的认证令牌
func (s *AdminService) validateAuth(ctx context.Context) error {
	if s.authManager == nil || !s.authManager.IsAuthEnabled() {
		return nil
	}

	token, err := extractTokenFromContext(ctx)
	if err != nil {
		return errorx.WrapError(common.ErrAuthRequired, err)
	}

	_, err = s.authManager.ValidateAdminToken(token)
	if err != nil {
		return errorx.WrapError(common.ErrAccessDenied, err)
	}

	return nil
}

// extractTokenFromContext 从 gRPC 上下文中提取认证令牌
func extractTokenFromContext(ctx context.Context) (string, error) {
	md, ok := common.ExtractMetadata(ctx)
	if !ok {
		return "", fmt.Errorf(common.ErrMetadataNotFound)
	}

	token := md.Get("authorization")
	if token == "" {
		token = md.Get("token")
	}

	if token == "" {
		return "", fmt.Errorf(common.ErrAuthTokenNotFound)
	}

	return token, nil
}

// ListNodes 列出节点（类似 kubectl get nodes）
// 返回所有节点，包括 Master 和 Worker
func (s *AdminService) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	nodeFilter := common.NewNodeFilterFromRequest(req)

	workerNodes := s.pool.GetAll()
	filteredWorkers := nodeFilter.FilterNodes(workerNodes)

	masterNodes := s.masterPool.GetAllAsNodeInfo()
	filteredMasters := nodeFilter.FilterNodes(masterNodes)

	allNodes := make([]*pb.NodeDetail, 0, len(filteredWorkers)+len(filteredMasters))
	for _, node := range filteredMasters {
		allNodes = append(allNodes, common.CommonNodeToNodeDetail(node))
	}
	for _, node := range filteredWorkers {
		allNodes = append(allNodes, common.CommonNodeToNodeDetail(node))
	}

	return &pb.ListNodesResponse{
		Nodes:      allNodes,
		TotalCount: int32(len(allNodes)),
	}, nil
}

// GetNodeInfo 获取节点详情
func (s *AdminService) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

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
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

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
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

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
			return nil, fmt.Errorf(common.ErrFailedListTasksByNode, err)
		}
		return tasks, nil
	}

	tasks, err := s.store.ListTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf(common.ErrFailedListTasksAll, err)
	}
	return tasks, nil
}

// DrainNode 排空节点（类似 kubectl drain）
func (s *AdminService) DrainNode(ctx context.Context, req *pb.DrainNodeRequest) (*pb.DrainNodeResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	node.SetState(common.NodeStateDraining)
	node.SetSchedulable(false)
	node.SetDisableReason("draining: " + req.Reason)

	s.logger.InfoKV("Node draining", "nodeID", req.NodeId, "force", req.Force, "reason", req.Reason)

	return &pb.DrainNodeResponse{
		Accepted: true,
		Message:  fmt.Sprintf("node %s is now draining", req.NodeId),
	}, nil
}

// EvictNode 驱逐节点（类似 kubectl delete node）
func (s *AdminService) EvictNode(ctx context.Context, req *pb.EvictNodeRequest) (*pb.EvictNodeResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	node.SetState(common.NodeStateOffline)
	s.logger.InfoKV("Node evicted",
		"node_id", req.NodeId,
		"hostname", node.GetHostname(),
		"reason", req.Reason,
		"force", req.Force,
		"reschedule_tasks", req.RescheduleTasks)

	return &pb.EvictNodeResponse{
		Success: true,
		Message: fmt.Sprintf("node %s evicted successfully", req.NodeId),
	}, nil
}

// DisableNode 停用节点（类似 kubectl cordon）
func (s *AdminService) DisableNode(ctx context.Context, req *pb.DisableNodeRequest) (*pb.DisableNodeResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	if !node.IsSchedulable() {
		return nil, fmt.Errorf(common.ErrNodeAlreadyDisabled, req.NodeId)
	}

	node.SetSchedulable(false)
	node.SetDisableReason(req.Reason)

	s.logger.InfoKV("Node disabled (cordon)",
		"node_id", req.NodeId,
		"reason", req.Reason)

	return &pb.DisableNodeResponse{
		Success: true,
		Message: fmt.Sprintf("node %s disabled (cordoned)", req.NodeId),
	}, nil
}

// EnableNode 启用节点（类似 kubectl uncordon）
func (s *AdminService) EnableNode(ctx context.Context, req *pb.EnableNodeRequest) (*pb.EnableNodeResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	if node.IsSchedulable() {
		return nil, fmt.Errorf(common.ErrNodeAlreadyEnabled, req.NodeId)
	}

	node.SetSchedulable(true)
	node.SetDisableReason("")

	s.logger.InfoKV("Node enabled (uncordon)",
		"node_id", req.NodeId)

	return &pb.EnableNodeResponse{
		Success: true,
		Message: fmt.Sprintf("node %s enabled (uncordoned)", req.NodeId),
	}, nil
}

// GetNodeTop 获取节点资源 Top（类似 kubectl top node）
func (s *AdminService) GetNodeTop(ctx context.Context, req *pb.GetNodeTopRequest) (*pb.GetNodeTopResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	nodes := s.pool.GetAll()
	if req.NodeId != "" {
		node, ok := s.pool.Get(req.NodeId)
		if !ok {
			return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
		}
		nodes = []common.NodeInfo{node}
	}

	topItems := make([]*pb.NodeTopInfo, 0, len(nodes))
	for _, node := range nodes {
		usage := node.GetResourceUsage()
		item := &pb.NodeTopInfo{
			NodeId:      node.GetID(),
			Hostname:    node.GetHostname(),
			CpuUsage:    node.GetCurrentLoad() / 100.0,
			MemoryUsage: 0,
			State:       common.CommonNodeStateToProto(node.GetState()),
			Schedulable: node.IsSchedulable(),
		}
		if usage != nil {
			item.CpuUsage = usage.CPUPercent / 100.0
			item.MemoryUsage = usage.MemoryPercent / 100.0
			item.MemoryTotal = usage.MemoryTotal
			item.MemoryUsed = usage.MemoryUsed
			item.RunningTasks = int32(usage.ActiveTasks)
			item.LoadAvg_1M = usage.LoadAvg1m
			item.LoadAvg_5M = usage.LoadAvg5m
			item.LoadAvg_15M = usage.LoadAvg15m
		}
		topItems = append(topItems, item)
	}

	return &pb.GetNodeTopResponse{
		Nodes:     topItems,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// GetNodeLogs 获取节点日志（类似 kubectl logs）
func (s *AdminService) GetNodeLogs(ctx context.Context, req *pb.GetNodeLogsRequest) (*pb.GetNodeLogsResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	node, ok := s.pool.Get(req.NodeId)
	if !ok {
		return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
	}

	logs := []*pb.LogEntry{
		{
			Timestamp: time.Now().UnixMilli(),
			Level:     "INFO",
			Message:   fmt.Sprintf("Node %s is running, state: %v", req.NodeId, node.GetState()),
			Fields:    map[string]string{"node_id": req.NodeId, "hostname": node.GetHostname()},
		},
	}

	return &pb.GetNodeLogsResponse{
		Logs:    logs,
		HasMore: false,
		NodeId:  req.NodeId,
	}, nil
}

// Authenticate CLI 客户端认证
func (s *AdminService) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	if s.authManager == nil || !s.authManager.IsAuthEnabled() {
		return &pb.AuthResponse{
			Success: false,
			Message: common.ErrAuthDisabled,
		}, nil
	}

	token, err := s.authManager.AuthenticateAdmin(req.Secret, req.ClientId)
	if err != nil {
		return &pb.AuthResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.AuthResponse{
		Success: true,
		Token:   token,
		Message: "authenticated successfully",
	}, nil
}

// ListMasters 列出 Master 节点（类似 kubectl get nodes -l role=master）
func (s *AdminService) ListMasters(ctx context.Context, req *pb.ListMastersRequest) (*pb.ListMastersResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	masters := s.masterPool.GetAll()
	if !req.IncludeOffline {
		onlineMasters := make([]*common.MasterNodeInfo, 0, len(masters))
		for _, m := range masters {
			if m.GetState() != common.NodeStateOffline {
				onlineMasters = append(onlineMasters, m)
			}
		}
		masters = onlineMasters
	}

	if len(req.LabelSelector) > 0 {
		filtered := make([]*common.MasterNodeInfo, 0, len(masters))
		for _, m := range masters {
			if matchLabels(m.GetLabels(), req.LabelSelector) {
				filtered = append(filtered, m)
			}
		}
		masters = filtered
	}

	leaderCount := int32(0)
	pbMasters := make([]*pb.NodeDetail, 0, len(masters))
	for _, m := range masters {
		if m.IsLeader {
			leaderCount++
		}
		pbMasters = append(pbMasters, common.CommonNodeToNodeDetail(m))
	}

	return &pb.ListMastersResponse{
		Masters:     pbMasters,
		TotalCount:  int32(len(pbMasters)),
		LeaderCount: leaderCount,
	}, nil
}

// ListWorkers 列出 Worker 节点（类似 kubectl get nodes -l role=worker）
func (s *AdminService) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	workers := s.pool.GetAll()

	if len(req.FilterStates) > 0 {
		stateSet := make(map[common.NodeState]bool)
		for _, s := range req.FilterStates {
			stateSet[common.ProtoNodeStateToCommon(s)] = true
		}
		filtered := make([]common.NodeInfo, 0, len(workers))
		for _, w := range workers {
			if stateSet[w.GetState()] {
				filtered = append(filtered, w)
			}
		}
		workers = filtered
	}

	if req.RegionFilter != "" {
		filtered := make([]common.NodeInfo, 0, len(workers))
		for _, w := range workers {
			if w.GetRegion() == req.RegionFilter {
				filtered = append(filtered, w)
			}
		}
		workers = filtered
	}

	if !req.IncludeOffline {
		onlineWorkers := make([]common.NodeInfo, 0, len(workers))
		for _, w := range workers {
			if w.GetState() != common.NodeStateOffline {
				onlineWorkers = append(onlineWorkers, w)
			}
		}
		workers = onlineWorkers
	}

	if len(req.LabelSelector) > 0 {
		filtered := make([]common.NodeInfo, 0, len(workers))
		for _, w := range workers {
			if matchLabels(w.GetLabels(), req.LabelSelector) {
				filtered = append(filtered, w)
			}
		}
		workers = filtered
	}

	healthyCount := int32(0)
	pbWorkers := make([]*pb.NodeDetail, 0, len(workers))
	for _, w := range workers {
		if w.GetState() == common.NodeStateIdle || w.GetState() == common.NodeStateRunning {
			healthyCount++
		}
		pbWorkers = append(pbWorkers, common.CommonNodeToNodeDetail(w))
	}

	return &pb.ListWorkersResponse{
		Workers:      pbWorkers,
		TotalCount:   int32(len(pbWorkers)),
		HealthyCount: healthyCount,
	}, nil
}

// AddTaint 给节点添加污点（类似 kubectl taint）
func (s *AdminService) AddTaint(ctx context.Context, req *pb.AddTaintRequest) (*pb.AddTaintResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	taint := common.ProtoTaintToCommon(req.Taint)

	// 先在 Worker 池中查找
	if node, ok := s.pool.Get(req.NodeId); ok {
		node.AddTaint(taint)
		s.logger.InfoKV("Taint added to worker node",
			"node_id", req.NodeId,
			"taint_key", taint.Key,
			"taint_effect", string(taint.Effect))
		return &pb.AddTaintResponse{
			Success: true,
			Message: fmt.Sprintf("taint %s=%s:%s added to node %s", taint.Key, taint.Value, taint.Effect, req.NodeId),
		}, nil
	}

	// 再在 Master 池中查找
	if master, ok := s.masterPool.Get(req.NodeId); ok {
		master.AddTaint(taint)
		s.logger.InfoKV("Taint added to master node",
			"node_id", req.NodeId,
			"taint_key", taint.Key,
			"taint_effect", string(taint.Effect))
		return &pb.AddTaintResponse{
			Success: true,
			Message: fmt.Sprintf("taint %s=%s:%s added to master %s", taint.Key, taint.Value, taint.Effect, req.NodeId),
		}, nil
	}

	return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
}

// RemoveTaint 移除节点污点
func (s *AdminService) RemoveTaint(ctx context.Context, req *pb.RemoveTaintRequest) (*pb.RemoveTaintResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	// 先在 Worker 池中查找
	if node, ok := s.pool.Get(req.NodeId); ok {
		if !node.RemoveTaint(req.TaintKey) {
			return &pb.RemoveTaintResponse{
				Success: false,
				Message: fmt.Sprintf("taint key %s not found on node %s", req.TaintKey, req.NodeId),
			}, nil
		}
		s.logger.InfoKV("Taint removed from worker node",
			"node_id", req.NodeId,
			"taint_key", req.TaintKey)
		return &pb.RemoveTaintResponse{
			Success: true,
			Message: fmt.Sprintf("taint %s removed from node %s", req.TaintKey, req.NodeId),
		}, nil
	}

	// 再在 Master 池中查找
	if master, ok := s.masterPool.Get(req.NodeId); ok {
		if !master.RemoveTaint(req.TaintKey) {
			return &pb.RemoveTaintResponse{
				Success: false,
				Message: fmt.Sprintf("taint key %s not found on master %s", req.TaintKey, req.NodeId),
			}, nil
		}
		s.logger.InfoKV("Taint removed from master node",
			"node_id", req.NodeId,
			"taint_key", req.TaintKey)
		return &pb.RemoveTaintResponse{
			Success: true,
			Message: fmt.Sprintf("taint %s removed from master %s", req.TaintKey, req.NodeId),
		}, nil
	}

	return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
}

// ClusterOverview 集群概览
func (s *AdminService) ClusterOverview(ctx context.Context, req *pb.ClusterOverviewRequest) (*pb.ClusterOverviewResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	workerNodes := s.pool.GetAll()
	masterNodes := s.masterPool.GetAllAsNodeInfo()

	collector := common.NewClusterStatsCollector()
	collector.Collect(workerNodes, true)

	resp := &pb.ClusterOverviewResponse{
		TotalNodes:       int32(len(workerNodes) + len(masterNodes)),
		HealthyNodes:     collector.HealthyNodes,
		OfflineNodes:     collector.OfflineNodes,
		DrainingNodes:    collector.DrainingNodes,
		AvgCpuUsage:      collector.AvgCPU(),
		AvgMemoryUsage:   collector.AvgMemory(),
		NodesByRegion:    collector.NodesByRegion,
		NodesByState:     collector.NodesByState,
		TotalMasterNodes: int32(len(masterNodes)),
		TotalWorkerNodes: int32(len(workerNodes)),
		ClusterUptimeMs:  time.Since(s.startTime).Milliseconds(),
	}

	stats, err := s.store.TaskStats(ctx)
	if err != nil {
		s.logger.WarnKV("Failed to get task stats", "error", err)
	} else if stats != nil {
		resp.TotalRunningTasks = int32(stats.Running)
		resp.TotalPendingTasks = int32(stats.Pending)
	}

	return resp, nil
}

// UpdateNodeLabels 更新节点标签（类似 kubectl label）
func (s *AdminService) UpdateNodeLabels(ctx context.Context, req *pb.UpdateNodeLabelsRequest) (*pb.UpdateNodeLabelsResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	if node, ok := s.pool.Get(req.NodeId); ok {
		labels := node.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range req.Labels {
			labels[k] = v
		}
		s.logger.InfoKV("Labels updated on worker node", "node_id", req.NodeId, "label_count", len(req.Labels))
		return &pb.UpdateNodeLabelsResponse{
			Success: true,
			Message: fmt.Sprintf("labels updated on node %s", req.NodeId),
		}, nil
	}

	if master, ok := s.masterPool.Get(req.NodeId); ok {
		labels := master.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range req.Labels {
			labels[k] = v
		}
		s.logger.InfoKV("Labels updated on master node", "node_id", req.NodeId, "label_count", len(req.Labels))
		return &pb.UpdateNodeLabelsResponse{
			Success: true,
			Message: fmt.Sprintf("labels updated on master %s", req.NodeId),
		}, nil
	}

	return nil, fmt.Errorf(common.ErrNodeNotFound, req.NodeId)
}

// AdminCancelTask 管理端取消任务
func (s *AdminService) AdminCancelTask(ctx context.Context, req *pb.AdminCancelTaskRequest) (*pb.AdminCancelTaskResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	s.logger.InfoKV("Task cancel requested", "task_id", req.TaskId, "reason", req.Reason, "force", req.Force)

	return &pb.AdminCancelTaskResponse{
		Success: true,
		Message: fmt.Sprintf("task %s cancel requested", req.TaskId),
	}, nil
}

// AdminRetryTask 管理端重试任务
func (s *AdminService) AdminRetryTask(ctx context.Context, req *pb.AdminRetryTaskRequest) (*pb.AdminRetryTaskResponse, error) {
	if err := s.validateAuth(ctx); err != nil {
		return nil, err
	}

	s.logger.InfoKV("Task retry requested", "task_id", req.TaskId, "target_node_id", req.TargetNodeId)

	return &pb.AdminRetryTaskResponse{
		Accepted: true,
		Message:  fmt.Sprintf("task %s retry requested", req.TaskId),
	}, nil
}

// matchLabels 检查节点标签是否匹配选择器
func matchLabels(nodeLabels, selector map[string]string) bool {
	for k, v := range selector {
		if nodeVal, ok := nodeLabels[k]; !ok || nodeVal != v {
			return false
		}
	}
	return true
}
