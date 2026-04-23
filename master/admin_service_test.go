/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 15:20:00
 * @FilePath: \kronos-cluster\master\admin_service_test.go
 * @Description: Master AdminService gRPC 服务端实现测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"testing"
	"time"

	"github.com/kamalyes/kronos-cluster/common"
	pb "github.com/kamalyes/kronos-cluster/proto"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
)

func newTestAdminService() (*AdminService, *NodePool[*common.BaseNodeInfo], *MasterPool, TaskStore) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)
	masterPool := NewMasterPool(log)
	store := NewMemoryTaskStore(log)
	adapter := &nodePoolAdapter[*common.BaseNodeInfo]{pool: pool}
	authManager := common.NewAuthManager(config)
	svc := NewAdminService(adapter, masterPool, store, log, authManager)
	return svc, pool, masterPool, store
}

func TestAdminServiceListNodesEmpty(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Nodes)
	assert.Equal(t, int32(0), resp.TotalCount)
}

func TestAdminServiceListNodesAll(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	pool.Register(newTestBaseNode())
	pool.Register(newTestBaseNode())

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 2)
	assert.Equal(t, int32(2), resp.TotalCount)
}

func TestAdminServiceListNodesExcludeOffline(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetState(common.NodeStateOffline)

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{
		IncludeOffline: false,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, node1.ID, resp.Nodes[0].NodeInfo.NodeId)
}

func TestAdminServiceListNodesFilterByState(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetState(common.NodeStateRunning)

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{
		FilterStates:   []pb.NodeState{pb.NodeState_NODE_STATE_RUNNING},
		IncludeOffline: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, node2.ID, resp.Nodes[0].NodeInfo.NodeId)
}

func TestAdminServiceListNodesFilterByRegion(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node1 := newTestBaseNode()
	node1.Region = "shanghai"
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{
		RegionFilter:   testRegion,
		IncludeOffline: true,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, node2.ID, resp.Nodes[0].NodeInfo.NodeId)
}

func TestAdminServiceGetNodeInfo(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	node.Hostname = "test-host"
	pool.Register(node)

	resp, err := svc.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: node.ID})
	assert.NoError(t, err)
	assert.NotNil(t, resp.Node)
	assert.Equal(t, node.ID, resp.Node.NodeInfo.NodeId)
	assert.Equal(t, "test-host", resp.Node.NodeInfo.Hostname)
}

func TestAdminServiceGetNodeInfoNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceGetClusterStats(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)
	node2.SetState(common.NodeStateOffline)

	resp, err := svc.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, int32(2), resp.TotalNodes)
	assert.Equal(t, int32(1), resp.HealthyNodes)
	assert.Equal(t, int32(1), resp.OfflineNodes)
	assert.Equal(t, int32(0), resp.DrainingNodes)
}

func TestAdminServiceGetClusterStatsWithResourceStats(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetResourceUsage(&common.ResourceUsage{CPUPercent: 50, MemoryPercent: 60})

	resp, err := svc.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{
		IncludeResourceStats: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.TotalNodes)
	assert.Equal(t, 50.0, resp.AvgCpuUsage)
	assert.Equal(t, 60.0, resp.AvgMemoryUsage)
}

func TestAdminServiceGetClusterStatsWithTaskStats(t *testing.T) {
	svc, pool, _, store := newTestAdminService()

	pool.Register(newTestBaseNode())

	task := &common.TaskInfo{
		ID:    newTestNodeID(),
		Type:  common.TaskTypeCommand,
		State: common.TaskStateRunning,
	}
	store.SaveTask(context.Background(), task)

	resp, err := svc.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{
		IncludeTaskStats: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(1), resp.TotalRunningTasks)
}

func TestAdminServiceListTasksEmpty(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Tasks)
	assert.Equal(t, int32(0), resp.TotalCount)
}

func TestAdminServiceListTasksAll(t *testing.T) {
	svc, _, _, store := newTestAdminService()

	task1 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeCommand, []byte("test"))
	task2 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeScript, []byte("test"))
	store.SaveTask(context.Background(), task1)
	store.SaveTask(context.Background(), task2)

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 2)
}

func TestAdminServiceListTasksFilterByState(t *testing.T) {
	svc, _, _, store := newTestAdminService()

	task1 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeCommand, []byte("test"))
	store.SaveTask(context.Background(), task1)

	task2 := &common.TaskInfo{
		ID:      newTestNodeID(),
		Type:    common.TaskTypeScript,
		State:   common.TaskStateRunning,
		Payload: []byte("test"),
	}
	store.SaveTask(context.Background(), task2)

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{
		FilterStates: []pb.TaskState{pb.TaskState_TASK_STATE_PENDING},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 1)
	assert.Equal(t, task1.ID, resp.Tasks[0].TaskId)
}

func TestAdminServiceListTasksFilterByType(t *testing.T) {
	svc, _, _, store := newTestAdminService()

	task1 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeCommand, []byte("test"))
	task2 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeScript, []byte("test"))
	store.SaveTask(context.Background(), task1)
	store.SaveTask(context.Background(), task2)

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{
		TaskTypeFilter: "command",
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 1)
	assert.Equal(t, task1.ID, resp.Tasks[0].TaskId)
}

func TestAdminServiceListTasksByNode(t *testing.T) {
	svc, _, _, store := newTestAdminService()

	nodeID := newTestNodeID()

	task1 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeCommand, []byte("test"))
	task1.TargetNode = nodeID
	store.SaveTask(context.Background(), task1)

	task2 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeScript, []byte("test"))
	task2.TargetNode = newTestNodeID()
	store.SaveTask(context.Background(), task2)

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{
		NodeIdFilter: nodeID,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 1)
	assert.Equal(t, task1.ID, resp.Tasks[0].TaskId)
}

func TestAdminServiceDrainNode(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateRunning)

	resp, err := svc.DrainNode(context.Background(), &pb.DrainNodeRequest{
		NodeId: node.ID,
		Force:  true,
		Reason: "maintenance",
	})
	assert.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Contains(t, resp.Message, node.ID)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateDraining, found.GetState())
}

func TestAdminServiceDrainNodeNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.DrainNode(context.Background(), &pb.DrainNodeRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceEvictNode(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateRunning)

	resp, err := svc.EvictNode(context.Background(), &pb.EvictNodeRequest{
		NodeId:          node.ID,
		Reason:          "maintenance",
		Force:           true,
		RescheduleTasks: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Contains(t, resp.Message, node.ID)

	found, _ := pool.Get(node.ID)
	assert.Equal(t, common.NodeStateOffline, found.GetState())
}

func TestAdminServiceEvictNodeNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.EvictNode(context.Background(), &pb.EvictNodeRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceDisableNode(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetSchedulable(true)

	resp, err := svc.DisableNode(context.Background(), &pb.DisableNodeRequest{
		NodeId: node.ID,
		Reason: "cordon test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Contains(t, resp.Message, "disabled")

	found, _ := pool.Get(node.ID)
	assert.False(t, found.IsSchedulable())
}

func TestAdminServiceDisableNodeAlreadyDisabled(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetSchedulable(false)

	resp, err := svc.DisableNode(context.Background(), &pb.DisableNodeRequest{
		NodeId: node.ID,
		Reason: "already cordoned",
	})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceDisableNodeNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.DisableNode(context.Background(), &pb.DisableNodeRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceEnableNode(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetSchedulable(false)

	resp, err := svc.EnableNode(context.Background(), &pb.EnableNodeRequest{
		NodeId: node.ID,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Contains(t, resp.Message, "enabled")

	found, _ := pool.Get(node.ID)
	assert.True(t, found.IsSchedulable())
}

func TestAdminServiceEnableNodeAlreadyEnabled(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetSchedulable(true)

	resp, err := svc.EnableNode(context.Background(), &pb.EnableNodeRequest{
		NodeId: node.ID,
	})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceEnableNodeNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.EnableNode(context.Background(), &pb.EnableNodeRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceGetNodeTop(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetResourceUsage(&common.ResourceUsage{
		CPUPercent:    45.5,
		MemoryPercent: 60.2,
		MemoryTotal:   16384,
		MemoryUsed:    9830,
		ActiveTasks:   5,
		LoadAvg1m:     1.2,
		LoadAvg5m:     0.8,
		LoadAvg15m:    0.5,
	})

	resp, err := svc.GetNodeTop(context.Background(), &pb.GetNodeTopRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, node.ID, resp.Nodes[0].NodeId)
	assert.Greater(t, resp.Nodes[0].CpuUsage, 0.0)
	assert.Greater(t, resp.Nodes[0].MemoryUsage, 0.0)
	assert.NotZero(t, resp.Timestamp)
}

func TestAdminServiceGetNodeTopByNodeId(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node1 := newTestBaseNode()
	pool.Register(node1)

	node2 := newTestBaseNode()
	pool.Register(node2)

	resp, err := svc.GetNodeTop(context.Background(), &pb.GetNodeTopRequest{
		NodeId: node1.ID,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, node1.ID, resp.Nodes[0].NodeId)
}

func TestAdminServiceGetNodeTopNodeNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.GetNodeTop(context.Background(), &pb.GetNodeTopRequest{
		NodeId: "nonexistent",
	})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceGetNodeLogs(t *testing.T) {
	svc, pool, _, _ := newTestAdminService()

	node := newTestBaseNode()
	pool.Register(node)
	node.SetState(common.NodeStateRunning)

	resp, err := svc.GetNodeLogs(context.Background(), &pb.GetNodeLogsRequest{
		NodeId: node.ID,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.Logs)
	assert.Equal(t, node.ID, resp.NodeId)
	assert.False(t, resp.HasMore)
}

func TestAdminServiceGetNodeLogsNotFound(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.GetNodeLogs(context.Background(), &pb.GetNodeLogsRequest{
		NodeId: "nonexistent",
	})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceAuthenticateDisabled(t *testing.T) {
	svc, _, _, _ := newTestAdminService()

	resp, err := svc.Authenticate(context.Background(), &pb.AuthRequest{
		Secret:   "any",
		ClientId: "client1",
	})
	assert.NoError(t, err)
	assert.False(t, resp.Success)
}

func TestAdminServiceAuthenticateWithAuth(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{
		EnableAuth:      true,
		Secret:          "admin-secret",
		TokenExpiration: 1 * time.Hour,
		TokenIssuer:     "test",
	}
	pool := NewNodePool(selector, log, config)
	masterPool := NewMasterPool(log)
	store := NewMemoryTaskStore(log)
	adapter := &nodePoolAdapter[*common.BaseNodeInfo]{pool: pool}
	authManager := common.NewAuthManager(config)
	svc := NewAdminService(adapter, masterPool, store, log, authManager)

	resp, err := svc.Authenticate(context.Background(), &pb.AuthRequest{
		Secret:   "admin-secret",
		ClientId: "client1",
	})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.Token)
}

func TestAdminServiceAuthenticateWrongSecret(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{
		EnableAuth:      true,
		Secret:          "admin-secret",
		TokenExpiration: 1 * time.Hour,
		TokenIssuer:     "test",
	}
	pool := NewNodePool(selector, log, config)
	masterPool := NewMasterPool(log)
	store := NewMemoryTaskStore(log)
	adapter := &nodePoolAdapter[*common.BaseNodeInfo]{pool: pool}
	authManager := common.NewAuthManager(config)
	svc := NewAdminService(adapter, masterPool, store, log, authManager)

	resp, err := svc.Authenticate(context.Background(), &pb.AuthRequest{
		Secret:   "wrong-secret",
		ClientId: "client1",
	})
	assert.NoError(t, err)
	assert.False(t, resp.Success)
}
