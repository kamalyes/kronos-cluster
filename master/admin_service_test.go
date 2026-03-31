/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 15:20:00
 * @FilePath: \go-distributed\master\admin_service_test.go
 * @Description: Master AdminService gRPC 服务端实现测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"context"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newTestAdminService() (*AdminService, *NodePool[*common.BaseNodeInfo], TaskStore) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)
	store := NewMemoryTaskStore(log)
	adapter := &nodePoolAdapter[*common.BaseNodeInfo]{pool: pool}
	svc := NewAdminService(adapter, store, log)
	return svc, pool, store
}

func TestAdminServiceListNodesEmpty(t *testing.T) {
	svc, _, _ := newTestAdminService()

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Nodes)
	assert.Equal(t, int32(0), resp.TotalCount)
}

func TestAdminServiceListNodesAll(t *testing.T) {
	svc, pool, _ := newTestAdminService()

	pool.Register(newTestBaseNode())
	pool.Register(newTestBaseNode())

	resp, err := svc.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 2)
	assert.Equal(t, int32(2), resp.TotalCount)
}

func TestAdminServiceListNodesExcludeOffline(t *testing.T) {
	svc, pool, _ := newTestAdminService()

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
	svc, pool, _ := newTestAdminService()

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
	svc, pool, _ := newTestAdminService()

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
	svc, pool, _ := newTestAdminService()

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
	svc, _, _ := newTestAdminService()

	resp, err := svc.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestAdminServiceGetClusterStats(t *testing.T) {
	svc, pool, _ := newTestAdminService()

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
	svc, pool, _ := newTestAdminService()

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
	svc, pool, store := newTestAdminService()

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
	svc, _, _ := newTestAdminService()

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Tasks)
	assert.Equal(t, int32(0), resp.TotalCount)
}

func TestAdminServiceListTasksAll(t *testing.T) {
	svc, _, store := newTestAdminService()

	task1 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeCommand, []byte("test"))
	task2 := common.NewTaskInfo(newTestNodeID(), common.TaskTypeScript, []byte("test"))
	store.SaveTask(context.Background(), task1)
	store.SaveTask(context.Background(), task2)

	resp, err := svc.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 2)
}

func TestAdminServiceListTasksFilterByState(t *testing.T) {
	svc, _, store := newTestAdminService()

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
	svc, _, store := newTestAdminService()

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
	svc, _, store := newTestAdminService()

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
	svc, pool, _ := newTestAdminService()

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
	svc, _, _ := newTestAdminService()

	resp, err := svc.DrainNode(context.Background(), &pb.DrainNodeRequest{NodeId: "nonexistent"})
	assert.Error(t, err)
	assert.Nil(t, resp)
}
