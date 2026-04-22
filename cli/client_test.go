/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 18:09:18
 * @FilePath: \go-distributed\cli\client_test.go
 * @Description: CLI 客户端集成测试 - 启动真实 Master gRPC 服务端，验证端到端通信
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-distributed/master"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MasterCLISuite struct {
	suite.Suite
	master *master.Master[*common.BaseNodeInfo]
	client *Client
	port   int
	log    logger.ILogger
}

func TestMasterCLISuite(t *testing.T) {
	suite.Run(t, new(MasterCLISuite))
}

func (s *MasterCLISuite) SetupSuite() {
	s.log = logger.NewEmptyLogger()

	lis, err := net.Listen("tcp", ":0")
	require.NoError(s.T(), err)
	s.port = lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	config := &common.MasterConfig{
		GRPCPort:             s.port,
		TransportType:        common.TransportTypeGRPC,
		HeartbeatInterval:    5 * time.Second,
		HeartbeatTimeout:     15 * time.Second,
		HeartbeatMaxFailures: 3,
	}

	s.master, err = master.NewMaster(
		config,
		func(info common.NodeInfo) (*common.BaseNodeInfo, error) {
			base, _ := info.(*common.BaseNodeInfo)
			return base, nil
		},
		master.NewMemoryTaskStore(s.log),
		s.log,
	)
	require.NoError(s.T(), err)

	err = s.master.Start(context.Background())
	require.NoError(s.T(), err)

	time.Sleep(200 * time.Millisecond)

	s.client, err = NewClient(
		fmt.Sprintf("localhost:%d", s.port),
		WithLogger(s.log),
	)
	require.NoError(s.T(), err)
}

func (s *MasterCLISuite) TearDownSuite() {
	if s.client != nil {
		s.client.Close()
	}
	if s.master != nil {
		s.master.Stop()
	}
}

func (s *MasterCLISuite) TestPing() {
	err := s.client.Ping(context.Background())
	assert.NoError(s.T(), err)
}

func (s *MasterCLISuite) TestListNodesEmpty() {
	resp, err := s.client.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(s.T(), err)
	assert.Len(s.T(), resp.Nodes, 1)
	assert.Equal(s.T(), int32(1), resp.TotalCount)
	assert.Equal(s.T(), pb.NodeRole_NODE_ROLE_MASTER, resp.Nodes[0].Role)
}

func (s *MasterCLISuite) TestGetClusterStatsEmpty() {
	resp, err := s.client.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{})
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int32(0), resp.TotalNodes)
	assert.Equal(s.T(), int32(0), resp.HealthyNodes)
}

func (s *MasterCLISuite) TestListTasksEmpty() {
	resp, err := s.client.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), resp.Tasks)
	assert.Equal(s.T(), int32(0), resp.TotalCount)
}

func (s *MasterCLISuite) TestGetNodeInfoNotFound() {
	resp, err := s.client.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: "nonexistent"})
	assert.Error(s.T(), err)
	assert.Nil(s.T(), resp)
}

func (s *MasterCLISuite) TestDrainNodeNotFound() {
	resp, err := s.client.DrainNode(context.Background(), &pb.DrainNodeRequest{NodeId: "nonexistent"})
	assert.Error(s.T(), err)
	assert.Nil(s.T(), resp)
}

func (s *MasterCLISuite) TestListNodesWithRegisteredNode() {
	pool := s.master.GetPool()
	node := &common.BaseNodeInfo{
		ID:       "integ-node-1",
		Hostname: "integ-host",
		IP:       "10.0.0.1",
		Region:   "beijing",
		Labels:   map[string]string{"env": "integration"},
	}
	err := pool.Register(node)
	require.NoError(s.T(), err)
	defer pool.Unregister("integ-node-1")

	resp, err := s.client.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(s.T(), err)
	assert.True(s.T(), resp.TotalCount >= 1)

	found := false
	for _, n := range resp.Nodes {
		if n.NodeInfo.NodeId == "integ-node-1" {
			found = true
			assert.Equal(s.T(), "integ-host", n.NodeInfo.Hostname)
			assert.Equal(s.T(), "10.0.0.1", n.NodeInfo.Ip)
			break
		}
	}
	assert.True(s.T(), found, "registered node should appear in ListNodes")
}

func (s *MasterCLISuite) TestGetNodeInfoWithRegisteredNode() {
	pool := s.master.GetPool()
	node := &common.BaseNodeInfo{
		ID:       "integ-node-2",
		Hostname: "integ-host-2",
		IP:       "10.0.0.2",
		Region:   "shanghai",
		Labels:   map[string]string{"env": "integration"},
	}
	err := pool.Register(node)
	require.NoError(s.T(), err)
	defer pool.Unregister("integ-node-2")

	resp, err := s.client.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: "integ-node-2"})
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), resp.Node)
	assert.Equal(s.T(), "integ-node-2", resp.Node.NodeInfo.NodeId)
	assert.Equal(s.T(), "integ-host-2", resp.Node.NodeInfo.Hostname)
}

func (s *MasterCLISuite) TestGetClusterStatsWithNodes() {
	pool := s.master.GetPool()

	node1 := &common.BaseNodeInfo{ID: "stats-node-1", Hostname: "h1", IP: "10.0.0.1", Region: "beijing"}
	node2 := &common.BaseNodeInfo{ID: "stats-node-2", Hostname: "h2", IP: "10.0.0.2", Region: "shanghai"}
	pool.Register(node1)
	pool.Register(node2)
	defer func() {
		pool.Unregister("stats-node-1")
		pool.Unregister("stats-node-2")
	}()

	node2.SetState(common.NodeStateOffline)

	resp, err := s.client.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{})
	assert.NoError(s.T(), err)
	assert.True(s.T(), resp.TotalNodes >= 2)
	assert.True(s.T(), resp.OfflineNodes >= 1)
}

func (s *MasterCLISuite) TestListTasksWithSubmittedTask() {
	tm := s.master.GetTaskManager()
	task, err := tm.SubmitTask(common.TaskTypeCommand, []byte("echo integration"))
	require.NoError(s.T(), err)

	resp, err := s.client.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(s.T(), err)

	found := false
	for _, t := range resp.Tasks {
		if t.TaskId == task.ID {
			found = true
			assert.Equal(s.T(), common.TaskTypeCommand, common.TaskType(t.TaskType))
			break
		}
	}
	assert.True(s.T(), found, "submitted task should appear in ListTasks")
}

func (s *MasterCLISuite) TestListTasksFilterByState() {
	tm := s.master.GetTaskManager()
	task, err := tm.SubmitTask(common.TaskTypeScript, []byte("print('hello')"))
	require.NoError(s.T(), err)

	resp, err := s.client.ListTasks(context.Background(), &pb.ListTasksRequest{
		FilterStates: []pb.TaskState{pb.TaskState_TASK_STATE_PENDING},
	})
	assert.NoError(s.T(), err)

	found := false
	for _, t := range resp.Tasks {
		if t.TaskId == task.ID {
			found = true
			break
		}
	}
	assert.True(s.T(), found, "pending task should be found by state filter")
}

func (s *MasterCLISuite) TestDrainNodeWithRegisteredNode() {
	pool := s.master.GetPool()
	node := &common.BaseNodeInfo{
		ID:       "drain-node-1",
		Hostname: "drain-host",
		IP:       "10.0.0.3",
		Region:   "beijing",
	}
	err := pool.Register(node)
	require.NoError(s.T(), err)
	node.SetState(common.NodeStateRunning)
	defer pool.Unregister("drain-node-1")

	resp, err := s.client.DrainNode(context.Background(), &pb.DrainNodeRequest{
		NodeId: "drain-node-1",
		Force:  true,
		Reason: "maintenance",
	})
	assert.NoError(s.T(), err)
	assert.True(s.T(), resp.Accepted)
	assert.Contains(s.T(), resp.Message, "drain-node-1")

	found, _ := pool.Get("drain-node-1")
	assert.Equal(s.T(), common.NodeStateDraining, found.GetState())
}

func (s *MasterCLISuite) TestGetClusterStatsWithResourceStats() {
	pool := s.master.GetPool()
	node := &common.BaseNodeInfo{
		ID:       "resource-node-1",
		Hostname: "resource-host",
		IP:       "10.0.0.4",
		Region:   "beijing",
	}
	pool.Register(node)
	node.SetResourceUsage(&common.ResourceUsage{CPUPercent: 75, MemoryPercent: 50})
	defer pool.Unregister("resource-node-1")

	resp, err := s.client.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{
		IncludeResourceStats: true,
	})
	assert.NoError(s.T(), err)
	assert.True(s.T(), resp.AvgCpuUsage > 0)
	assert.True(s.T(), resp.AvgMemoryUsage > 0)
}

func (s *MasterCLISuite) TestListNodesFilterByRegion() {
	pool := s.master.GetPool()
	node := &common.BaseNodeInfo{
		ID:       "region-node-1",
		Hostname: "region-host",
		IP:       "10.0.0.5",
		Region:   "guangzhou",
	}
	pool.Register(node)
	defer pool.Unregister("region-node-1")

	resp, err := s.client.ListNodes(context.Background(), &pb.ListNodesRequest{
		RegionFilter:   "guangzhou",
		IncludeOffline: true,
	})
	assert.NoError(s.T(), err)
	assert.True(s.T(), resp.TotalCount >= 1)

	found := false
	for _, n := range resp.Nodes {
		if n.NodeInfo.NodeId == "region-node-1" {
			found = true
			break
		}
	}
	assert.True(s.T(), found, "node should be found by region filter")
}

func (s *MasterCLISuite) TestClientConnectionState() {
	assert.Equal(s.T(), common.ConnectionStateReady, s.client.GetState())
	assert.True(s.T(), s.client.IsReady())
}

func (s *MasterCLISuite) TestClientWaitReady() {
	err := s.client.WaitReady(context.Background(), 2*time.Second)
	assert.NoError(s.T(), err)
}

func (s *MasterCLISuite) TestClientStartStop() {
	c, err := NewClient(
		fmt.Sprintf("localhost:%d", s.port),
		WithLogger(s.log),
		WithHealthCheckInterval(5*time.Second),
	)
	require.NoError(s.T(), err)
	defer c.Close()

	err = c.Start(context.Background())
	assert.NoError(s.T(), err)
	assert.True(s.T(), c.running.Load())

	err = c.Stop()
	assert.NoError(s.T(), err)
	assert.False(s.T(), c.running.Load())
}

func (s *MasterCLISuite) TestClientReconnect() {
	c, err := NewClient(
		fmt.Sprintf("localhost:%d", s.port),
		WithLogger(s.log),
		WithReconnectPolicy(&common.ReconnectPolicy{
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     1 * time.Second,
			MaxRetries:      3,
		}),
	)
	require.NoError(s.T(), err)
	defer c.Close()

	err = c.Ping(context.Background())
	assert.NoError(s.T(), err)

	assert.Equal(s.T(), common.ConnectionStateReady, c.GetState())
}

func TestClientStartAlreadyRunning(t *testing.T) {
	c, err := NewClient("localhost:0", WithLogger(nil))
	if err != nil {
		t.Skip("cannot connect to gRPC server")
	}
	defer c.Close()

	c.Start(context.Background())
	defer c.Stop()

	err = c.Start(context.Background())
	assert.Error(t, err)
}

func TestClientStopNotRunning(t *testing.T) {
	c, err := NewClient("localhost:0", WithLogger(nil))
	if err != nil {
		t.Skip("cannot connect to gRPC server")
	}
	defer c.Close()

	err = c.Stop()
	assert.Error(t, err)
}

func TestClientPingNoConnection(t *testing.T) {
	c := &Client{
		state:   common.ConnectionStateDisconnected,
		running: nil,
		logger:  nil,
	}

	err := c.Ping(context.Background())
	assert.Error(t, err)
}

func TestClientWaitReadyTimeout(t *testing.T) {
	c := &Client{
		state:   common.ConnectionStateDisconnected,
		running: nil,
		logger:  nil,
	}

	err := c.WaitReady(context.Background(), 100*time.Millisecond)
	assert.Error(t, err)
}
