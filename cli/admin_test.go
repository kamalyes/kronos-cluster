/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 00:00:00
 * @Description: CLI AdminService API 调用测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package cli

import (
	"context"
	"testing"

	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type mockAdminServiceClient struct {
	listNodesResp       *pb.ListNodesResponse
	getNodeInfoResp     *pb.GetNodeInfoResponse
	getClusterStatsResp *pb.ClusterStatsResponse
	listTasksResp       *pb.ListTasksResponse
	drainNodeResp       *pb.DrainNodeResponse
	evictNodeResp       *pb.EvictNodeResponse
	disableNodeResp     *pb.DisableNodeResponse
	enableNodeResp      *pb.EnableNodeResponse
	getNodeTopResp      *pb.GetNodeTopResponse
	getNodeLogsResp     *pb.GetNodeLogsResponse
	err                 error
}

func (m *mockAdminServiceClient) ListNodes(ctx context.Context, in *pb.ListNodesRequest, opts ...grpc.CallOption) (*pb.ListNodesResponse, error) {
	return m.listNodesResp, m.err
}

func (m *mockAdminServiceClient) GetNodeInfo(ctx context.Context, in *pb.GetNodeInfoRequest, opts ...grpc.CallOption) (*pb.GetNodeInfoResponse, error) {
	return m.getNodeInfoResp, m.err
}

func (m *mockAdminServiceClient) GetClusterStats(ctx context.Context, in *pb.ClusterStatsRequest, opts ...grpc.CallOption) (*pb.ClusterStatsResponse, error) {
	return m.getClusterStatsResp, m.err
}

func (m *mockAdminServiceClient) ListTasks(ctx context.Context, in *pb.ListTasksRequest, opts ...grpc.CallOption) (*pb.ListTasksResponse, error) {
	return m.listTasksResp, m.err
}

func (m *mockAdminServiceClient) DrainNode(ctx context.Context, in *pb.DrainNodeRequest, opts ...grpc.CallOption) (*pb.DrainNodeResponse, error) {
	return m.drainNodeResp, m.err
}

func (m *mockAdminServiceClient) EvictNode(ctx context.Context, in *pb.EvictNodeRequest, opts ...grpc.CallOption) (*pb.EvictNodeResponse, error) {
	return m.evictNodeResp, m.err
}

func (m *mockAdminServiceClient) DisableNode(ctx context.Context, in *pb.DisableNodeRequest, opts ...grpc.CallOption) (*pb.DisableNodeResponse, error) {
	return m.disableNodeResp, m.err
}

func (m *mockAdminServiceClient) EnableNode(ctx context.Context, in *pb.EnableNodeRequest, opts ...grpc.CallOption) (*pb.EnableNodeResponse, error) {
	return m.enableNodeResp, m.err
}

func (m *mockAdminServiceClient) GetNodeTop(ctx context.Context, in *pb.GetNodeTopRequest, opts ...grpc.CallOption) (*pb.GetNodeTopResponse, error) {
	return m.getNodeTopResp, m.err
}

func (m *mockAdminServiceClient) GetNodeLogs(ctx context.Context, in *pb.GetNodeLogsRequest, opts ...grpc.CallOption) (*pb.GetNodeLogsResponse, error) {
	return m.getNodeLogsResp, m.err
}

func (m *mockAdminServiceClient) Authenticate(ctx context.Context, in *pb.AuthRequest, opts ...grpc.CallOption) (*pb.AuthResponse, error) {
	return &pb.AuthResponse{Success: true, Token: "mock-token"}, m.err
}

func (m *mockAdminServiceClient) ListMasters(ctx context.Context, in *pb.ListMastersRequest, opts ...grpc.CallOption) (*pb.ListMastersResponse, error) {
	return &pb.ListMastersResponse{}, m.err
}

func (m *mockAdminServiceClient) ListWorkers(ctx context.Context, in *pb.ListWorkersRequest, opts ...grpc.CallOption) (*pb.ListWorkersResponse, error) {
	return &pb.ListWorkersResponse{}, m.err
}

func (m *mockAdminServiceClient) AddTaint(ctx context.Context, in *pb.AddTaintRequest, opts ...grpc.CallOption) (*pb.AddTaintResponse, error) {
	return &pb.AddTaintResponse{}, m.err
}

func (m *mockAdminServiceClient) RemoveTaint(ctx context.Context, in *pb.RemoveTaintRequest, opts ...grpc.CallOption) (*pb.RemoveTaintResponse, error) {
	return &pb.RemoveTaintResponse{}, m.err
}

func newTestClientWithMock(mock pb.AdminServiceClient) *Client {
	return &Client{
		client:       mock,
		enableAuth:   false,
		controlPlane: &common.ControlPlaneConfig{},
		running:      syncx.NewBool(false),
	}
}

func TestClientListNodes(t *testing.T) {
	mock := &mockAdminServiceClient{
		listNodesResp: &pb.ListNodesResponse{
			Nodes:      []*pb.NodeDetail{{NodeInfo: &pb.BaseNodeInfo{NodeId: "node-1"}}},
			TotalCount: 1,
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, int32(1), resp.TotalCount)
}

func TestClientListNodesError(t *testing.T) {
	mock := &mockAdminServiceClient{
		err: common.NewErrorWithCode(common.CodeNodeNotFound, "node not found"),
	}
	c := newTestClientWithMock(mock)

	resp, err := c.ListNodes(context.Background(), &pb.ListNodesRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestClientGetNodeInfo(t *testing.T) {
	mock := &mockAdminServiceClient{
		getNodeInfoResp: &pb.GetNodeInfoResponse{
			Node: &pb.NodeDetail{NodeInfo: &pb.BaseNodeInfo{NodeId: "node-1", Hostname: "test-host"}},
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.GetNodeInfo(context.Background(), &pb.GetNodeInfoRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "node-1", resp.Node.NodeInfo.NodeId)
}

func TestClientGetClusterStats(t *testing.T) {
	mock := &mockAdminServiceClient{
		getClusterStatsResp: &pb.ClusterStatsResponse{
			TotalNodes:   3,
			HealthyNodes: 2,
			OfflineNodes: 1,
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.GetClusterStats(context.Background(), &pb.ClusterStatsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, int32(3), resp.TotalNodes)
	assert.Equal(t, int32(2), resp.HealthyNodes)
}

func TestClientListTasks(t *testing.T) {
	mock := &mockAdminServiceClient{
		listTasksResp: &pb.ListTasksResponse{
			Tasks:      []*pb.TaskInfo{{TaskId: "task-1"}},
			TotalCount: 1,
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.ListTasks(context.Background(), &pb.ListTasksRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Tasks, 1)
}

func TestClientDrainNode(t *testing.T) {
	mock := &mockAdminServiceClient{
		drainNodeResp: &pb.DrainNodeResponse{
			Accepted: true,
			Message:  "node node-1 is now draining",
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.DrainNode(context.Background(), &pb.DrainNodeRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.True(t, resp.Accepted)
}

func TestClientEvictNode(t *testing.T) {
	mock := &mockAdminServiceClient{
		evictNodeResp: &pb.EvictNodeResponse{
			Success: true,
			Message: "node node-1 evicted successfully",
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.EvictNode(context.Background(), &pb.EvictNodeRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestClientDisableNode(t *testing.T) {
	mock := &mockAdminServiceClient{
		disableNodeResp: &pb.DisableNodeResponse{
			Success: true,
			Message: "node node-1 disabled (cordoned)",
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.DisableNode(context.Background(), &pb.DisableNodeRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestClientEnableNode(t *testing.T) {
	mock := &mockAdminServiceClient{
		enableNodeResp: &pb.EnableNodeResponse{
			Success: true,
			Message: "node node-1 enabled (uncordoned)",
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.EnableNode(context.Background(), &pb.EnableNodeRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestClientGetNodeTop(t *testing.T) {
	mock := &mockAdminServiceClient{
		getNodeTopResp: &pb.GetNodeTopResponse{
			Nodes: []*pb.NodeTopInfo{
				{NodeId: "node-1", CpuUsage: 0.45, MemoryUsage: 0.60},
			},
			Timestamp: 1000,
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.GetNodeTop(context.Background(), &pb.GetNodeTopRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Nodes, 1)
	assert.Equal(t, "node-1", resp.Nodes[0].NodeId)
}

func TestClientGetNodeLogs(t *testing.T) {
	mock := &mockAdminServiceClient{
		getNodeLogsResp: &pb.GetNodeLogsResponse{
			Logs:    []*pb.LogEntry{{Message: "node running", Level: "INFO"}},
			NodeId:  "node-1",
			HasMore: false,
		},
	}
	c := newTestClientWithMock(mock)

	resp, err := c.GetNodeLogs(context.Background(), &pb.GetNodeLogsRequest{NodeId: "node-1"})
	assert.NoError(t, err)
	assert.NotEmpty(t, resp.Logs)
	assert.Equal(t, "node-1", resp.NodeId)
}

func TestClientWithAuth(t *testing.T) {
	c := &Client{
		authToken:    "test-token",
		enableAuth:   true,
		controlPlane: &common.ControlPlaneConfig{},
	}

	ctx := c.withAuth(context.Background())
	assert.NotNil(t, ctx)
}

func TestClientWithAuthEmpty(t *testing.T) {
	c := &Client{
		controlPlane: &common.ControlPlaneConfig{},
	}

	ctx := c.withAuth(context.Background())
	assert.NotNil(t, ctx)
}
