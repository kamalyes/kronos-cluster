/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 10:17:08
 * @FilePath: \go-distributed\worker\worker_test.go
 * @Description: Worker 工作节点单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-distributed/transport"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/stretchr/testify/assert"
)

const (
	testMasterAddr = "localhost:9000"
	testRedisAddr  = "localhost:6379"
	testRegion     = "beijing"
)

func TestNewWorkerNilConfig(t *testing.T) {
	log := logger.NewEmptyLogger()

	w, err := NewWorker(nil, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func TestNewWorkerNilInitInfo(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}

	w, err := NewWorker[*common.BaseNodeInfo](config, nil, log)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func TestNewWorkerUnsupportedTransport(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: "nats",
	}

	w, err := NewWorker(config, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func TestNewWorkerGRPC(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}

	w, err := NewWorker(config, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)
	assert.NoError(t, err)
	assert.NotNil(t, w)
	assert.NotNil(t, w.GetInfo())
	assert.NotNil(t, w.GetMonitor())
	assert.False(t, w.IsRunning())
}

func TestNewWorkerRedis(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}

	w, err := NewWorker(config, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)
	assert.NoError(t, err)
	assert.NotNil(t, w)
}

func TestNewWorkerPopulatesBaseNode(t *testing.T) {
	workerID := random.UUID()
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      workerID,
		MasterAddr:    testMasterAddr,
		GRPCPort:      9001,
		Region:        testRegion,
		Labels:        map[string]string{"env": "prod"},
		TransportType: common.TransportTypeGRPC,
	}

	w, err := NewWorker(config, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)
	assert.NoError(t, err)

	info := w.GetInfo()
	assert.Equal(t, workerID, info.GetID())
	assert.Equal(t, int32(9001), info.GetGRPCPort())
	assert.Equal(t, testRegion, info.GetRegion())
	assert.Equal(t, common.NodeStateIdle, info.GetState())
	assert.NotNil(t, info.GetLabels())
}

func TestWorkerStopWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}

	w, _ := NewWorker(config, func() *common.BaseNodeInfo {
		return &common.BaseNodeInfo{}
	}, log)

	err := w.Stop()
	assert.Error(t, err)
}

type mockWorkerTransport struct {
	registerCalls int32
}

func (m *mockWorkerTransport) Connect(ctx context.Context) error {
	return nil
}

func (m *mockWorkerTransport) Close() error {
	return nil
}

func (m *mockWorkerTransport) Register(ctx context.Context, nodeInfo common.NodeInfo, extension []byte) (*transport.RegistrationResult, error) {
	atomic.AddInt32(&m.registerCalls, 1)
	return &transport.RegistrationResult{Success: true, Message: "ok"}, nil
}

func (m *mockWorkerTransport) Heartbeat(ctx context.Context, nodeID string, state common.NodeState, extension []byte) (*transport.HeartbeatResult, error) {
	return &transport.HeartbeatResult{OK: false, Message: "node not found"}, nil
}

func (m *mockWorkerTransport) Unregister(ctx context.Context, nodeID string, reason string) error {
	return nil
}

func (m *mockWorkerTransport) OnTaskDispatched(handler func(task *common.TaskInfo) (*common.DispatchResult, error)) {
}

func (m *mockWorkerTransport) OnTaskCancelled(handler func(taskID string) error) {
}

func (m *mockWorkerTransport) ReportTaskStatus(ctx context.Context, update *common.TaskStatusUpdate) error {
	return nil
}

func (m *mockWorkerTransport) ConnectStream(ctx context.Context) error {
	return nil
}

func (m *mockWorkerTransport) RegisterWithSecret(ctx context.Context, nodeInfo common.NodeInfo, joinSecret string, extension []byte) (*transport.RegistrationResult, error) {
	return &transport.RegistrationResult{Success: true, Message: "ok"}, nil
}

func TestWorkerHeartbeatRejectedDoesNotDirectlyRegisterWhenReconnectInProgress(t *testing.T) {
	mockTransport := &mockWorkerTransport{}
	w := &Worker[*common.BaseNodeInfo]{
		config:       &common.WorkerConfig{MasterAddr: testMasterAddr},
		info:         &common.BaseNodeInfo{ID: random.UUID()},
		transport:    mockTransport,
		logger:       logger.NewEmptyLogger(),
		reconnecting: syncx.NewBool(true), // Simulate reconnect loop already in progress
	}

	err := w.sendHeartbeat(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockTransport.registerCalls))
}
