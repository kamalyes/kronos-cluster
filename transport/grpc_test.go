/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 11:26:33
 * @FilePath: \go-distributed\transport\grpc_test.go
 * @Description: gRPC 传输层单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"context"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProtoNodeStateToCommon(t *testing.T) {
	tests := []struct {
		name     string
		proto    pb.NodeState
		expected common.NodeState
	}{
		{"空闲", pb.NodeState_NODE_STATE_IDLE, common.NodeStateIdle},
		{"运行中", pb.NodeState_NODE_STATE_RUNNING, common.NodeStateRunning},
		{"繁忙", pb.NodeState_NODE_STATE_BUSY, common.NodeStateBusy},
		{"错误", pb.NodeState_NODE_STATE_ERROR, common.NodeStateError},
		{"离线", pb.NodeState_NODE_STATE_OFFLINE, common.NodeStateOffline},
		{"过载", pb.NodeState_NODE_STATE_OVERLOADED, common.NodeStateOverloaded},
		{"未指定", pb.NodeState_NODE_STATE_UNSPECIFIED, common.NodeStateIdle},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, common.ProtoNodeStateToCommon(tt.proto))
		})
	}
}

func TestCommonNodeStateToProto(t *testing.T) {
	tests := []struct {
		name     string
		state    common.NodeState
		expected pb.NodeState
	}{
		{"空闲", common.NodeStateIdle, pb.NodeState_NODE_STATE_IDLE},
		{"运行中", common.NodeStateRunning, pb.NodeState_NODE_STATE_RUNNING},
		{"繁忙", common.NodeStateBusy, pb.NodeState_NODE_STATE_BUSY},
		{"错误", common.NodeStateError, pb.NodeState_NODE_STATE_ERROR},
		{"离线", common.NodeStateOffline, pb.NodeState_NODE_STATE_OFFLINE},
		{"过载", common.NodeStateOverloaded, pb.NodeState_NODE_STATE_OVERLOADED},
		{"未知", common.NodeState("unknown"), pb.NodeState_NODE_STATE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, common.CommonNodeStateToProto(tt.state))
		})
	}
}

func TestGRPCMasterTransportStopWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}
	mt := NewGRPCMasterTransport(config, log)

	err := mt.Stop()
	assert.Error(t, err)
}

func TestGRPCWorkerTransportCloseWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}
	wt := NewGRPCWorkerTransport(config, log)

	err := wt.Close()
	assert.NoError(t, err)
}

func TestGRPCWorkerTransportRegisterNotConnected(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}
	wt := NewGRPCWorkerTransport(config, log)

	node := &common.BaseNodeInfo{ID: random.UUID()}
	result, err := wt.Register(context.Background(), node, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestGRPCWorkerTransportHeartbeatNotConnected(t *testing.T) {
	log := logger.NewEmptyLogger()
	nodeID := random.UUID()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}
	wt := NewGRPCWorkerTransport(config, log)

	result, err := wt.Heartbeat(context.Background(), nodeID, common.NodeStateIdle, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestGRPCWorkerTransportUnregisterNotConnected(t *testing.T) {
	log := logger.NewEmptyLogger()
	nodeID := random.UUID()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}
	wt := NewGRPCWorkerTransport(config, log)

	err := wt.Unregister(context.Background(), nodeID, "shutdown")
	assert.Error(t, err)
}

func TestGRPCMasterTransportOnRegisterBeforeStart(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}
	mt := NewGRPCMasterTransport(config, log)

	called := false
	mt.OnRegister(func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error) {
		called = true
		return &RegistrationResult{Success: true}, nil
	})

	assert.False(t, called)
}

func TestGRPCMasterTransportOnHeartbeatBeforeStart(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}
	mt := NewGRPCMasterTransport(config, log)

	mt.OnHeartbeat(func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error) {
		return &HeartbeatResult{OK: true}, nil
	})
}

func TestGRPCMasterTransportOnUnregisterBeforeStart(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}
	mt := NewGRPCMasterTransport(config, log)

	mt.OnUnregister(func(nodeID string, reason string) error {
		return nil
	})
}
