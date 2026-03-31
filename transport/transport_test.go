/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 10:16:55
 * @FilePath: \go-distributed\transport\transport_test.go
 * @Description: 传输层接口与数据结构单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"testing"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
)

const (
	testMasterAddr = "localhost:9000"
	testRedisAddr  = "localhost:6379"
)

func TestRegistrationResult(t *testing.T) {
	result := &RegistrationResult{
		Success:           true,
		Message:           "registered",
		Token:             "test-token",
		HeartbeatInterval: 5,
	}

	assert.True(t, result.Success)
	assert.Equal(t, "registered", result.Message)
	assert.Equal(t, "test-token", result.Token)
	assert.Equal(t, int64(5), result.HeartbeatInterval)
}

func TestRegistrationResultFailed(t *testing.T) {
	result := &RegistrationResult{
		Success: false,
		Message: "rejected",
	}

	assert.False(t, result.Success)
	assert.Equal(t, "rejected", result.Message)
	assert.Empty(t, result.Token)
}

func TestHeartbeatResult(t *testing.T) {
	result := &HeartbeatResult{
		OK:       true,
		Message:  "OK",
		Commands: []string{"reload", "restart"},
	}

	assert.True(t, result.OK)
	assert.Equal(t, "OK", result.Message)
	assert.Len(t, result.Commands, 2)
	assert.Contains(t, result.Commands, "reload")
}

func TestHeartbeatResultFailed(t *testing.T) {
	result := &HeartbeatResult{
		OK:      false,
		Message: "node not found",
	}

	assert.False(t, result.OK)
	assert.Equal(t, "node not found", result.Message)
	assert.Nil(t, result.Commands)
}

func TestGRPCTransportFactory(t *testing.T) {
	factory := NewGRPCTransportFactory()
	assert.Equal(t, common.TransportTypeGRPC, factory.Type())
}

func TestRedisTransportFactory(t *testing.T) {
	factory := NewRedisTransportFactory()
	assert.Equal(t, common.TransportTypeRedis, factory.Type())
}

func TestGRPCTransportFactoryCreateMasterTransport(t *testing.T) {
	factory := NewGRPCTransportFactory()
	config := &common.MasterConfig{
		GRPCPort:      9000,
		TransportType: common.TransportTypeGRPC,
	}

	transport, err := factory.CreateMasterTransport(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestGRPCTransportFactoryCreateWorkerTransport(t *testing.T) {
	factory := NewGRPCTransportFactory()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}

	transport, err := factory.CreateWorkerTransport(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestRedisTransportFactoryCreateMasterTransport(t *testing.T) {
	factory := NewRedisTransportFactory()
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}

	transport, err := factory.CreateMasterTransport(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestRedisTransportFactoryCreateWorkerTransport(t *testing.T) {
	factory := NewRedisTransportFactory()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}

	transport, err := factory.CreateWorkerTransport(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestGRPCMasterTransportNotRunningByDefault(t *testing.T) {
	config := &common.MasterConfig{
		GRPCPort:      9000,
		TransportType: common.TransportTypeGRPC,
	}
	mt := NewGRPCMasterTransport(config, nil)
	assert.NotNil(t, mt)
}

func TestGRPCWorkerTransportNotRunningByDefault(t *testing.T) {
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		MasterAddr:    testMasterAddr,
		TransportType: common.TransportTypeGRPC,
	}
	wt := NewGRPCWorkerTransport(config, nil)
	assert.NotNil(t, wt)
}

func TestRedisMasterTransportNotRunningByDefault(t *testing.T) {
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	mt := NewRedisMasterTransport(config, nil)
	assert.NotNil(t, mt)
}

func TestRedisWorkerTransportNotRunningByDefault(t *testing.T) {
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	wt := NewRedisWorkerTransport(config, nil)
	assert.NotNil(t, wt)
}
