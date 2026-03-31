/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \go-distributed\transport\redis_test.go
 * @Description: Redis 传输层单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"testing"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
)

func TestRedisMasterTransportStopWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	mt := NewRedisMasterTransport(config, log)

	err := mt.Stop()
	assert.Error(t, err)
}

func TestRedisWorkerTransportCloseWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.WorkerConfig{
		WorkerID:      random.UUID(),
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	wt := NewRedisWorkerTransport(config, log)

	err := wt.Close()
	assert.NoError(t, err)
}

func TestRedisMasterTransportOnRegister(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	mt := NewRedisMasterTransport(config, log)

	called := false
	mt.OnRegister(func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error) {
		called = true
		return &RegistrationResult{Success: true}, nil
	})

	assert.False(t, called)
}

func TestRedisMasterTransportOnHeartbeat(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	mt := NewRedisMasterTransport(config, log)

	mt.OnHeartbeat(func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error) {
		return &HeartbeatResult{OK: true}, nil
	})
}

func TestRedisMasterTransportOnUnregister(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		TransportType: common.TransportTypeRedis,
		RedisAddr:     testRedisAddr,
	}
	mt := NewRedisMasterTransport(config, log)

	mt.OnUnregister(func(nodeID string, reason string) error {
		return nil
	})
}

func TestRedisConstants(t *testing.T) {
	assert.Equal(t, "distributed:nodes", redisKeyNodes)
	assert.Equal(t, "distributed:node:", redisKeyNodePrefix)
	assert.Equal(t, "distributed:heartbeat:", redisKeyHeartbeat)
	assert.Equal(t, "distributed:register", redisChannelRegister)
	assert.Equal(t, "distributed:heartbeat", redisChannelHeartbeat)
	assert.Equal(t, "distributed:unregister", redisChannelUnregister)
}
