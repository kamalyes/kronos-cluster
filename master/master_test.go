/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 00:00:00
 * @FilePath: \kronos-cluster\master\master_test.go
 * @Description: Master 主控制器单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"github.com/kamalyes/kronos-cluster/common"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newTestConverter() func(common.NodeInfo) (*common.BaseNodeInfo, error) {
	return func(info common.NodeInfo) (*common.BaseNodeInfo, error) {
		base, _ := info.(*common.BaseNodeInfo)
		return base, nil
	}
}

func newTestStore(log logger.ILogger) TaskStore {
	return NewMemoryTaskStore(log)
}

func TestNewMasterNilConfig(t *testing.T) {
	log := logger.NewEmptyLogger()
	m, err := NewMaster(nil, newTestConverter(), nil, log)
	assert.Error(t, err)
	assert.Nil(t, m)
}

func TestNewMasterNilConverter(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      9000,
		TransportType: common.TransportTypeGRPC,
	}

	m, err := NewMaster[*common.BaseNodeInfo](config, nil, nil, log)
	assert.Error(t, err)
	assert.Nil(t, m)
}

func TestNewMasterUnsupportedTransport(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      9000,
		TransportType: "nats",
	}

	m, err := NewMaster(config, newTestConverter(), newTestStore(log), log)
	assert.Error(t, err)
	assert.Nil(t, m)
}

func TestNewMasterGRPC(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}

	m, err := NewMaster(config, newTestConverter(), newTestStore(log), log)
	assert.NoError(t, err)
	assert.NotNil(t, m)
	assert.NotNil(t, m.GetPool())
	assert.NotNil(t, m.GetHealthChecker())
	assert.NotNil(t, m.GetConfig())
	assert.False(t, m.IsRunning())
}

func TestNewMasterDefaultConfig(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}

	m, err := NewMaster(config, newTestConverter(), newTestStore(log), log)
	assert.NoError(t, err)

	assert.Equal(t, 5*time.Second, m.GetConfig().HeartbeatInterval)
	assert.Equal(t, 15*time.Second, m.GetConfig().HeartbeatTimeout)
	assert.Equal(t, 3, m.GetConfig().HeartbeatMaxFailures)
	assert.Equal(t, "kronos-cluster-secret-key", m.GetConfig().Secret)
	assert.Equal(t, 24*time.Hour, m.GetConfig().TokenExpiration)
	assert.Equal(t, "kronos-cluster-master", m.GetConfig().TokenIssuer)
}

func TestNewMasterCustomConfig(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:             0,
		TransportType:        common.TransportTypeGRPC,
		HeartbeatInterval:    10 * time.Second,
		HeartbeatTimeout:     30 * time.Second,
		HeartbeatMaxFailures: 5,
		Secret:               "my-custom-secret",
		TokenExpiration:      2 * time.Hour,
		TokenIssuer:          "custom-issuer",
	}

	m, err := NewMaster(config, newTestConverter(), newTestStore(log), log)
	assert.NoError(t, err)

	assert.Equal(t, 10*time.Second, m.GetConfig().HeartbeatInterval)
	assert.Equal(t, 30*time.Second, m.GetConfig().HeartbeatTimeout)
	assert.Equal(t, 5, m.GetConfig().HeartbeatMaxFailures)
	assert.Equal(t, "my-custom-secret", m.GetConfig().Secret)
	assert.Equal(t, 2*time.Hour, m.GetConfig().TokenExpiration)
	assert.Equal(t, "custom-issuer", m.GetConfig().TokenIssuer)
}

func TestMasterStopWhenNotRunning(t *testing.T) {
	log := logger.NewEmptyLogger()
	config := &common.MasterConfig{
		GRPCPort:      0,
		TransportType: common.TransportTypeGRPC,
	}

	m, _ := NewMaster(config, newTestConverter(), newTestStore(log), log)
	err := m.Stop()
	assert.Error(t, err)
}
