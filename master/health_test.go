/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \go-distributed\master\health_test.go
 * @Description: 健康检查器单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"testing"
	"time"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-logger"
	"github.com/stretchr/testify/assert"
)

func TestNewHealthChecker(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)

	hc := NewHealthChecker(pool, 5*time.Second, 15*time.Second, 3, log)
	assert.NotNil(t, hc)
}

func TestHealthCheckerSetInterval(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)

	hc := NewHealthChecker(pool, 5*time.Second, 15*time.Second, 3, log)
	hc.SetInterval(10 * time.Second)
	assert.Equal(t, 10*time.Second, hc.interval)
}

func TestHealthCheckerSetTimeout(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)

	hc := NewHealthChecker(pool, 5*time.Second, 15*time.Second, 3, log)
	hc.SetTimeout(30 * time.Second)
	assert.Equal(t, 30*time.Second, hc.timeout)
}

func TestHealthCheckerSetMaxFailures(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)

	hc := NewHealthChecker(pool, 5*time.Second, 15*time.Second, 3, log)
	hc.SetMaxFailures(5)
	assert.Equal(t, 5, hc.maxFailures)
}

func TestHealthCheckerStopWithoutStart(t *testing.T) {
	log := logger.NewEmptyLogger()
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	config := &common.MasterConfig{}
	pool := NewNodePool(selector, log, config)

	hc := NewHealthChecker(pool, 5*time.Second, 15*time.Second, 3, log)
	hc.Stop()
}
