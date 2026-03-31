/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 10:18:16
 * @FilePath: \go-distributed\logger\logger_test.go
 * @Description: 日志适配器单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDistributedLogger(t *testing.T) {
	log := NewDistributedLogger("master")
	assert.NotNil(t, log)
}

func TestNewDistributedLoggerDifferentPrefixes(t *testing.T) {
	masterLog := NewDistributedLogger("master")
	workerLog := NewDistributedLogger("worker")

	assert.NotNil(t, masterLog)
	assert.NotNil(t, workerLog)
}

func TestNewNoOpLogger(t *testing.T) {
	log := NewNoOpLogger()
	assert.NotNil(t, log)
}

func TestILoggerType(t *testing.T) {
	log := NewDistributedLogger("test")
	var _ ILogger = log
}
