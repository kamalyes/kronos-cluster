/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 18:25:25
 * @FilePath: \kronos-cluster\logger\logger.go
 * @Description: 日志适配器 - 封装 go-logger 提供分布式组件日志能力
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package logger

import (
	"time"

	gologger "github.com/kamalyes/go-logger"
)

// ILogger 日志接口 - 类型别名，与 go-logger.ILogger 保持一致
type ILogger = gologger.ILogger

// NewDistributedLogger 创建分布式组件专用日志实例 
// 配置默认日志级别为 INFO，不显示调用者信息，开启彩色输出，时间格式为 RFC3339 标准
// prefix: 日志前缀，通常为组件名称（如 "master"、"worker"）
func NewDistributedLogger(prefix string) ILogger {
	return gologger.New().
		WithPrefix(prefix).
		WithLevel(gologger.INFO).
		WithShowCaller(false).
		WithColorful(true).
		WithTimeFormat(time.RFC3339)
}

// NewNoOpLogger 创建空操作日志实例 - 用于不需要日志输出的场景
func NewNoOpLogger() ILogger {
	return gologger.NewEmptyLogger()
}
