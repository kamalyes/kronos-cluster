/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \go-distributed\common\strategy.go
 * @Description: 策略与传输类型常量定义
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

// SelectStrategy 节点选择策略类型
type SelectStrategy string

const (
	SelectStrategyRandom        SelectStrategy = "random"         // 随机选择
	SelectStrategyLeastLoaded   SelectStrategy = "least_loaded"   // 最小负载优先
	SelectStrategyLocationAware SelectStrategy = "location_aware" // 区域感知优先
	SelectStrategyRoundRobin    SelectStrategy = "round_robin"    // 轮询选择
)

// SplitStrategy 任务拆分策略类型
type SplitStrategy string

const (
	SplitStrategyEqual    SplitStrategy = "equal"    // 均分策略
	SplitStrategyWeighted SplitStrategy = "weighted" // 加权策略
	SplitStrategyCustom   SplitStrategy = "custom"   // 自定义策略
)

// FlushPolicy 统计缓冲区刷写策略
type FlushPolicy string

const (
	FlushPolicyTime FlushPolicy = "time" // 定时刷写
	FlushPolicySize FlushPolicy = "size" // 按大小刷写
	FlushPolicyBoth FlushPolicy = "both" // 时间+大小双策略
)

// TransportType 传输协议类型
type TransportType string

const (
	TransportTypeGRPC  TransportType = "grpc"  // gRPC 传输协议
	TransportTypeRedis TransportType = "redis" // Redis Pub/Sub 传输协议
)
