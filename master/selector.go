/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \go-distributed\master\selector.go
 * @Description: 节点选择策略 - 支持随机、最小负载、区域感知、轮询四种策略
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"math/rand"
	"sort"

	"github.com/kamalyes/go-distributed/common"
)

// NodeSelector 节点选择器接口
type NodeSelector[T common.NodeInfo] interface {
	// Select 从节点列表中选取指定数量的节点
	Select(nodes []T, count int) []T
}

// RandomSelector 随机选择器 - 随机打乱后选取前 count 个节点
type RandomSelector[T common.NodeInfo] struct{}

// NewRandomSelector 创建随机选择器
func NewRandomSelector[T common.NodeInfo]() *RandomSelector[T] {
	return &RandomSelector[T]{}
}

// Select 随机选取节点
func (s *RandomSelector[T]) Select(nodes []T, count int) []T {
	if count >= len(nodes) {
		return nodes
	}

	selected := make([]T, len(nodes))
	copy(selected, nodes)

	rand.Shuffle(len(selected), func(i, j int) {
		selected[i], selected[j] = selected[j], selected[i]
	})

	return selected[:count]
}

// LeastLoadedSelector 最小负载选择器 - 按负载从低到高排序后选取
type LeastLoadedSelector[T common.NodeInfo] struct{}

// NewLeastLoadedSelector 创建最小负载选择器
func NewLeastLoadedSelector[T common.NodeInfo]() *LeastLoadedSelector[T] {
	return &LeastLoadedSelector[T]{}
}

// Select 选取负载最低的节点
func (s *LeastLoadedSelector[T]) Select(nodes []T, count int) []T {
	if count >= len(nodes) {
		return nodes
	}

	sorted := make([]T, len(nodes))
	copy(sorted, nodes)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].GetCurrentLoad() < sorted[j].GetCurrentLoad()
	})

	return sorted[:count]
}

// LocationAwareSelector 区域感知选择器 - 优先选择指定区域的节点
type LocationAwareSelector[T common.NodeInfo] struct {
	preferredRegions []string // 优先区域列表
}

// NewLocationAwareSelector 创建区域感知选择器
func NewLocationAwareSelector[T common.NodeInfo](regions []string) *LocationAwareSelector[T] {
	return &LocationAwareSelector[T]{preferredRegions: regions}
}

// Select 优先选取指定区域的节点，不足时从其他区域补充
func (s *LocationAwareSelector[T]) Select(nodes []T, count int) []T {
	preferred := make([]T, 0)
	others := make([]T, 0)

	for _, node := range nodes {
		if s.isPreferred(node.GetRegion()) {
			preferred = append(preferred, node)
		} else {
			others = append(others, node)
		}
	}

	result := make([]T, 0, count)
	result = append(result, preferred...)
	if len(result) < count && len(others) > 0 {
		need := count - len(result)
		if need > len(others) {
			need = len(others)
		}
		result = append(result, others[:need]...)
	}

	if len(result) > count {
		result = result[:count]
	}

	return result
}

// isPreferred 判断区域是否在优先列表中
func (s *LocationAwareSelector[T]) isPreferred(region string) bool {
	for _, r := range s.preferredRegions {
		if r == region {
			return true
		}
	}
	return false
}

// RoundRobinSelector 轮询选择器 - 按顺序循环选取节点
type RoundRobinSelector[T common.NodeInfo] struct {
	index int // 当前轮询索引
}

// NewRoundRobinSelector 创建轮询选择器
func NewRoundRobinSelector[T common.NodeInfo]() *RoundRobinSelector[T] {
	return &RoundRobinSelector[T]{index: 0}
}

// Select 轮询选取节点
func (s *RoundRobinSelector[T]) Select(nodes []T, count int) []T {
	n := len(nodes)
	if n == 0 {
		return nil
	}

	// 统一从当前 index 开始旋转，保证每次调用起始节点按轮询顺序变化
	actual := count
	if actual > n {
		actual = n
	}

	result := make([]T, actual)
	for i := 0; i < actual; i++ {
		result[i] = nodes[(s.index+i)%n]
	}

	s.index = (s.index + 1) % n
	return result
}

// NewSelector 根据策略类型创建对应的节点选择器
func NewSelector[T common.NodeInfo](strategy common.SelectStrategy, regions []string) NodeSelector[T] {
	switch strategy {
	case common.SelectStrategyRandom:
		return NewRandomSelector[T]()
	case common.SelectStrategyLeastLoaded:
		return NewLeastLoadedSelector[T]()
	case common.SelectStrategyLocationAware:
		return NewLocationAwareSelector[T](regions)
	case common.SelectStrategyRoundRobin:
		return NewRoundRobinSelector[T]()
	default:
		return NewRandomSelector[T]()
	}
}
