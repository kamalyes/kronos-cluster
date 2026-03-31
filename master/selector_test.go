/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 09:20:03
 * @FilePath: \go-distributed\master\selector_test.go
 * @Description: 节点选择策略单元测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package master

import (
	"testing"

	"github.com/kamalyes/go-distributed/common"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/stretchr/testify/assert"
)

const (
	testRegionBeijing   = "beijing"
	testRegionShanghai  = "shanghai"
	testRegionGuangzhou = "guangzhou"
	testRegionChengdu   = "chengdu"
	testRegionBJ        = "bj"
	testRegionSH        = "sh"
	testRegionGZ        = "gz"
)

func newSelectorNode(id string, load float64, region string) *common.BaseNodeInfo {
	return &common.BaseNodeInfo{
		ID:          id,
		CurrentLoad: load,
		Region:      region,
		State:       common.NodeStateIdle,
	}
}

func newSelectorNodeID() string {
	return random.UUID()
}

func TestRandomSelectorSelect(t *testing.T) {
	selector := NewRandomSelector[*common.BaseNodeInfo]()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBJ),
		newSelectorNode(newSelectorNodeID(), 0.5, testRegionSH),
		newSelectorNode(newSelectorNodeID(), 0.7, testRegionGZ),
	}

	selected := selector.Select(nodes, 2)
	assert.Len(t, selected, 2)
}

func TestRandomSelectorSelectMoreThanAvailable(t *testing.T) {
	selector := NewRandomSelector[*common.BaseNodeInfo]()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBJ),
	}

	selected := selector.Select(nodes, 5)
	assert.Len(t, selected, 1)
}

func TestRandomSelectorSelectEmpty(t *testing.T) {
	selector := NewRandomSelector[*common.BaseNodeInfo]()

	nodes := []*common.BaseNodeInfo{}
	selected := selector.Select(nodes, 2)
	assert.Empty(t, selected)
}

func TestLeastLoadedSelectorSelect(t *testing.T) {
	selector := NewLeastLoadedSelector[*common.BaseNodeInfo]()

	node2ID := newSelectorNodeID()
	node3ID := newSelectorNodeID()
	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.7, testRegionBJ),
		newSelectorNode(node2ID, 0.3, testRegionSH),
		newSelectorNode(node3ID, 0.5, testRegionGZ),
	}

	selected := selector.Select(nodes, 2)
	assert.Len(t, selected, 2)
	assert.Equal(t, node2ID, selected[0].GetID())
	assert.Equal(t, node3ID, selected[1].GetID())
}

func TestLeastLoadedSelectorSelectMoreThanAvailable(t *testing.T) {
	selector := NewLeastLoadedSelector[*common.BaseNodeInfo]()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBJ),
	}

	selected := selector.Select(nodes, 5)
	assert.Len(t, selected, 1)
}

func TestLeastLoadedSelectorSelectEmpty(t *testing.T) {
	selector := NewLeastLoadedSelector[*common.BaseNodeInfo]()

	selected := selector.Select([]*common.BaseNodeInfo{}, 2)
	assert.Empty(t, selected)
}

func TestLocationAwareSelectorSelectPreferred(t *testing.T) {
	selector := NewLocationAwareSelector[*common.BaseNodeInfo]([]string{testRegionBeijing})

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionShanghai),
		newSelectorNode(newSelectorNodeID(), 0.5, testRegionBeijing),
		newSelectorNode(newSelectorNodeID(), 0.7, testRegionBeijing),
	}

	selected := selector.Select(nodes, 2)
	assert.Len(t, selected, 2)
	assert.Equal(t, testRegionBeijing, selected[0].GetRegion())
	assert.Equal(t, testRegionBeijing, selected[1].GetRegion())
}

func TestLocationAwareSelectorSelectMixed(t *testing.T) {
	selector := NewLocationAwareSelector[*common.BaseNodeInfo]([]string{testRegionBeijing})

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBeijing),
		newSelectorNode(newSelectorNodeID(), 0.5, testRegionShanghai),
		newSelectorNode(newSelectorNodeID(), 0.7, testRegionGuangzhou),
	}

	selected := selector.Select(nodes, 3)
	assert.Len(t, selected, 3)
	assert.Equal(t, testRegionBeijing, selected[0].GetRegion())
}

func TestLocationAwareSelectorNoPreferredNodes(t *testing.T) {
	selector := NewLocationAwareSelector[*common.BaseNodeInfo]([]string{testRegionChengdu})

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBeijing),
		newSelectorNode(newSelectorNodeID(), 0.5, testRegionShanghai),
	}

	selected := selector.Select(nodes, 2)
	assert.Len(t, selected, 2)
}

func TestLocationAwareSelectorEmptyPreferredRegions(t *testing.T) {
	selector := NewLocationAwareSelector[*common.BaseNodeInfo](nil)

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBeijing),
	}

	selected := selector.Select(nodes, 1)
	assert.Len(t, selected, 1)
}

func TestRoundRobinSelectorSelect(t *testing.T) {
	selector := NewRoundRobinSelector[*common.BaseNodeInfo]()

	node1ID := newSelectorNodeID()
	node2ID := newSelectorNodeID()
	node3ID := newSelectorNodeID()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(node1ID, 0.3, testRegionBJ),
		newSelectorNode(node2ID, 0.5, testRegionSH),
		newSelectorNode(node3ID, 0.7, testRegionGZ),
	}

	selected1 := selector.Select(nodes, 1)
	assert.Len(t, selected1, 1)
	assert.Equal(t, node1ID, selected1[0].GetID())

	selected2 := selector.Select(nodes, 1)
	assert.Len(t, selected2, 1)
	assert.Equal(t, node2ID, selected2[0].GetID())

	selected3 := selector.Select(nodes, 1)
	assert.Len(t, selected3, 1)
	assert.Equal(t, node3ID, selected3[0].GetID())

	selected4 := selector.Select(nodes, 1)
	assert.Len(t, selected4, 1)
	assert.Equal(t, node1ID, selected4[0].GetID())
}

func TestRoundRobinSelectorSelectMultiple(t *testing.T) {
	selector := NewRoundRobinSelector[*common.BaseNodeInfo]()

	node1ID := newSelectorNodeID()
	node2ID := newSelectorNodeID()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(node1ID, 0.3, testRegionBJ),
		newSelectorNode(node2ID, 0.5, testRegionSH),
		newSelectorNode(newSelectorNodeID(), 0.7, testRegionGZ),
	}

	selected := selector.Select(nodes, 2)
	assert.Len(t, selected, 2)
	assert.Equal(t, node1ID, selected[0].GetID())
	assert.Equal(t, node2ID, selected[1].GetID())
}

func TestRoundRobinSelectorSelectMoreThanAvailable(t *testing.T) {
	selector := NewRoundRobinSelector[*common.BaseNodeInfo]()

	nodes := []*common.BaseNodeInfo{
		newSelectorNode(newSelectorNodeID(), 0.3, testRegionBJ),
	}

	selected := selector.Select(nodes, 5)
	assert.Len(t, selected, 1)
}

func TestNewSelectorRandom(t *testing.T) {
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyRandom, nil)
	assert.NotNil(t, selector)
	_, ok := selector.(*RandomSelector[*common.BaseNodeInfo])
	assert.True(t, ok)
}

func TestNewSelectorLeastLoaded(t *testing.T) {
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLeastLoaded, nil)
	assert.NotNil(t, selector)
	_, ok := selector.(*LeastLoadedSelector[*common.BaseNodeInfo])
	assert.True(t, ok)
}

func TestNewSelectorLocationAware(t *testing.T) {
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyLocationAware, []string{testRegionBJ})
	assert.NotNil(t, selector)
	_, ok := selector.(*LocationAwareSelector[*common.BaseNodeInfo])
	assert.True(t, ok)
}

func TestNewSelectorRoundRobin(t *testing.T) {
	selector := NewSelector[*common.BaseNodeInfo](common.SelectStrategyRoundRobin, nil)
	assert.NotNil(t, selector)
	_, ok := selector.(*RoundRobinSelector[*common.BaseNodeInfo])
	assert.True(t, ok)
}

func TestNewSelectorDefault(t *testing.T) {
	selector := NewSelector[*common.BaseNodeInfo]("unknown", nil)
	assert.NotNil(t, selector)
	_, ok := selector.(*RandomSelector[*common.BaseNodeInfo])
	assert.True(t, ok)
}
