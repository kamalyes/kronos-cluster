/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 13:30:30
 * @FilePath: \go-distributed\common\convert.go
 * @Description: Proto ↔ Common 类型转换函数
 *
 * 集中管理 protobuf 枚举/消息与 common 包类型之间的双向转换，
 * 避免在 transport/grpc.go、master、worker 等多处重复维护。
 *
 * 转换规则:
 *   - Proto 枚举 → Common 字符串类型（如 pb.NodeState_NODE_STATE_IDLE → NodeStateIdle）
 *   - Common 字符串类型 → Proto 枚举（如 TaskStatePending → pb.TaskState_TASK_STATE_PENDING）
 *   - Proto 消息 → Common 结构体（如 pb.TaskInfo → TaskInfo）
 *   - Common 结构体 → Proto 消息（如 TaskInfo → pb.TaskInfo）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	pb "github.com/kamalyes/go-distributed/proto"
	"time"
)

// =====================================================================
// NodeState 转换
// =====================================================================

// ProtoNodeStateToCommon 将 protobuf NodeState 枚举转换为 NodeState
func ProtoNodeStateToCommon(state pb.NodeState) NodeState {
	switch state {
	case pb.NodeState_NODE_STATE_IDLE:
		return NodeStateIdle
	case pb.NodeState_NODE_STATE_RUNNING:
		return NodeStateRunning
	case pb.NodeState_NODE_STATE_BUSY:
		return NodeStateBusy
	case pb.NodeState_NODE_STATE_ERROR:
		return NodeStateError
	case pb.NodeState_NODE_STATE_OFFLINE:
		return NodeStateOffline
	case pb.NodeState_NODE_STATE_OVERLOADED:
		return NodeStateOverloaded
	case pb.NodeState_NODE_STATE_DRAINING:
		return NodeStateDraining
	default:
		return NodeStateIdle
	}
}

// CommonNodeStateToProto 将 NodeState 转换为 protobuf NodeState 枚举
func CommonNodeStateToProto(state NodeState) pb.NodeState {
	switch state {
	case NodeStateIdle:
		return pb.NodeState_NODE_STATE_IDLE
	case NodeStateRunning:
		return pb.NodeState_NODE_STATE_RUNNING
	case NodeStateBusy:
		return pb.NodeState_NODE_STATE_BUSY
	case NodeStateError:
		return pb.NodeState_NODE_STATE_ERROR
	case NodeStateOffline:
		return pb.NodeState_NODE_STATE_OFFLINE
	case NodeStateOverloaded:
		return pb.NodeState_NODE_STATE_OVERLOADED
	case NodeStateDraining:
		return pb.NodeState_NODE_STATE_DRAINING
	default:
		return pb.NodeState_NODE_STATE_UNSPECIFIED
	}
}

// =====================================================================
// TaskState 转换
// =====================================================================

// ProtoTaskStateToCommon 将 protobuf TaskState 枚举转换为 TaskState
func ProtoTaskStateToCommon(state pb.TaskState) TaskState {
	switch state {
	case pb.TaskState_TASK_STATE_PENDING:
		return TaskStatePending
	case pb.TaskState_TASK_STATE_SCHEDULED:
		return TaskStateScheduled
	case pb.TaskState_TASK_STATE_DISPATCHED:
		return TaskStateDispatched
	case pb.TaskState_TASK_STATE_RUNNING:
		return TaskStateRunning
	case pb.TaskState_TASK_STATE_SUCCEEDED:
		return TaskStateSucceeded
	case pb.TaskState_TASK_STATE_FAILED:
		return TaskStateFailed
	case pb.TaskState_TASK_STATE_CANCELLED:
		return TaskStateCancelled
	case pb.TaskState_TASK_STATE_TIMEOUT:
		return TaskStateTimeout
	case pb.TaskState_TASK_STATE_RETRYING:
		return TaskStateRetrying
	default:
		return TaskStatePending
	}
}

// CommonTaskStateToProto 将 TaskState 转换为 protobuf TaskState 枚举
func CommonTaskStateToProto(state TaskState) pb.TaskState {
	switch state {
	case TaskStatePending:
		return pb.TaskState_TASK_STATE_PENDING
	case TaskStateScheduled:
		return pb.TaskState_TASK_STATE_SCHEDULED
	case TaskStateDispatched:
		return pb.TaskState_TASK_STATE_DISPATCHED
	case TaskStateRunning:
		return pb.TaskState_TASK_STATE_RUNNING
	case TaskStateSucceeded:
		return pb.TaskState_TASK_STATE_SUCCEEDED
	case TaskStateFailed:
		return pb.TaskState_TASK_STATE_FAILED
	case TaskStateCancelled:
		return pb.TaskState_TASK_STATE_CANCELLED
	case TaskStateTimeout:
		return pb.TaskState_TASK_STATE_TIMEOUT
	case TaskStateRetrying:
		return pb.TaskState_TASK_STATE_RETRYING
	default:
		return pb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

// =====================================================================
// ConnectionState 转换
// =====================================================================

// ProtoConnectionStateToCommon 将 protobuf ConnectionState 枚举转换为 ConnectionState
func ProtoConnectionStateToCommon(state pb.ConnectionState) ConnectionState {
	switch state {
	case pb.ConnectionState_CONNECTION_STATE_DISCONNECTED:
		return ConnectionStateDisconnected
	case pb.ConnectionState_CONNECTION_STATE_CONNECTING:
		return ConnectionStateConnecting
	case pb.ConnectionState_CONNECTION_STATE_CONNECTED:
		return ConnectionStateConnected
	case pb.ConnectionState_CONNECTION_STATE_READY:
		return ConnectionStateReady
	case pb.ConnectionState_CONNECTION_STATE_DRAINING:
		return ConnectionStateDraining
	case pb.ConnectionState_CONNECTION_STATE_RECONNECTING:
		return ConnectionStateReconnecting
	default:
		return ConnectionStateDisconnected
	}
}

// CommonConnectionStateToProto 将 ConnectionState 转换为 protobuf ConnectionState 枚举
func CommonConnectionStateToProto(state ConnectionState) pb.ConnectionState {
	switch state {
	case ConnectionStateDisconnected:
		return pb.ConnectionState_CONNECTION_STATE_DISCONNECTED
	case ConnectionStateConnecting:
		return pb.ConnectionState_CONNECTION_STATE_CONNECTING
	case ConnectionStateConnected:
		return pb.ConnectionState_CONNECTION_STATE_CONNECTED
	case ConnectionStateReady:
		return pb.ConnectionState_CONNECTION_STATE_READY
	case ConnectionStateDraining:
		return pb.ConnectionState_CONNECTION_STATE_DRAINING
	case ConnectionStateReconnecting:
		return pb.ConnectionState_CONNECTION_STATE_RECONNECTING
	default:
		return pb.ConnectionState_CONNECTION_STATE_UNSPECIFIED
	}
}

// =====================================================================
// BaseNodeInfo 转换
// =====================================================================

// ProtoBaseNodeInfoToCommon 将 protobuf BaseNodeInfo 转换为 BaseNodeInfo
func ProtoBaseNodeInfoToCommon(info *pb.BaseNodeInfo) *BaseNodeInfo {
	if info == nil {
		return &BaseNodeInfo{}
	}
	return &BaseNodeInfo{
		ID:       info.NodeId,
		Hostname: info.Hostname,
		IP:       info.Ip,
		GRPCPort: info.GrpcPort,
		CPUCores: int(info.CpuCores),
		Memory:   info.Memory,
		Version:  info.Version,
		Region:   info.Region,
		Labels:   info.Labels,
	}
}

// CommonNodeInfoToProto 将 NodeInfo 接口转换为 protobuf BaseNodeInfo
func CommonNodeInfoToProto(nodeInfo NodeInfo) *pb.BaseNodeInfo {
	if nodeInfo == nil {
		return &pb.BaseNodeInfo{}
	}
	return &pb.BaseNodeInfo{
		NodeId:   nodeInfo.GetID(),
		Hostname: nodeInfo.GetHostname(),
		Ip:       nodeInfo.GetIP(),
		GrpcPort: nodeInfo.GetGRPCPort(),
		CpuCores: int32(nodeInfo.GetCPUCores()),
		Memory:   nodeInfo.GetMemory(),
		Version:  nodeInfo.GetVersion(),
		Region:   nodeInfo.GetRegion(),
		Labels:   nodeInfo.GetLabels(),
	}
}

// =====================================================================
// TaskInfo 转换
// =====================================================================

// ProtoTaskToCommon 将 protobuf TaskInfo 转换为 TaskInfo
func ProtoTaskToCommon(task *pb.TaskInfo) *TaskInfo {
	if task == nil {
		return &TaskInfo{}
	}
	return &TaskInfo{
		ID:         task.TaskId,
		Type:       TaskType(task.TaskType),
		Payload:    task.Payload,
		Priority:   task.Priority,
		MaxRetries: task.MaxRetries,
		Timeout:    time.Duration(task.TimeoutMs) * time.Millisecond,
		Metadata:   task.Metadata,
		TargetNode: task.TargetNodeId,
		State:      ProtoTaskStateToCommon(task.State),
		RetryCount: task.RetryCount,
		CreatedAt:  time.UnixMilli(task.CreatedAt),
	}
}

// CommonTaskToProto 将 TaskInfo 转换为 protobuf TaskInfo
func CommonTaskToProto(task *TaskInfo) *pb.TaskInfo {
	if task == nil {
		return &pb.TaskInfo{}
	}
	return &pb.TaskInfo{
		TaskId:       task.ID,
		TaskType:     string(task.Type),
		Payload:      task.Payload,
		Priority:     task.Priority,
		MaxRetries:   task.MaxRetries,
		TimeoutMs:    task.Timeout.Milliseconds(),
		Metadata:     task.Metadata,
		TargetNodeId: task.TargetNode,
		State:        CommonTaskStateToProto(task.State),
		RetryCount:   task.RetryCount,
		CreatedAt:    task.CreatedAt.UnixMilli(),
	}
}

// =====================================================================
// TaskStatusUpdate 转换
// =====================================================================

// ProtoTaskStatusUpdateToCommon 将 protobuf TaskStatusUpdate 转换为 TaskStatusUpdate
func ProtoTaskStatusUpdateToCommon(update *pb.TaskStatusUpdate) *TaskStatusUpdate {
	if update == nil {
		return &TaskStatusUpdate{}
	}
	return &TaskStatusUpdate{
		TaskID:       update.TaskId,
		NodeID:       update.NodeId,
		State:        ProtoTaskStateToCommon(update.State),
		Result:       update.Result,
		ErrorMessage: update.ErrorMessage,
		Progress:     float64(update.Progress),
		Timestamp:    time.UnixMilli(update.Timestamp),
	}
}

// CommonTaskStatusUpdateToProto 将 TaskStatusUpdate 转换为 protobuf TaskStatusUpdate
func CommonTaskStatusUpdateToProto(update *TaskStatusUpdate) *pb.TaskStatusUpdate {
	if update == nil {
		return &pb.TaskStatusUpdate{}
	}
	return &pb.TaskStatusUpdate{
		TaskId:       update.TaskID,
		NodeId:       update.NodeID,
		State:        CommonTaskStateToProto(update.State),
		Result:       update.Result,
		ErrorMessage: update.ErrorMessage,
		Progress:     float32(update.Progress),
		Timestamp:    update.Timestamp.UnixMilli(),
	}
}

// =====================================================================
// NodeCapacity 转换
// =====================================================================

// ProtoNodeCapacityToCommon 将 protobuf NodeCapacity 转换为 ResourceUsage
func ProtoNodeCapacityToCommon(cap *pb.NodeCapacity) *ResourceUsage {
	if cap == nil {
		return &ResourceUsage{}
	}
	return &ResourceUsage{
		CPUPercent:    cap.CpuUsage * 100,
		MemoryPercent: cap.MemoryUsage * 100,
		MemoryTotal:   cap.AvailableMemory,
		ActiveTasks:   int(cap.RunningTasks),
		LoadAvg1m:     cap.LoadAvg_1M,
		LoadAvg5m:     cap.LoadAvg_5M,
		LoadAvg15m:    cap.LoadAvg_15M,
	}
}

// CommonResourceUsageToProto 将 ResourceUsage 转换为 protobuf NodeCapacity
func CommonResourceUsageToProto(usage *ResourceUsage) *pb.NodeCapacity {
	if usage == nil {
		return &pb.NodeCapacity{}
	}
	return &pb.NodeCapacity{
		RunningTasks:    int32(usage.ActiveTasks),
		CpuUsage:        usage.CPUPercent / 100,
		MemoryUsage:     usage.MemoryPercent / 100,
		AvailableMemory: usage.MemoryTotal - usage.MemoryUsed,
		LoadAvg_1M:      usage.LoadAvg1m,
		LoadAvg_5M:      usage.LoadAvg5m,
		LoadAvg_15M:     usage.LoadAvg15m,
	}
}

// CommonNodeToNodeDetail 将 NodeInfo 转换为 pb.NodeDetail
func CommonNodeToNodeDetail(node NodeInfo) *pb.NodeDetail {
	if node == nil {
		return &pb.NodeDetail{}
	}

	usage := node.GetResourceUsage()
	var capacity *pb.NodeCapacity
	if usage != nil {
		capacity = CommonResourceUsageToProto(usage)
	}

	return &pb.NodeDetail{
		NodeInfo:        CommonNodeInfoToProto(node),
		State:           CommonNodeStateToProto(node.GetState()),
		Capacity:        capacity,
		RegisteredAtMs:  node.GetRegisteredAt().UnixMilli(),
		LastHeartbeatMs: node.GetLastHeartbeat().UnixMilli(),
		Schedulable:     node.IsSchedulable(),
		DisableReason:   node.GetDisableReason(),
	}
}
