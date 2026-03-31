/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-29 15:59:23
 * @Description: gRPC 传输层实现 - Master/Worker 的 gRPC 通信（含任务下发、双向流、连接生命周期）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package transport

import (
	"context"
	"fmt"
	"github.com/kamalyes/go-distributed/common"
	pb "github.com/kamalyes/go-distributed/proto"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/kamalyes/go-toolbox/pkg/retry"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"sync"
	"time"
)

// =====================================================================
// Master 端 gRPC 传输层
// =====================================================================

const (
	msgHandlerNotRegistered    = "handler not registered"
	msgWorkerConnectionStored  = "Worker connection stored"
	msgWorkerConnectionRemoved = "Worker connection removed"
)

// GRPCMasterTransport gRPC Master 端传输层实现
type GRPCMasterTransport struct {
	config              *common.MasterConfig
	grpcServer          *grpc.Server
	service             *grpcMasterService
	adminServiceHandler pb.AdminServiceServer
	logger              logger.ILogger
	running             *syncx.Bool
	cancelFunc          context.CancelFunc
	workerConns         sync.Map
}

// workerStreamConn 记录 Worker 的双向流连接
type workerStreamConn struct {
	nodeID string
	stream grpc.BidiStreamingServer[pb.WorkerReport, pb.MasterCommand]
	sendCh chan *pb.MasterCommand
}

// grpcMasterService gRPC Master 服务实现
type grpcMasterService struct {
	pb.UnimplementedMasterServiceServer
	registerHandler      func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error)
	heartbeatHandler     func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error)
	unregisterHandler    func(nodeID string, reason string) error
	taskStatusHandler    func(update *common.TaskStatusUpdate) error
	connectStreamHandler func(nodeID string, stream grpc.BidiStreamingServer[pb.WorkerReport, pb.MasterCommand]) error
	logger               logger.ILogger
	masterTransport      *GRPCMasterTransport
}

// NewGRPCMasterTransport 创建 gRPC Master 传输层
func NewGRPCMasterTransport(config *common.MasterConfig, log logger.ILogger) *GRPCMasterTransport {
	return &GRPCMasterTransport{
		config:  config,
		logger:  log,
		running: syncx.NewBool(false),
	}
}

// RegisterAdminService 注册 AdminService gRPC 服务端实现
func (t *GRPCMasterTransport) RegisterAdminService(handler pb.AdminServiceServer) {
	t.adminServiceHandler = handler
}

// ClearWorkerConns 清空 Worker 连接
func (t *GRPCMasterTransport) ClearWorkerConns() {
	t.workerConns.Range(func(key, value any) bool {
		t.workerConns.Delete(key)
		return true
	})
	t.logger.Info("Worker connections cleared")
}

// IsNodeConnected 检查节点是否有活跃的流连接
func (t *GRPCMasterTransport) IsNodeConnected(nodeID string) bool {
	_, ok := t.workerConns.Load(nodeID)
	return ok
}

// Start 启动 gRPC Master 传输层服务
func (t *GRPCMasterTransport) Start(ctx context.Context) error {
	if !t.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyRunning, "grpc master transport")
	}

	ctx, cancel := context.WithCancel(ctx)
	t.cancelFunc = cancel

	t.service = &grpcMasterService{logger: t.logger, masterTransport: t}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", t.config.GRPCPort))
	if err != nil {
		return errorx.WrapError(common.ErrFailedListen, err)
	}

	t.grpcServer = grpc.NewServer()
	pb.RegisterMasterServiceServer(t.grpcServer, t.service)

	if t.adminServiceHandler != nil {
		pb.RegisterAdminServiceServer(t.grpcServer, t.adminServiceHandler)
	}

	go func() {
		t.logger.InfoKV("gRPC master transport listening", "port", t.config.GRPCPort)
		if err := t.grpcServer.Serve(lis); err != nil {
			t.logger.ErrorKV("gRPC server error", "error", err)
		}
	}()

	return nil
}

// Stop 停止 gRPC Master 传输层服务
func (t *GRPCMasterTransport) Stop() error {
	if !t.running.CAS(true, false) {
		return fmt.Errorf(common.ErrNotRunning, "grpc master transport")
	}

	if t.cancelFunc != nil {
		t.cancelFunc()
	}

	if t.grpcServer != nil {
		t.grpcServer.GracefulStop()
	}

	t.workerConns.Range(func(key, value any) bool {
		conn := value.(*workerStreamConn)
		close(conn.sendCh)
		t.workerConns.Delete(key)
		return true
	})

	t.logger.Info("gRPC master transport stopped")
	return nil
}

// OnRegister 设置节点注册回调
func (t *GRPCMasterTransport) OnRegister(handler func(nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error)) {
	if t.service != nil {
		t.service.registerHandler = handler
	}
}

// OnHeartbeat 设置节点心跳回调
func (t *GRPCMasterTransport) OnHeartbeat(handler func(nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error)) {
	if t.service != nil {
		t.service.heartbeatHandler = handler
	}
}

// OnUnregister 设置节点注销回调
func (t *GRPCMasterTransport) OnUnregister(handler func(nodeID string, reason string) error) {
	if t.service != nil {
		t.service.unregisterHandler = handler
	}
}

// OnTaskStatusUpdate 设置任务状态更新回调
func (t *GRPCMasterTransport) OnTaskStatusUpdate(handler func(update *common.TaskStatusUpdate) error) {
	if t.service != nil {
		t.service.taskStatusHandler = handler
	}
}

// DispatchTask 通过 WorkerService Unary RPC 向 Worker 下发任务
func (t *GRPCMasterTransport) DispatchTask(ctx context.Context, nodeID string, task *common.TaskInfo) error {
	connVal, ok := t.workerConns.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrWorkerConnNotFound, nodeID)
	}

	conn := connVal.(*workerStreamConn)
	cmd := &pb.MasterCommand{
		Command: &pb.MasterCommand_DispatchTask{
			DispatchTask: &pb.DispatchTaskRequest{
				Task: common.CommonTaskToProto(task),
			},
		},
	}

	select {
	case conn.sendCh <- cmd:
		return nil
	default:
		return fmt.Errorf(common.ErrWorkerChannelFull, nodeID)
	}
}

// CancelTask 通过双向流向 Worker 发送取消任务命令
func (t *GRPCMasterTransport) CancelTask(ctx context.Context, nodeID string, taskID string) error {
	connVal, ok := t.workerConns.Load(nodeID)
	if !ok {
		return fmt.Errorf(common.ErrNodeNotFound, nodeID)
	}

	conn := connVal.(*workerStreamConn)
	cmd := &pb.MasterCommand{
		Command: &pb.MasterCommand_CancelTask{
			CancelTask: &pb.CancelTaskRequest{
				TaskId: taskID,
			},
		},
	}

	select {
	case conn.sendCh <- cmd:
		return nil
	default:
		return fmt.Errorf(common.ErrTaskDispatchFailed, taskID, nodeID)
	}
}

// RegisterNode 处理 Worker 节点注册请求
func (s *grpcMasterService) RegisterNode(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if s.registerHandler == nil {
		return &pb.RegisterResponse{Success: false, Message: msgHandlerNotRegistered}, nil
	}

	nodeInfo := common.ProtoBaseNodeInfoToCommon(req.NodeInfo)
	result, err := s.registerHandler(nodeInfo, req.Extension)
	if err != nil {
		return &pb.RegisterResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.RegisterResponse{
		Success:           result.Success,
		Message:           result.Message,
		Token:             result.Token,
		HeartbeatInterval: result.HeartbeatInterval,
	}, nil
}

// Heartbeat 处理 Worker 节点心跳请求
func (s *grpcMasterService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if s.heartbeatHandler == nil {
		return &pb.HeartbeatResponse{Ok: false, Message: msgHandlerNotRegistered}, nil
	}

	state := common.ProtoNodeStateToCommon(req.State)
	result, err := s.heartbeatHandler(req.NodeId, state, req.StatusExtension)
	if err != nil {
		return &pb.HeartbeatResponse{Ok: false, Message: err.Error()}, nil
	}

	return &pb.HeartbeatResponse{
		Ok:       result.OK,
		Message:  result.Message,
		Commands: result.Commands,
	}, nil
}

// UnregisterNode 处理 Worker 节点注销请求
func (s *grpcMasterService) UnregisterNode(ctx context.Context, req *pb.UnregisterRequest) (*pb.UnregisterResponse, error) {
	if s.unregisterHandler == nil {
		return &pb.UnregisterResponse{Success: false, Message: msgHandlerNotRegistered}, nil
	}

	err := s.unregisterHandler(req.NodeId, req.Reason)
	if err != nil {
		return &pb.UnregisterResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.UnregisterResponse{Success: true, Message: "unregistered"}, nil
}

// ReportTaskStatus 处理 Worker 任务状态回报
func (s *grpcMasterService) ReportTaskStatus(ctx context.Context, req *pb.TaskStatusUpdate) (*pb.TaskStatusUpdateResponse, error) {
	if s.taskStatusHandler == nil {
		return &pb.TaskStatusUpdateResponse{Acknowledged: false}, nil
	}

	update := common.ProtoTaskStatusUpdateToCommon(req)
	err := s.taskStatusHandler(update)
	if err != nil {
		return &pb.TaskStatusUpdateResponse{Acknowledged: false, Message: err.Error()}, nil
	}

	return &pb.TaskStatusUpdateResponse{Acknowledged: true}, nil
}

// ConnectStream 处理双向流式连接
func (s *grpcMasterService) ConnectStream(stream grpc.BidiStreamingServer[pb.WorkerReport, pb.MasterCommand]) error {
	sendCh := make(chan *pb.MasterCommand, 64)
	go s.forwardCommands(stream, sendCh)
	defer close(sendCh)

	var nodeID string

	for {
		report, err := stream.Recv()
		if err == io.EOF {
			s.logger.InfoKV("Stream closed by worker", "nodeID", nodeID)
			s.masterTransport.workerConns.Delete(nodeID)
			s.logger.InfoKV(msgWorkerConnectionRemoved, "nodeID", nodeID)
			return nil
		}
		if err != nil {
			s.logger.ErrorKV("Stream error, removing worker connection", "nodeID", nodeID, "error", err)
			s.masterTransport.workerConns.Delete(nodeID)
			s.logger.InfoKV(msgWorkerConnectionRemoved, "nodeID", nodeID)
			return err
		}

		if shouldExit := s.handleStreamReport(report); shouldExit {
			s.logger.InfoKV("Stream exiting", "nodeID", nodeID)
			s.masterTransport.workerConns.Delete(nodeID)
			s.logger.InfoKV(msgWorkerConnectionRemoved, "nodeID", nodeID)
			return nil
		}

		// 当接收到Connect消息时，存储worker连接
		if connReport, ok := report.Report.(*pb.WorkerReport_Connect); ok {
			nodeID = connReport.Connect.NodeId
			// 将连接存储到GRPCMasterTransport的workerConns中
			s.masterTransport.workerConns.Store(nodeID, &workerStreamConn{
				nodeID: nodeID,
				stream: stream,
				sendCh: sendCh,
			})
			s.logger.InfoKV(msgWorkerConnectionStored, "nodeID", nodeID)
		}
	}
}

func (s *grpcMasterService) forwardCommands(stream grpc.BidiStreamingServer[pb.WorkerReport, pb.MasterCommand], sendCh <-chan *pb.MasterCommand) {
	for cmd := range sendCh {
		if err := stream.Send(cmd); err != nil {
			s.logger.ErrorKV("failed to send command via stream", "error", err)
			return
		}
	}
}

func (s *grpcMasterService) handleStreamReport(report *pb.WorkerReport) bool {
	switch r := report.Report.(type) {
	case *pb.WorkerReport_Connect:
		s.logger.InfoKV("worker connected via stream", "nodeID", r.Connect.NodeId)
	case *pb.WorkerReport_Heartbeat:
		s.handleStreamHeartbeat(r.Heartbeat)
	case *pb.WorkerReport_TaskStatus:
		s.handleStreamTaskStatus(r.TaskStatus)
	case *pb.WorkerReport_Disconnect:
		s.handleStreamDisconnect(r.Disconnect)
		return true
	case *pb.WorkerReport_DrainComplete:
		s.logger.InfoKV("worker drain complete", "nodeID", r.DrainComplete.NodeId)
	case *pb.WorkerReport_CapacityUpdate:
		s.logger.DebugKV("worker capacity update", "nodeID", r.CapacityUpdate.NodeId)
	}
	return false
}

func (s *grpcMasterService) handleStreamHeartbeat(hb *pb.HeartbeatRequest) {
	if s.heartbeatHandler == nil {
		return
	}
	state := common.ProtoNodeStateToCommon(hb.State)
	s.heartbeatHandler(hb.NodeId, state, hb.StatusExtension)
}

func (s *grpcMasterService) handleStreamTaskStatus(ts *pb.TaskStatusUpdate) {
	if s.taskStatusHandler == nil {
		return
	}
	s.taskStatusHandler(common.ProtoTaskStatusUpdateToCommon(ts))
}

func (s *grpcMasterService) handleStreamDisconnect(dc *pb.DisconnectRequest) {
	s.logger.InfoKV("worker disconnecting via stream", "nodeID", dc.NodeId, "reason", dc.Reason)
	if s.unregisterHandler != nil {
		s.unregisterHandler(dc.NodeId, dc.Reason)
	}
}

// =====================================================================
// Worker 端 gRPC 传输层
// =====================================================================

// GRPCWorkerTransport gRPC Worker 端传输层实现
type GRPCWorkerTransport struct {
	config              *common.WorkerConfig
	masterClient        pb.MasterServiceClient
	masterConn          *grpc.ClientConn
	workerServer        *grpc.Server
	workerService       *grpcWorkerService
	logger              logger.ILogger
	running             *syncx.Bool
	stream              grpc.BidiStreamingClient[pb.WorkerReport, pb.MasterCommand]
	streamMu            sync.Mutex
	taskDispatchHandler func(task *common.TaskInfo) (*common.DispatchResult, error)
	taskCancelHandler   func(taskID string) error
}

// grpcWorkerService gRPC Worker 服务实现 - 处理 Master 的任务下发和取消请求
type grpcWorkerService struct {
	pb.UnimplementedWorkerServiceServer
	taskDispatchHandler func(task *common.TaskInfo) (*common.DispatchResult, error)
	taskCancelHandler   func(taskID string) error
	logger              logger.ILogger
}

// NewGRPCWorkerTransport 创建 gRPC Worker 传输层
func NewGRPCWorkerTransport(config *common.WorkerConfig, log logger.ILogger) *GRPCWorkerTransport {
	return &GRPCWorkerTransport{
		config:  config,
		logger:  log,
		running: syncx.NewBool(false),
	}
}

// Connect 连接到 Master 节点并启动 Worker gRPC 服务
func (t *GRPCWorkerTransport) Connect(ctx context.Context) error {
	if !t.running.CAS(false, true) {
		return fmt.Errorf(common.ErrAlreadyConnected, "grpc worker transport")
	}

	// 连接 Master 时添加重试机制
	var conn *grpc.ClientConn

	// 使用配置中的连接重试参数，如果未设置则使用默认值，并限制范围
	connectMaxRetries := mathx.IfDefaultAndClamp(t.config.ConnectMaxRetries, 5, 1, 10)
	connectRetryInterval := mathx.IfDefaultAndClamp(t.config.ConnectRetryInterval, 1*time.Second, 500*time.Millisecond, 120*time.Second)
	backoffMultiplier := mathx.IfDefaultAndClamp(t.config.BackoffMultiplier, 1.5, 1.0, 5.0)

	err := retry.NewRetryWithCtx(ctx).
		SetAttemptCount(connectMaxRetries).
		SetInterval(connectRetryInterval).
		SetBackoffMultiplier(backoffMultiplier).
		SetJitter(true).
		SetErrCallback(func(nowAttemptCount, remainCount int, err error, funcName ...string) {
			t.logger.WarnContextKV(ctx, "Connection attempt failed, will retry",
				"attempt", nowAttemptCount,
				"remaining", remainCount,
				"max_retries", connectMaxRetries,
				"retry_interval", connectRetryInterval,
				"backoff_multiplier", backoffMultiplier,
				"addr", t.config.MasterAddr,
				"error", err)
		}).
		SetSuccessCallback(func(funcName ...string) {
			t.logger.InfoContextKV(ctx, "Connection attempt succeeded",
				"addr", t.config.MasterAddr)
		}).
		Do(func() error {
			c, e := grpc.NewClient(
				t.config.MasterAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if e != nil {
				return e
			}
			conn = c
			return nil
		})

	if err != nil {
		t.running.Store(false)
		return errorx.WrapError(common.ErrFailedConnectMaster, err)
	}

	t.masterConn = conn
	t.masterClient = pb.NewMasterServiceClient(conn)

	if t.config.GRPCPort > 0 {
		t.workerService = &grpcWorkerService{logger: t.logger}
		t.workerServer = grpc.NewServer()
		pb.RegisterWorkerServiceServer(t.workerServer, t.workerService)

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", t.config.GRPCPort))
		if err != nil {
			t.masterConn.Close()
			t.running.Store(false)
			return errorx.WrapError(common.ErrFailedListen, err)
		}

		go func() {
			t.logger.InfoContextKV(ctx, "Worker gRPC server listening", "port", t.config.GRPCPort)
			if err := t.workerServer.Serve(lis); err != nil {
				t.logger.ErrorContextKV(ctx, "Worker gRPC server error", "error", err)
			}
		}()
	}

	t.logger.InfoContextKV(ctx, "Connected to master via gRPC", "addr", t.config.MasterAddr)
	return nil
}

// Close 关闭与 Master 的连接及 Worker gRPC 服务
func (t *GRPCWorkerTransport) Close() error {
	if !t.running.CAS(true, false) {
		return nil
	}

	if t.workerServer != nil {
		t.workerServer.GracefulStop()
	}

	if t.masterConn != nil {
		t.masterConn.Close()
	}

	t.logger.Info("gRPC worker transport closed")
	return nil
}

// Register 向 Master 注册当前 Worker 节点
func (t *GRPCWorkerTransport) Register(ctx context.Context, nodeInfo common.NodeInfo, extension []byte) (*RegistrationResult, error) {
	if t.masterClient == nil {
		return nil, errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	req := &pb.RegisterRequest{
		NodeInfo:  common.CommonNodeInfoToProto(nodeInfo),
		Extension: extension,
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := t.masterClient.RegisterNode(ctx, req)
	if err != nil {
		return nil, errorx.WrapError(common.ErrRegisterFailed, err)
	}

	return &RegistrationResult{
		Success:           resp.Success,
		Message:           resp.Message,
		Token:             resp.Token,
		HeartbeatInterval: resp.HeartbeatInterval,
	}, nil
}

// Heartbeat 向 Master 发送心跳
func (t *GRPCWorkerTransport) Heartbeat(ctx context.Context, nodeID string, state common.NodeState, extension []byte) (*HeartbeatResult, error) {
	if t.masterClient == nil {
		return nil, errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	req := &pb.HeartbeatRequest{
		NodeId:          nodeID,
		Timestamp:       time.Now().Unix(),
		State:           common.CommonNodeStateToProto(state),
		StatusExtension: extension,
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := t.masterClient.Heartbeat(ctx, req)
	if err != nil {
		return nil, errorx.WrapError(common.ErrHeartbeatFailed, err)
	}

	return &HeartbeatResult{
		OK:       resp.Ok,
		Message:  resp.Message,
		Commands: resp.Commands,
	}, nil
}

// Unregister 从 Master 注销当前 Worker 节点
func (t *GRPCWorkerTransport) Unregister(ctx context.Context, nodeID string, reason string) error {
	if t.masterClient == nil {
		return fmt.Errorf(common.ErrNotConnectedToMaster)
	}

	req := &pb.UnregisterRequest{
		NodeId: nodeID,
		Reason: reason,
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := t.masterClient.UnregisterNode(ctx, req)
	if err != nil {
		return errorx.WrapError(common.ErrUnregisterFailed, err)
	}

	if !resp.Success {
		return fmt.Errorf(common.ErrUnregisterRejected, resp.Message)
	}

	return nil
}

// OnTaskDispatched 设置任务下发回调
func (t *GRPCWorkerTransport) OnTaskDispatched(handler func(task *common.TaskInfo) (*common.DispatchResult, error)) {
	t.taskDispatchHandler = handler
	if t.workerService != nil {
		t.workerService.taskDispatchHandler = handler
	}
}

// OnTaskCancelled 设置任务取消回调
func (t *GRPCWorkerTransport) OnTaskCancelled(handler func(taskID string) error) {
	t.taskCancelHandler = handler
	if t.workerService != nil {
		t.workerService.taskCancelHandler = handler
	}
}

// ReportTaskStatus 通过 Unary RPC 向 Master 上报任务状态
func (t *GRPCWorkerTransport) ReportTaskStatus(ctx context.Context, update *common.TaskStatusUpdate) error {
	if t.masterClient == nil {
		return errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	req := common.CommonTaskStatusUpdateToProto(update)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := t.masterClient.ReportTaskStatus(ctx, req)
	if err != nil {
		return fmt.Errorf(common.ErrFailedPublish, "task status update")
	}

	return nil
}

// ConnectStream 打开双向流连接到 Master
func (t *GRPCWorkerTransport) ConnectStream(ctx context.Context) error {
	if t.masterClient == nil {
		return errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	stream, err := t.masterClient.ConnectStream(ctx)
	if err != nil {
		return errorx.WrapError(common.ErrFailedConnectMaster, err)
	}

	t.streamMu.Lock()
	t.stream = stream
	t.streamMu.Unlock()

	// 发送Connect消息给Master，以便Master存储worker连接
	connectReport := &pb.WorkerReport{
		Report: &pb.WorkerReport_Connect{
			Connect: &pb.ConnectRequest{
				NodeId: t.config.WorkerID,
			},
		},
	}
	if err := t.SendStreamReport(connectReport); err != nil {
		t.logger.WarnContextKV(ctx, "Failed to send connect report", "error", err)
	}

	// 启动goroutine持续接收Master发送的命令
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				cmd, err := t.RecvStreamCommand()
				if err != nil {
					t.logger.DebugContextKV(ctx, "Stream recv error, will retry", "error", err)
					time.Sleep(1 * time.Second)
					continue
				}

				switch c := cmd.Command.(type) {
				case *pb.MasterCommand_DispatchTask:
					if t.taskDispatchHandler != nil {
						task := common.ProtoTaskToCommon(c.DispatchTask.Task)
						result, err := t.taskDispatchHandler(task)
						if err != nil {
							t.logger.ErrorContextKV(ctx, "Task dispatch failed", "task_id", task.ID, "error", err)
						}
						t.logger.InfoContextKV(ctx, "Task dispatched via stream", "task_id", task.ID, "accepted", result.Accepted)
					}
				case *pb.MasterCommand_CancelTask:
					if t.taskCancelHandler != nil {
						err := t.taskCancelHandler(c.CancelTask.TaskId)
						if err != nil {
							t.logger.ErrorContextKV(ctx, "Task cancel failed", "task_id", c.CancelTask.TaskId, "error", err)
						}
						t.logger.InfoContextKV(ctx, "Task cancelled via stream", "task_id", c.CancelTask.TaskId)
					}
				}
			}
		}
	}()

	return nil
}

// SendStreamReport 通过双向流向 Master 发送消息
func (t *GRPCWorkerTransport) SendStreamReport(report *pb.WorkerReport) error {
	t.streamMu.Lock()
	defer t.streamMu.Unlock()

	if t.stream == nil {
		return errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	return t.stream.Send(report)
}

// RecvStreamCommand 从双向流接收 Master 命令
func (t *GRPCWorkerTransport) RecvStreamCommand() (*pb.MasterCommand, error) {
	t.streamMu.Lock()
	stream := t.stream
	t.streamMu.Unlock()

	if stream == nil {
		return nil, errorx.WrapError(common.ErrNotConnectedToMaster)
	}

	return stream.Recv()
}

// DispatchTask 处理 Master 通过 Unary RPC 下发的任务
func (s *grpcWorkerService) DispatchTask(ctx context.Context, req *pb.DispatchTaskRequest) (*pb.DispatchTaskResponse, error) {
	if s.taskDispatchHandler == nil {
		return &pb.DispatchTaskResponse{Accepted: false, Message: msgHandlerNotRegistered}, nil
	}

	task := common.ProtoTaskToCommon(req.Task)
	result, err := s.taskDispatchHandler(task)
	if err != nil {
		return &pb.DispatchTaskResponse{Accepted: false, Message: err.Error()}, nil
	}

	return &pb.DispatchTaskResponse{
		Accepted: result.Accepted,
		Message:  result.Message,
	}, nil
}

// CancelTask 处理 Master 通过 Unary RPC 发送的取消任务请求
func (s *grpcWorkerService) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	if s.taskCancelHandler == nil {
		return &pb.CancelTaskResponse{Success: false, Message: msgHandlerNotRegistered}, nil
	}

	err := s.taskCancelHandler(req.TaskId)
	if err != nil {
		return &pb.CancelTaskResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.CancelTaskResponse{Success: true}, nil
}

// HealthCheck 处理 Master 健康检查请求
func (s *grpcWorkerService) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Healthy: true,
		State:   pb.NodeState_NODE_STATE_RUNNING,
	}, nil
}

// QueryTasks 处理 Master 查询任务请求
func (s *grpcWorkerService) QueryTasks(ctx context.Context, req *pb.QueryTasksRequest) (*pb.QueryTasksResponse, error) {
	return &pb.QueryTasksResponse{Tasks: nil, TotalCount: 0}, nil
}

// UpdateCapacity 处理容量更新确认
func (s *grpcWorkerService) UpdateCapacity(ctx context.Context, req *pb.UpdateCapacityRequest) (*pb.UpdateCapacityResponse, error) {
	return &pb.UpdateCapacityResponse{Acknowledged: true}, nil
}

// =====================================================================
// 传输层工厂
// =====================================================================

// GRPCTransportFactory gRPC 传输层工厂
type GRPCTransportFactory struct{}

// NewGRPCTransportFactory 创建 gRPC 传输层工厂
func NewGRPCTransportFactory() *GRPCTransportFactory {
	return &GRPCTransportFactory{}
}

// CreateMasterTransport 创建 gRPC Master 传输层
func (f *GRPCTransportFactory) CreateMasterTransport(config *common.MasterConfig, log logger.ILogger) (MasterTransport, error) {
	return NewGRPCMasterTransport(config, log), nil
}

// CreateWorkerTransport 创建 gRPC Worker 传输层
func (f *GRPCTransportFactory) CreateWorkerTransport(config *common.WorkerConfig, log logger.ILogger) (WorkerTransport, error) {
	return NewGRPCWorkerTransport(config, log), nil
}

// Type 返回传输协议类型
func (f *GRPCTransportFactory) Type() common.TransportType {
	return common.TransportTypeGRPC
}
