package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	pb "distributed-home-value-forecaster/proto"
)

type workerInfo struct {
	workerID    string
	maxTasks    int32
	activeTasks int32
	stream      pb.WorkerService_ConnectToCoordinatorServer
}

type coordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	pb.UnimplementedWorkerServiceServer

	mu      sync.Mutex
	workers map[string]*workerInfo
}

func newCoordinatorServer() *coordinatorServer {
	return &coordinatorServer{
		workers: make(map[string]*workerInfo),
	}
}

// Coordinator Service -- outline
func (s *coordinatorServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.TaskAcknowledgement, error) {
	log.Printf("[client] SubmitTask received: %s", req.GetRequestPayload())

	return &pb.TaskAcknowledgement{
		TaskId:   "task-placeholder-id",
		Accepted: true,
	}, nil
}

func (s *coordinatorServer) GetTaskStatus(ctx context.Context, req *pb.GetTaskStatusRequest) (*pb.TaskStatus, error) {
	log.Printf("[client] GetTaskSTatus for task_id: %s", req.GetTaskId())

	return &pb.TaskStatus{
		TaskId: req.GetTaskId(),
		Status: "status",
	}, nil
}

// Worker Service -- outline
func (s *coordinatorServer) ConnectToCoordinator(stream pb.WorkerService_ConnectToCoordinatorServer) error {
	var workerID string

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[worker %s] disconnected (EOF)", workerID)
			s.removeWorker(workerID, 0, nil)
			return nil
		}
		if err != nil {
			log.Printf("[worker %s] stream error: %v", workerID, err)
			s.removeWorker(workerID, 0, nil)
			return err
		}

		switch payload := msg.Payload.(type) {

		case *pb.WorkerMessage_Registration:
			workerID = payload.Registration.GetWorkerId()
			log.Printf("[worker %s] registering, max_concurrent_tasks=%d", workerID, payload.Registration.GetMaxConcurrentTasks())
			s.addWorker(workerID, payload.Registration.GetMaxConcurrentTasks(), stream)

			ack := &pb.CoordinatorMessage{
				Payload: &pb.CoordinatorMessage_WorkerAck{
					WorkerAck: &pb.WorkerAcknowledgement{
						WorkerId:   workerID,
						Registered: true,
					},
				},
			}
			if err := stream.Send(ack); err != nil {
				log.Printf("[worker %s] failed to send registration ack: %v", workerID, err)
			}

		case *pb.WorkerMessage_Heartbeat:
			hb := payload.Heartbeat
			log.Printf("[worker %s] heartbeatL cpu=%.1f%% mem=%.1f%% active_tasks=%d",
				hb.GetWorkerId(), hb.GetCpuUsage(), hb.GetMemUsage(), hb.GetActiveTasks())

		case *pb.WorkerMessage_TaskStatus:
			status := payload.TaskStatus
			log.Printf("[worker %s] task_status: task_id=%s status=%s",
				workerID, status.GetTaskId(), status.GetStatus())

		default:
			log.Printf("[worker %s] received unknown message type", workerID)
		}
	}
}

// Helper Functions
func (s *coordinatorServer) addWorker(id string, maxTasks int32, stream pb.WorkerService_ConnectToCoordinatorServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[id] = &workerInfo{
		workerID: id,
		maxTasks: maxTasks,
		stream:   stream,
	}
}

func (s *coordinatorServer) removeWorker(id string, maxTasks int32, stream pb.WorkerService_ConnectToCoordinatorServer) {
	if id == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, id)
	log.Printf("[worker %s] removed from registry", id)
}

func (s *coordinatorServer) updateActiveTasks(id string, active int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.workers[id]; ok {
		w.activeTasks = active
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	srv := newCoordinatorServer()

	pb.RegisterCoordinatorServiceServer(grpcServer, srv)
	pb.RegisterWorkerServiceServer(grpcServer, srv)

	log.Println("coordinator listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to server: %v", err)
	}
}
