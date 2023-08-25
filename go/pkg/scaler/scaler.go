package scaler

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client2 "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type listNode struct {
	next     *listNode
	instance *model2.Instance
}

type scaler struct {
	config         *config.Config
	metaData       *model2.Meta
	platformClient platform_client2.Client

	mu sync.Mutex
	wg sync.WaitGroup

	instances    map[string]*model2.Instance
	idleInstance *list.List

	freeLock  *sync.Mutex
	freeCount int
	free      *listNode
	freeTail  *listNode

	usingLock  *sync.Mutex
	usingCache map[string]*listNode
}

func NewScaler(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scaler := &scaler{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		instances:      make(map[string]*model2.Instance),
		idleInstance:   list.New(),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scaler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scaler
}

func (s *scaler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	return nil, nil
}

func (s *scaler) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, "assignment is nil")
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	needDestroy := false
	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	s.mu.Lock()
	defer s.mu.Unlock()
	if instance := s.instances[instanceId]; instance != nil {
		slotId = instance.Slot.Id
		instance.LastIdleTime = time.Now()
		if needDestroy {
			log.Printf("request id %s, instance %s need be destroy", request.Assigment.RequestId, instanceId)
			return reply, nil
		}

		if instance.Busy == false {
			log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return reply, nil
		}
		instance.Busy = false
		s.idleInstance.PushFront(instance)
	} else {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *scaler) Stats() Stats {
	return Stats{
		TotalInstance:     s.freeCount,
		TotalIdleInstance: len(s.usingCache),
	}
}

func (s *scaler) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		needDestroy := make([]*listNode, 0)

		s.freeLock.Lock()
		prev := s.free
		for ptr := s.free.next; ptr != nil; ptr = ptr.next {
			if time.Since(ptr.instance.LastIdleTime) < s.config.IdleDurationBeforeGC {
				break
			}
			prev.next = ptr.next
			ptr.next = nil
			needDestroy = append(needDestroy, ptr)
			prev = ptr
		}
		s.freeLock.Unlock()

		for _, ptr := range needDestroy {
			go func(ptr *listNode) {
				reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %fs", time.Since(ptr.instance.LastIdleTime).Seconds(), s.config.IdleDurationBeforeGC.Seconds())
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				s.deleteSlot(ctx, uuid.NewString(), ptr.instance.Slot.Id, ptr.instance.Id, ptr.instance.Meta.Key, reason)
			}(ptr)
		}
	}
}

func (s *scaler) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	log.Printf("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Printf("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *scaler) pushFree(listNode *listNode) {
	s.freeLock.Lock()
	defer s.freeLock.Unlock()

	if s.freeTail == nil {
		s.free.next = listNode
		s.freeTail = listNode
	} else {
		s.freeTail.next = listNode
		s.freeTail = listNode
	}
	s.freeCount++
}

func (s *scaler) popFree() *listNode {
	s.freeLock.Lock()
	defer s.freeLock.Unlock()

	if s.freeTail == nil {
		return nil
	} else {
		result := s.free.next
		s.free.next = result.next
		result.next = nil
		s.freeCount--
		return result
	}
}
