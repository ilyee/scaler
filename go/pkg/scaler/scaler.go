package scaler

import (
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

	freeLock  *sync.Mutex
	freeCount int
	free      *listNode
	freeTail  *listNode

	instances *sync.Map
}

func NewScaler(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	dummy := &listNode{
		instance: nil,
		next:     nil,
	}
	scaler := &scaler{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		freeLock:       &sync.Mutex{},
		freeCount:      0,
		free:           dummy,
		freeTail:       dummy,
		instances:      &sync.Map{},
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scaler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()
	return scaler
}

func (s *scaler) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()
	defer func() {
		log.Printf("Assign, request id: %s, instance id: %s, cost %dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	}()
	log.Printf("Assign, request id: %s", request.RequestId)

	if s.freeCount != 0 {
		node := s.popFree()
		if node != nil {
			instance := node.instance
			instance.Busy = true
			instanceId = instance.Id
			log.Printf("Assign, request id: %s, instance %s reused", request.RequestId, instance.Id)
			return &pb.AssignReply{
				Status: pb.Status_Ok,
				Assigment: &pb.Assignment{
					RequestId:  request.RequestId,
					MetaKey:    instance.Meta.Key,
					InstanceId: instanceId,
				},
				ErrorMessage: nil,
			}, nil
		}
	}

	instance, err := s.createNew(ctx, instanceId, request)
	if err != nil {
		return nil, err
	}
	instance.Busy = true
	s.add(instance)

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *scaler) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, "assignment is nil")
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()

	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}

	needDestroy := false
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}

	instance := s.get(instanceId)
	if instance == nil {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	if !instance.Busy {
		log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
		return reply, nil
	}
	instance.Busy = false

	if needDestroy {
		s.remove(instanceId)
		go func() {
			s.deleteSlot(ctx, request.Assigment.RequestId, instance.Slot.Id, instanceId, request.Assigment.MetaKey, "bad instance")
		}()
		return reply, nil
	}
	instance.Busy = false
	instance.LastIdleTime = time.Now()
	s.pushFree(&listNode{
		instance: instance,
		next:     nil,
	})

	return reply, nil
}

func (s *scaler) Stats() Stats {
	total := 0
	s.instances.Range(func(key, value interface{}) bool {
		total += 1
		return true
	})
	return Stats{
		TotalInstance:     s.freeCount,
		TotalIdleInstance: total,
	}
}

func (s *scaler) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		needDestroy := make([]*listNode, 0)

		s.freeLock.Lock()
		ptr := s.free.next
		for ptr != nil {
			if time.Since(ptr.instance.LastIdleTime) < s.config.IdleDurationBeforeGC {
				break
			}
			ptr.next = nil
			needDestroy = append(needDestroy, ptr)
			ptr = ptr.next
		}
		s.free.next = ptr
		s.freeLock.Unlock()

		for _, ptr := range needDestroy {
			go func(ptr *listNode) {
				reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %fs", time.Since(ptr.instance.LastIdleTime).Seconds(), s.config.IdleDurationBeforeGC.Seconds())
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				s.remove(ptr.instance.Slot.Id)
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

	listNode.next = nil
	s.freeTail.next = listNode
	s.freeTail = listNode
	s.freeCount++
}

func (s *scaler) popFree() *listNode {
	s.freeLock.Lock()
	defer s.freeLock.Unlock()

	if s.free.next == nil {
		return nil
	} else {
		result := s.free.next
		if result.next == nil {
			s.freeTail = s.free
		}
		s.free.next = result.next
		result.next = nil
		s.freeCount--
		return result
	}
}

func (s *scaler) get(instanceId string) *model2.Instance {
	obj, ok := s.instances.Load(instanceId)
	if !ok {
		return nil
	}
	return obj.(*model2.Instance)
}

func (s *scaler) add(instance *model2.Instance) {
	s.instances.Store(instance.Id, instance)
}

func (s *scaler) remove(id string) {
	s.instances.Delete(id)
}

func (s *scaler) createNew(ctx context.Context, id string, request *pb.AssignRequest) (*model2.Instance, error) {
	resourceConfig := model2.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.Print(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	meta := &model2.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	instance, err := s.platformClient.Init(ctx, request.RequestId, id, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		log.Print(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	return instance, nil
}
