package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	desc "github.com/DmitryKovganov/go-microservices-chat-server/pkg/chat_v1"
)

const grpcPort = 50052

type server struct {
	desc.UnimplementedChatV1Server
}

type Chat struct {
	id      int64
	userIds []int64
}

type SyncMap struct {
	chats map[int64]*Chat
	mutex sync.RWMutex
}

var state = &SyncMap{
	chats: make(map[int64]*Chat),
}

func (s *server) Create(ctx context.Context, req *desc.CreateRequest) (*desc.CreateResponse, error) {
	log.Printf("Create, req: %+v", req)

	if req.GetUserIds() == nil {
		return nil, status.Error(codes.InvalidArgument, "UserIds is required")
	}

	if len(req.GetUserIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "UserIds must be not empty")
	}

	createdChat := &Chat{
		id:      rand.Int63(),
		userIds: req.GetUserIds(),
	}

	state.mutex.Lock()
	defer state.mutex.Unlock()

	state.chats[createdChat.id] = createdChat

	return &desc.CreateResponse{
		Id: createdChat.id,
	}, nil
}

func (s *server) Delete(ctx context.Context, req *desc.DeleteRequest) (*emptypb.Empty, error) {
	log.Printf("Delete, req: %+v", req)

	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "Id is required")
	}

	state.mutex.Lock()
	defer state.mutex.Unlock()

	_, ok := state.chats[req.Id]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Chat not found")
	}

	delete(state.chats, req.Id)

	return nil, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	reflection.Register(s)
	desc.RegisterChatV1Server(s, &server{})

	log.Printf("server listening at %v", lis.Addr())

	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
