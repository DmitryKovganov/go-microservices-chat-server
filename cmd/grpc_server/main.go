package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

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

type Message struct {
	fromUserId int64
	text       string
	createdAt  time.Time
}

type Chat struct {
	id       int64
	userIds  []int64
	messages []*Message
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
		id:       rand.Int63(),
		userIds:  req.GetUserIds(),
		messages: []*Message{},
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

func (s *server) SendMessage(ctx context.Context, req *desc.SendMessageRequest) (*emptypb.Empty, error) {
	log.Printf("SendMessage, req: %+v", req)

	if req.GetChatId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "ChatId is required")
	}

	if req.GetFromUserId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "FromUserId is required")
	}

	if req.GetText() == "" {
		return nil, status.Error(codes.InvalidArgument, "Text is required")
	}

	if req.GetCreatedAt().String() == "" {
		return nil, status.Error(codes.InvalidArgument, "CreatedAt is required")
	}

	chat, ok := state.chats[req.GetChatId()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Chat not found")
	}

	isUserPresentedInChat := false
	for _, value := range chat.userIds {
		if value == req.GetFromUserId() {
			isUserPresentedInChat = true
			break
		}
	}

	if !isUserPresentedInChat {
		return nil, status.Error(codes.InvalidArgument, "FromUserId is not presented in chat user ids")
	}

	state.mutex.Lock()
	defer state.mutex.Unlock()

	chat.messages = append(chat.messages, &Message{
		fromUserId: req.GetFromUserId(),
		text:       req.GetText(),
		createdAt:  req.GetCreatedAt().AsTime(),
	})

	log.Printf("SendMessage, text: %+v", req.GetText())

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

	log.Printf("Server listening at %v", lis.Addr())

	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
