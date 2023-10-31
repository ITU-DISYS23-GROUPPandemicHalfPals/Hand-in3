package main

import (
	"chat/chat"
	"context"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type server struct {
	Lamport int64

	Broadcast chan *chat.BroadcastMessage
	Streams   map[string]chan *chat.BroadcastMessage

	lamportMutex sync.Mutex
	streamsMutex sync.Mutex
	chat.UnimplementedChatServer
}

func Server() *server {
	return &server{
		Lamport: 0,

		Broadcast: make(chan *chat.BroadcastMessage, 1000),
		Streams:   make(map[string]chan *chat.BroadcastMessage),
	}
}

func main() {
	s := Server()

	ctx, cancel := context.WithCancel(context.Background())

	log.Print("Starting server")
	server := grpc.NewServer()
	chat.RegisterChatServer(server, s)

	listener, error := net.Listen("tcp", ":5000")
	if error != nil {
		log.Fatalf("Starting listener failed: %s", error)
	}

	go s.broadcast()

	go func() {
		_ = server.Serve(listener)
		cancel()
	}()

	<-ctx.Done()

	close(s.Broadcast)
	server.GracefulStop()
	cancel()
}

func (s *server) broadcast() {
	for response := range s.Broadcast {
		s.streamsMutex.Lock()
		for _, stream := range s.Streams {
			select {
			case stream <- response:
			default:
				log.Print("Client stream full")
			}
		}
		s.streamsMutex.Unlock()
	}
}

func (s *server) Join(_ context.Context, request *chat.RequestMessage) (*chat.ResponseMessage, error) {
	s.incrementMaxLamport(request.Lamport)

	log.Printf("%s: has joined (at Lamport time %d)", request.Name, s.Lamport)

	s.incrementLamport()
	s.Broadcast <- &chat.BroadcastMessage{
		Lamport: s.Lamport,
		Name:    request.Name,
		Body:    "has joined",
	}

	s.incrementLamport()
	return &chat.ResponseMessage{
		Lamport: s.Lamport,
		Name:    request.Name,
	}, nil
}

func (s *server) Leave(_ context.Context, request *chat.RequestMessage) (*chat.ResponseMessage, error) {
	s.incrementMaxLamport(request.Lamport)

	log.Printf("%s: has left (at Lamport time %d)", request.Name, s.Lamport)

	s.incrementLamport()
	s.Broadcast <- &chat.BroadcastMessage{
		Lamport: s.Lamport,
		Name:    request.Name,
		Body:    "has left",
	}

	s.incrementLamport()
	return &chat.ResponseMessage{
		Lamport: s.Lamport,
		Name:    request.Name,
	}, nil
}

func (s *server) Publish(server chat.Chat_PublishServer) error {
	md, _ := metadata.FromIncomingContext(server.Context())
	name := md["name"][0]

	go s.sendBroadcasts(server, name)

	for {
		request, error := server.Recv()

		if error == io.EOF {
			break
		} else if error != nil {
			return error
		}

		s.incrementMaxLamport(request.Lamport)

		s.incrementLamport()
		s.Broadcast <- &chat.BroadcastMessage{
			Lamport: s.Lamport,
			Name:    name,
			Body:    request.Body,
		}
	}

	<-server.Context().Done()
	return server.Context().Err()
}

func (s *server) sendBroadcasts(server chat.Chat_PublishServer, name string) {
	stream := s.openStream(name)
	defer s.closeStream(name)

	for {
		select {
		case <-server.Context().Done():
			return
		case response := <-stream:
			status, ok := status.FromError(server.Send(response))
			if ok {
				switch status.Code() {
				case codes.OK:
				case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
					return
				default:
					return
				}
			}
		}
	}
}

func (s *server) openStream(name string) (stream chan *chat.BroadcastMessage) {
	stream = make(chan *chat.BroadcastMessage, 100)

	s.streamsMutex.Lock()
	s.Streams[name] = stream
	s.streamsMutex.Unlock()

	return
}

func (s *server) closeStream(name string) {
	s.streamsMutex.Lock()

	stream, ok := s.Streams[name]
	if ok {
		delete(s.Streams, name)
		close(stream)
	}

	s.streamsMutex.Unlock()
}

func (s *server) incrementMaxLamport(lamport int64) {
	s.lamportMutex.Lock()
	s.Lamport = max(s.Lamport, lamport) + 1
	s.lamportMutex.Unlock()
}

func (s *server) incrementLamport() {
	s.lamportMutex.Lock()
	s.Lamport += 1
	s.lamportMutex.Unlock()
}
