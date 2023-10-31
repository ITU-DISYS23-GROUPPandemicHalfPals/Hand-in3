package main

import (
	"bufio"
	"chat/chat"
	"context"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type client struct {
	Lamport int64
	Name    string
	Leave   bool

	LamportMutex sync.Mutex
	chat.ChatClient
}

func Client(name string) *client {
	return &client{
		Lamport: 0,
		Name:    name,
		Leave:   false,
	}
}

func main() {
	c := Client(os.Args[1])

	ctx := context.Background()
	connectionCtx, cancel := context.WithTimeout(ctx, time.Second)

	connection, error := grpc.DialContext(connectionCtx, ":5000", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if error != nil {
		log.Fatalf("Connecting to server failed: %s", error)
	}

	c.ChatClient = chat.NewChatClient(connection)

	error = c.join(ctx)
	if error != nil {
		log.Fatalf("Joining chat failed: %s", error)
	}

	error = c.stream(ctx)
	if error != nil {
		log.Fatalf("Stream failed: %s", error)
	}

	error = c.leave(ctx)
	if error != nil {
		log.Fatalf("Leaving chat failed: %s", error)
	}

	connection.Close()
	cancel()
}

func (c *client) stream(ctx context.Context) error {
	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{"name": c.Name}))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, error := c.ChatClient.Publish(ctx)
	if error != nil {
		return error
	}
	defer client.CloseSend()

	go c.send(client)
	return c.receive(client)
}

func (c *client) send(client chat.Chat_PublishClient) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		select {
		case <-client.Context().Done():
			log.Print("Client loop stopped")
		default:
			if scanner.Scan() {
				text := scanner.Text()

				if text == "/leave" {
					c.Leave = true
				}

				if len(text) > 128 {
					log.Print("Sending message failed: Message is longer than 128 characters")
					continue
				}

				c.incrementLamport()
				error := client.Send(&chat.PublishMessage{
					Lamport: c.Lamport,
					Body:    text,
				})

				if error != nil {
					return
				}
			} else {
				return
			}
		}
	}
}

func (c *client) receive(streamClient chat.Chat_PublishClient) error {
	for {
		response, error := streamClient.Recv()

		if error != nil {
			return error
		}

		c.incrementMaxLamport(response.Lamport)

		if c.Leave {
			return nil
		}

		if response.Body != "/leave" {
			log.Printf("%s: %s (at Lamport time %d)", response.Name, response.Body, c.Lamport)
		}
	}
}

func (c *client) join(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	c.incrementLamport()
	response, error := c.ChatClient.Join(ctx, &chat.RequestMessage{
		Lamport: c.Lamport,
		Name:    c.Name,
	})

	if error != nil {
		return error
	}

	c.incrementMaxLamport(response.Lamport)

	log.Printf("%s: has joined (at Lamport time %d)", response.Name, c.Lamport)

	return nil
}

func (c *client) leave(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	c.incrementLamport()
	response, error := c.ChatClient.Leave(ctx, &chat.RequestMessage{
		Lamport: c.Lamport,
		Name:    c.Name,
	})

	status, ok := status.FromError(error)
	if ok && status.Code() == codes.Unavailable {
		return nil
	}

	c.incrementMaxLamport(response.Lamport)

	log.Printf("%s: has left (at Lamport time %d)", response.Name, c.Lamport)

	return error
}

func (c *client) incrementMaxLamport(lamport int64) {
	c.LamportMutex.Lock()
	c.Lamport = max(c.Lamport, lamport) + 1
	c.LamportMutex.Unlock()
}

func (c *client) incrementLamport() {
	c.LamportMutex.Lock()
	c.Lamport += 1
	c.LamportMutex.Unlock()
}
