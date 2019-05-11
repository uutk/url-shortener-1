package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	pb "github.com/vladkampov/url-shorterer/api"
	"google.golang.org/grpc"
	"net"
	"os"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

func (s *server) Shorten(ctx context.Context, in *pb.MessageRequest) (*pb.MessageReply, error) {
	log.Printf("Received: %v", in.Msg)
	return &pb.MessageReply{Message: "Hello " + in.Msg}, nil
}

func main() {
	log.Println("We are about to go...")
	port := os.Getenv("SHORTENER_DOMAIN_PORT")

	if len(port) == 0 {
		port = ":8080"
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterShortenerServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	log.Printf("Server is started at %s", port)
}
