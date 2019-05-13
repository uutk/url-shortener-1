package api

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/db"
	pb "github.com/vladkampov/url-shortener/service"
	"google.golang.org/grpc"
	"net"
	"os"
)

type server struct{}

func (s *server) Shorten(ctx context.Context, in *pb.URLRequest) (*pb.HashedURLReply, error) {
	log.Printf("Received: %v", in.Url)


	webUrl := os.Getenv("SHORTENER_DOMAIN_WEB_URL")

	if len(webUrl) == 0 {
		webUrl = "http://kmpv.me/"
	}

	hash, _ := db.WriteURL(in.Url)

	log.Printf("Hashed. New URL is: %v", webUrl + hash)
	return &pb.HashedURLReply{Url: webUrl + hash}, nil
}

func (s *server) GetUrl(ctx context.Context, in *pb.HashedUrlRequest) (*pb.URLReply, error) {
	log.Printf("Received: %v", in.Hash)

	url := db.ReadURL(in.Hash)

	return &pb.URLReply{Url: url.Url, Visited: url.Visited}, nil
}

func Init() *grpc.Server {
	port := os.Getenv("SHORTENER_DOMAIN_PORT")

	if len(port) == 0 {
		port = ":50051"
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	log.Printf("Server is started at %s", port)
	pb.RegisterShortenerServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}


	return s
}
