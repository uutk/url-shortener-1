package api

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/vladkampov/url-shortener/db"
	"github.com/vladkampov/url-shortener/helpers"
	pb "github.com/vladkampov/url-shortener/service"
	"google.golang.org/grpc"
	"net"
	"os"
)

type server struct{}

func (s *server) Shorten(ctx context.Context, in *pb.URLRequest) (*pb.HashedURLReply, error) {
	log.Printf("Received: %v", in.Url)

	if !helpers.IsUrl(in.Url) {
		return nil, errors.New("provided url is not a string")
	}

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

func Run() error {
	port := os.Getenv("SHORTENER_DOMAIN_PORT")

	if len(port) == 0 {
		port = ":50051"
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	log.Printf("Server is started at %s", port)
	pb.RegisterShortenerServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		return err
	}

	return nil
}
