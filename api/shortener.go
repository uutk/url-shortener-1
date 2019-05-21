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

func (s *server) GetUserInfo(ctx context.Context, in *pb.UserIdRequest) (*pb.UserObjectReply, error) {
	log.Printf("Received for GetUserInfo: %s", in.UserId)

	user, err := db.GetUserByForeignKey(in.UserId)
	if err != nil {
		log.Warnf("Can't read user by foreign key %s: %s", in.UserId, err)
		return nil, err
	}

	return &pb.UserObjectReply{Tokens: user.Tokens, CustomDomain: user.CustomDomain}, nil
}

func (s *server) SetCustomDomain(ctx context.Context, in *pb.CustomDomainRequest) (*pb.UserObjectReply, error) {
	log.Printf("Received for SetCustomDomain: foreign id: %s, custom domain: %s", in.UserId, in.CustomDomain)

	user, err := db.AddCustomDomainToUser(in.UserId, in.CustomDomain)
	if err != nil {
		log.Warnf("Can't set custom domain to user by foreign key %s: %s", in.UserId, err)
		return nil, err
	}

	return &pb.UserObjectReply{Tokens: user.Tokens, CustomDomain: user.CustomDomain}, nil
}

func (s *server) SetTokensAmount(ctx context.Context, in *pb.UpdateTokensRequest) (*pb.UserObjectReply, error) {
	log.Printf("Received for SetTokensAmount: foreign id: %s, amount: %d", in.UserId, in.Amount)

	user, err := db.AddCustomDomainToUser(in.UserId, in.Amount)
	if err != nil {
		log.Warnf("Can't set tokens amount to user by foreign key %s: %s", in.UserId, err)
		return nil, err
	}

	return &pb.UserObjectReply{Tokens: user.Tokens, CustomDomain: user.CustomDomain}, nil
}

func (s *server) GetMyUrls(ctx context.Context, in *pb.UserIdRequest) (*pb.ArrayURLsReply, error) {
	log.Printf("Received user_id for GetMyUrls: %s", in.UserId)
	var urlsReplyArray []*pb.FullURLObject

	urls, err := db.ReadURLsByUserId(in.UserId)
	if err != nil {
		log.Warnf("Can't read url by user id %s: $s", in.UserId, err)
		return nil, err
	}

	for _, item := range urls {
		urlsReplyArray = append(urlsReplyArray, &pb.FullURLObject{Url: item.Url, Hash: item.Hash, Visited: item.Visited, UserId: item.UserId.String()})
	}

	log.Printf("URL list goes to user id %s", in.UserId)
	return &pb.ArrayURLsReply{Urls: urlsReplyArray}, nil
}

func (s *server) Shorten(ctx context.Context, in *pb.URLRequest) (*pb.HashedURLReply, error) {
	log.Printf("Received for Shorten: %s", in.Url)

	if !helpers.IsUrl(in.Url) {
		return nil, errors.New("provided url is not a string")
	}

	webUrl := os.Getenv("SHORTENER_DOMAIN_WEB_URL")
	if len(webUrl) == 0 {
		webUrl = "https://kmpv.me/"
	}

	user, err := db.GetUserByForeignKey(in.UserId)
	if err != nil {
		log.Warnf("Can't read user by foreign key %s: %s", in.UserId, err)
	}

	if len(user.CustomDomain) != 0 {
		webUrl = user.CustomDomain + "/"
	}

	hash, err := db.WriteURL(in.Url, in.UserId)
	if err != nil {
		log.Warnf("Can't write url %s: %s", in.Url, err)
		return nil, err
	}

	log.Printf("Hashed. New URL is: %v", webUrl + hash)
	return &pb.HashedURLReply{Url: webUrl + hash}, nil
}

func (s *server) GetUrl(ctx context.Context, in *pb.HashedUrlRequest) (*pb.URLReply, error) {
	log.Printf("Received for GetUrl: %v", in.Hash)

	url, err := db.ReadURL(in.Hash)
	if err != nil {
		log.Warnf("Can't read url by hash %s: $s", in.Hash, err)
	}

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
