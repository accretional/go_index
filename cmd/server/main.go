package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/fred/go_index/proto/goindex"
	"github.com/fred/go_index/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := flag.Int("port", 50051, "gRPC server port")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterGoIndexServiceServer(s, server.NewGoIndexServer())
	reflection.Register(s)

	log.Printf("gRPC server listening on :%d", *port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
