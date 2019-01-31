package mockcbt

import (
	"context"

	"cloud.google.com/go/bigtable"
	errors "github.com/weathersource/go-errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

// New creates a new BigTable Client and MockServer
func New() (*bigtable.Client, *MockServer, error) {
	srv, err := newServer()
	if err != nil {
		return nil, nil, errors.NewUnknownError("Failed to create BigTable server.")
	}
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, errors.NewUnknownError("Failed to create BigTable connection.")
	}
	client, err := bigtable.NewClient(context.Background(), "projectID", "instanceID", option.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, errors.NewUnknownError("Failed to create BigTable client.")
	}
	return client, srv, nil
}
