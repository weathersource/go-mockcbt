package mockcbt

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	errors "github.com/weathersource/go-errors"
	pb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
)

func TestNewBtServer(t *testing.T) {
	assert := assert.New(t)

	server, err := newBtServer()
	assert.NotNil(server)
	assert.Nil(err)
}

type Bigtable_ReadRowsServer struct {
	grpc.ServerStream
	resp *pb.ReadRowsResponse
}

func (s *Bigtable_ReadRowsServer) Send(resp *pb.ReadRowsResponse) error {
	s.resp = resp
	return nil
}

type Bigtable_ReadRowsServerError struct {
	grpc.ServerStream
	resp *pb.ReadRowsResponse
}

func (s *Bigtable_ReadRowsServerError) Send(resp *pb.ReadRowsResponse) error {
	return errors.NewInternalError("")
}

type Bigtable_SampleRowKeysServer struct {
	grpc.ServerStream
	resp *pb.SampleRowKeysResponse
}

func (s *Bigtable_SampleRowKeysServer) Send(resp *pb.SampleRowKeysResponse) error {
	s.resp = resp
	return nil
}

type Bigtable_SampleRowKeysServerError struct {
	grpc.ServerStream
	resp *pb.SampleRowKeysResponse
}

func (s *Bigtable_SampleRowKeysServerError) Send(resp *pb.SampleRowKeysResponse) error {
	return errors.NewInternalError("")
}

type Bigtable_MutateRowsServer struct {
	grpc.ServerStream
	resp *pb.MutateRowsResponse
}

func (s *Bigtable_MutateRowsServer) Send(resp *pb.MutateRowsResponse) error {
	s.resp = resp
	return nil
}

type Bigtable_MutateRowsServerError struct {
	grpc.ServerStream
	resp *pb.MutateRowsResponse
}

func (s *Bigtable_MutateRowsServerError) Send(resp *pb.MutateRowsResponse) error {
	return errors.NewInternalError("")
}

func TestReadRows(t *testing.T) {
	assert := assert.New(t)
	_, srv, err := New()
	assert.Nil(err)

	s := Bigtable_ReadRowsServer{}
	se := Bigtable_ReadRowsServerError{}

	// test valid response
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{
			&pb.ReadRowsResponse{},
		},
	)
	err = srv.ReadRows(&pb.ReadRowsRequest{}, &s)
	assert.Nil(err)
	assert.NotNil(s.resp)

	// test error send
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{
			&pb.ReadRowsResponse{},
		},
	)
	err = srv.ReadRows(&pb.ReadRowsRequest{}, &se)
	assert.NotNil(err)

	// test error response
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		errors.NewInternalError(""),
	)
	err = srv.ReadRows(&pb.ReadRowsRequest{}, &s)
	assert.NotNil(err)

	// test error response in batch
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{
			errors.NewInternalError(""),
		},
	)
	err = srv.ReadRows(&pb.ReadRowsRequest{}, &s)
	assert.NotNil(err)

	// test wrong type in batch
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{
			&pb.ReadRowsRequest{},
		},
	)
	assert.Panics(func() {
		srv.ReadRows(&pb.ReadRowsRequest{}, &s)
	})
}

func TestSampleRowKeys(t *testing.T) {
	assert := assert.New(t)
	_, srv, err := New()
	assert.Nil(err)

	s := Bigtable_SampleRowKeysServer{}
	se := Bigtable_SampleRowKeysServerError{}

	// test valid response
	srv.AddRPC(
		&pb.SampleRowKeysRequest{},
		[]interface{}{
			&pb.SampleRowKeysResponse{},
		},
	)
	err = srv.SampleRowKeys(&pb.SampleRowKeysRequest{}, &s)
	assert.Nil(err)
	assert.NotNil(s.resp)

	// test error send
	srv.AddRPC(
		&pb.SampleRowKeysRequest{},
		[]interface{}{
			&pb.SampleRowKeysResponse{},
		},
	)
	err = srv.SampleRowKeys(&pb.SampleRowKeysRequest{}, &se)
	assert.NotNil(err)

	// test error response
	srv.AddRPC(
		&pb.SampleRowKeysRequest{},
		errors.NewInternalError(""),
	)
	err = srv.SampleRowKeys(&pb.SampleRowKeysRequest{}, &s)
	assert.NotNil(err)

	// test error response in batch
	srv.AddRPC(
		&pb.SampleRowKeysRequest{},
		[]interface{}{
			errors.NewInternalError(""),
		},
	)
	err = srv.SampleRowKeys(&pb.SampleRowKeysRequest{}, &s)
	assert.NotNil(err)

	// test wrong type in batch
	srv.AddRPC(
		&pb.SampleRowKeysRequest{},
		[]interface{}{
			&pb.SampleRowKeysRequest{},
		},
	)
	assert.Panics(func() {
		srv.SampleRowKeys(&pb.SampleRowKeysRequest{}, &s)
	})
}

func TestMutateRow(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := New()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.MutateRowRequest{},
		&pb.MutateRowResponse{},
	)
	resp, err := srv.MutateRow(ctx, &pb.MutateRowRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.MutateRowRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.MutateRow(ctx, &pb.MutateRowRequest{})
	assert.NotNil(err)
}

func TestMutateRows(t *testing.T) {
	assert := assert.New(t)
	_, srv, err := New()
	assert.Nil(err)

	s := Bigtable_MutateRowsServer{}
	se := Bigtable_MutateRowsServerError{}

	// test valid response
	srv.AddRPC(
		&pb.MutateRowsRequest{},
		[]interface{}{
			&pb.MutateRowsResponse{},
		},
	)
	err = srv.MutateRows(&pb.MutateRowsRequest{}, &s)
	assert.Nil(err)
	assert.NotNil(s.resp)

	// test error send
	srv.AddRPC(
		&pb.MutateRowsRequest{},
		[]interface{}{
			&pb.MutateRowsResponse{},
		},
	)
	err = srv.MutateRows(&pb.MutateRowsRequest{}, &se)
	assert.NotNil(err)

	// test error response
	srv.AddRPC(
		&pb.MutateRowsRequest{},
		errors.NewInternalError(""),
	)
	err = srv.MutateRows(&pb.MutateRowsRequest{}, &s)
	assert.NotNil(err)

	// test error response in batch
	srv.AddRPC(
		&pb.MutateRowsRequest{},
		[]interface{}{
			errors.NewInternalError(""),
		},
	)
	err = srv.MutateRows(&pb.MutateRowsRequest{}, &s)
	assert.NotNil(err)

	// test wrong type in batch
	srv.AddRPC(
		&pb.MutateRowsRequest{},
		[]interface{}{
			&pb.MutateRowsRequest{},
		},
	)
	assert.Panics(func() {
		srv.MutateRows(&pb.MutateRowsRequest{}, &s)
	})
}

func TestCheckAndMutateRow(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := New()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.CheckAndMutateRowRequest{},
		&pb.CheckAndMutateRowResponse{},
	)
	resp, err := srv.CheckAndMutateRow(ctx, &pb.CheckAndMutateRowRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.CheckAndMutateRowRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.CheckAndMutateRow(ctx, &pb.CheckAndMutateRowRequest{})
	assert.NotNil(err)
}

func TestReadModifyWriteRow(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := New()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.ReadModifyWriteRowRequest{},
		&pb.ReadModifyWriteRowResponse{},
	)
	resp, err := srv.ReadModifyWriteRow(ctx, &pb.ReadModifyWriteRowRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.ReadModifyWriteRowRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.ReadModifyWriteRow(ctx, &pb.ReadModifyWriteRowRequest{})
	assert.NotNil(err)
}
