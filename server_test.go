package mockcbt

import (
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	assert "github.com/stretchr/testify/assert"
	pb "google.golang.org/genproto/googleapis/bigtable/v2"
)

func TestNewServer(t *testing.T) {
	assert := assert.New(t)

	server, err := newServer()
	assert.NotNil(server)
	assert.Nil(err)
}

func TestReset(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.Nil(err)
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{},
	)
	srv.Reset()
	assert.Nil(srv.reqItems)
	assert.Nil(srv.resps)
}

func TestAddRPC(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.Nil(err)
	srv.AddRPC(
		&pb.ReadRowsRequest{},
		[]interface{}{},
	)
	assert.NotNil(srv.resps)
	assert.NotNil(srv.reqItems[0].wantReq)
	assert.Nil(srv.reqItems[0].adjust)
}

func TestAddRPCAdjust(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.Nil(err)
	srv.AddRPCAdjust(
		&pb.ReadRowsRequest{},
		[]interface{}{},
		func(req proto.Message) {},
	)
	assert.NotNil(srv.resps)
	assert.NotNil(srv.reqItems[0].wantReq)
	assert.NotNil(srv.reqItems[0].adjust)
}

func TestPopRPC(t *testing.T) {
	assert := assert.New(t)

	_, srv, err := New()
	assert.NotNil(srv)
	assert.Nil(err)

	// test no RPCs
	panicTest := func() {
		srv.popRPC(nil)
	}
	assert.Panics(panicTest)

	// test success adjust commit
	wantReq := &pb.MutateRowRequest{
		TableName: "foo",
	}
	tn := &(wantReq.TableName)
	srv.AddRPCAdjust(
		wantReq,
		&pb.MutateRowResponse{},
		func(gotReq proto.Message) {
			*tn = gotReq.(*pb.MutateRowRequest).TableName
		},
	)
	resp, err := srv.popRPC(&pb.MutateRowRequest{
		TableName: "bar",
	})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error non matching requests
	srv.AddRPC(
		&pb.MutateRowRequest{
			TableName: "foo",
		},
		&pb.MutateRowResponse{},
	)
	_, err = srv.popRPC(&pb.MutateRowRequest{
		TableName: "bar",
	})
	assert.NotNil(err)

	// test error response
	srv.AddRPC(
		&pb.MutateRowRequest{
			TableName: "foo",
		},
		errors.New("foobar"),
	)
	_, err = srv.popRPC(&pb.MutateRowRequest{
		TableName: "foo",
	})
	assert.NotNil(err)
}
