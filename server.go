package mockcbt

// A simple mock server.

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	errors "github.com/weathersource/go-errors"
	pb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// MockServer mocks the pb.BigtableServer interface
type MockServer struct {
	pb.BigtableServer
	Addr     string
	reqItems []reqItem
	resps    []interface{}
}

type reqItem struct {
	wantReq proto.Message
	adjust  func(gotReq proto.Message)
}

// Reset returns the MockServer to an empty state.
func (s *MockServer) Reset() {
	s.reqItems = nil
	s.resps = nil
}

// AddRPC adds a (request, response) pair to the server's list of expected
// interactions. The server will compare the incoming request with wantReq
// using proto.Equal. The response can be a message or an error.
//
// For the Listen RPC, resp should be a []interface{}, where each element
// is either ListenResponse or an error.
//
// Passing nil for wantReq disables the request check.
func (s *MockServer) AddRPC(wantReq proto.Message, resp interface{}) {
	s.AddRPCAdjust(wantReq, resp, nil)
}

// AddRPCAdjust is like AddRPC, but accepts a function that can be used
// to tweak the requests before comparison, for example to adjust for
// randomness.
func (s *MockServer) AddRPCAdjust(wantReq proto.Message, resp interface{}, adjust func(gotReq proto.Message)) {
	s.reqItems = append(s.reqItems, reqItem{wantReq, adjust})
	s.resps = append(s.resps, resp)
}

// popRPC compares the request with the next expected (request, response) pair.
// It returns the response, or an error if the request doesn't match what
// was expected or there are no expected rpcs.
func (s *MockServer) popRPC(gotReq proto.Message) (interface{}, error) {
	if len(s.reqItems) == 0 || len(s.resps) == 0 {
		panic("mockcbt.popRPC: Out of RPCs.")
	}
	ri := s.reqItems[0]
	resp := s.resps[0]
	s.reqItems = s.reqItems[1:]
	s.resps = s.resps[1:]
	if ri.wantReq != nil {
		if ri.adjust != nil {
			ri.adjust(gotReq)
		}
		if !proto.Equal(gotReq, ri.wantReq) {
			return nil, errors.NewUnknownError(fmt.Sprintf("mockcbt.popRPC: Bad request\ngot:  %T\n%s\nwant: %T\n%s",
				gotReq, proto.MarshalTextString(gotReq),
				ri.wantReq, proto.MarshalTextString(ri.wantReq)))
		}
	}
	if err, ok := resp.(error); ok {
		return nil, err
	}
	return resp, nil
}
