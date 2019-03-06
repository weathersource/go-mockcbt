package mockcbt

// A simple mock server.

import (
	"context"
	"fmt"

	gsrv "github.com/weathersource/go-gsrv"
	pb "google.golang.org/genproto/googleapis/bigtable/v2"
)

type MockBtServer struct {
	*MockServer
}

func newBtServer() (*MockBtServer, error) {
	srv, err := gsrv.NewServer()
	if err != nil {
		return nil, err
	}
	mock := &MockBtServer{&MockServer{Addr: srv.Addr}}
	pb.RegisterBigtableServer(srv.Gsrv, mock)
	srv.Start()
	return mock, nil
}

// ReadRows streams back the contents of all requested rows in key order,
// optionally applying the same Reader filter to each. Depending on their
// size, rows and cells may be broken up across multiple responses, but
// atomicity of each row will still be preserved. See the
// ReadRowsResponse documentation for details.
func (s *MockBtServer) ReadRows(req *pb.ReadRowsRequest, svr pb.Bigtable_ReadRowsServer) error {
	res, err := s.popRPC(req)
	if err != nil {
		return err
	}
	responses := res.([]interface{})
	for _, res := range responses {
		switch res := res.(type) {
		case *pb.ReadRowsResponse:
			if err := svr.Send(res); err != nil {
				return err
			}
		case error:
			return res
		default:
			panic(fmt.Sprintf("mockbigtable.ReadRows: Bad response type: %+v", res))
		}
	}
	return nil
}

// SampleRowKeys returns a sample of row keys in the table. The returned
// row keys will delimit contiguous sections of the table of approximately
// equal size, which can be used to break up the data for distributed tasks
// like mapreduces.

func (s *MockBtServer) SampleRowKeys(req *pb.SampleRowKeysRequest, svr pb.Bigtable_SampleRowKeysServer) error {
	res, err := s.popRPC(req)
	if err != nil {
		return err
	}
	responses := res.([]interface{})
	for _, res := range responses {
		switch res := res.(type) {
		case *pb.SampleRowKeysResponse:
			if err := svr.Send(res); err != nil {
				return err
			}
		case error:
			return res
		default:
			panic(fmt.Sprintf("mockbigtable.SampleRowKeys: Bad response type: %+v", res))
		}
	}
	return nil
}

// MutateRow mutates a row atomically. Cells already present in the row are
// left unchanged unless explicitly changed by `mutation`.
func (s *MockBtServer) MutateRow(ctx context.Context, req *pb.MutateRowRequest) (*pb.MutateRowResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.MutateRowResponse), nil
}

// MutateRows mutates multiple rows in a batch. Each individual row is
// mutated atomically as in MutateRow, but the entire batch is not executed
// atomically.
func (s *MockBtServer) MutateRows(req *pb.MutateRowsRequest, svr pb.Bigtable_MutateRowsServer) error {
	res, err := s.popRPC(req)
	if err != nil {
		return err
	}
	responses := res.([]interface{})
	for _, res := range responses {
		switch res := res.(type) {
		case *pb.MutateRowsResponse:
			if err := svr.Send(res); err != nil {
				return err
			}
		case error:
			return res
		default:
			panic(fmt.Sprintf("mockbigtable.MutateRows: Bad response type: %+v", res))
		}
	}
	return nil
}

// CheckAndMutateRow mutates a row atomically based on the output of a
// predicate Reader filter.
func (s *MockBtServer) CheckAndMutateRow(ctx context.Context, req *pb.CheckAndMutateRowRequest) (*pb.CheckAndMutateRowResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.CheckAndMutateRowResponse), nil
}

// ReadModifyWriteRow modifies a row atomically on the server. The method
// reads the latest existing timestamp and value from the specified columns
// and writes a new entry based on predefined read/modify/write rules. The
// new value for the timestamp is the greater of the existing timestamp or
// the current server time. The method returns the new contents of all
// modified cells.
func (s *MockBtServer) ReadModifyWriteRow(ctx context.Context, req *pb.ReadModifyWriteRowRequest) (*pb.ReadModifyWriteRowResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ReadModifyWriteRowResponse), nil
}
