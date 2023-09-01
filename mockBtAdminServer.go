package mockcbt

// A simple mock server.

import (
	"context"

	empty "google.golang.org/protobuf/types/known/emptypb"
	gsrv "github.com/weathersource/go-gsrv"
	pb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	v1 "google.golang.org/genproto/googleapis/iam/v1"
	"google.golang.org/genproto/googleapis/longrunning"
)

type MockBtAdminServer struct {
	*MockServer
}

func newBtAdminServer() (*MockBtAdminServer, error) {
	srv, err := gsrv.NewServer()
	if err != nil {
		return nil, err
	}
	mock := &MockBtAdminServer{&MockServer{Addr: srv.Addr}}
	pb.RegisterBigtableInstanceAdminServer(srv.Gsrv, mock)
	srv.Start()
	return mock, nil
}

// Create an instance within a project.
func (s *MockBtAdminServer) CreateInstance(ctx context.Context, req *pb.CreateInstanceRequest) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// Gets information about an instance.
func (s *MockBtAdminServer) GetInstance(ctx context.Context, req *pb.GetInstanceRequest) (*pb.Instance, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.Instance), nil
}

// Lists information about instances in a project.
func (s *MockBtAdminServer) ListInstances(ctx context.Context, req *pb.ListInstancesRequest) (*pb.ListInstancesResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ListInstancesResponse), nil
}

// Updates an instance within a project.
func (s *MockBtAdminServer) UpdateInstance(ctx context.Context, req *pb.Instance) (*pb.Instance, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.Instance), nil
}

// Partially updates an instance within a project.
func (s *MockBtAdminServer) PartialUpdateInstance(ctx context.Context, req *pb.PartialUpdateInstanceRequest) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// Delete an instance from a project.
func (s *MockBtAdminServer) DeleteInstance(ctx context.Context, req *pb.DeleteInstanceRequest) (*empty.Empty, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*empty.Empty), nil
}

// Creates a cluster within an instance.
func (s *MockBtAdminServer) CreateCluster(ctx context.Context, req *pb.CreateClusterRequest) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// Gets information about a cluster.
func (s *MockBtAdminServer) GetCluster(ctx context.Context, req *pb.GetClusterRequest) (*pb.Cluster, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.Cluster), nil
}

// Lists information about clusters in an instance.
func (s *MockBtAdminServer) ListClusters(ctx context.Context, req *pb.ListClustersRequest) (*pb.ListClustersResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ListClustersResponse), nil
}

// Updates a cluster within an instance.
func (s *MockBtAdminServer) UpdateCluster(ctx context.Context, req *pb.Cluster) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// To disable autoscaling, clear cluster_config.cluster_autoscaling_config,
// and explicitly set a serve_node count via the update_mask.
func (s *MockBtAdminServer) PartialUpdateCluster(ctx context.Context, req *pb.PartialUpdateClusterRequest) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// Deletes a cluster from an instance.
func (s *MockBtAdminServer) DeleteCluster(ctx context.Context, req *pb.DeleteClusterRequest) (*empty.Empty, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*empty.Empty), nil
}

// Creates an app profile within an instance.
func (s *MockBtAdminServer) CreateAppProfile(ctx context.Context, req *pb.CreateAppProfileRequest) (*pb.AppProfile, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.AppProfile), nil
}

// Gets information about an app profile.
func (s *MockBtAdminServer) GetAppProfile(ctx context.Context, req *pb.GetAppProfileRequest) (*pb.AppProfile, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.AppProfile), nil
}

// Lists information about app profiles in an instance.
func (s *MockBtAdminServer) ListAppProfiles(ctx context.Context, req *pb.ListAppProfilesRequest) (*pb.ListAppProfilesResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ListAppProfilesResponse), nil
}

// Updates an app profile within an instance.
func (s *MockBtAdminServer) UpdateAppProfile(ctx context.Context, req *pb.UpdateAppProfileRequest) (*longrunning.Operation, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*longrunning.Operation), nil
}

// Deletes an app profile from an instance.
func (s *MockBtAdminServer) DeleteAppProfile(ctx context.Context, req *pb.DeleteAppProfileRequest) (*empty.Empty, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*empty.Empty), nil
}

// Gets the access control policy for an instance resource. Returns an empty
// policy if an instance exists but does not have a policy set.
func (s *MockBtAdminServer) GetIamPolicy(ctx context.Context, req *v1.GetIamPolicyRequest) (*v1.Policy, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*v1.Policy), nil
}

// Sets the access control policy on an instance resource. Replaces any
// existing policy.
func (s *MockBtAdminServer) SetIamPolicy(ctx context.Context, req *v1.SetIamPolicyRequest) (*v1.Policy, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*v1.Policy), nil
}

// Returns permissions that the caller has on the specified instance resource.
func (s *MockBtAdminServer) TestIamPermissions(ctx context.Context, req *v1.TestIamPermissionsRequest) (*v1.TestIamPermissionsResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*v1.TestIamPermissionsResponse), nil
}

// Lists hot tablets in a cluster, within the time range provided. Hot
// tablets are ordered based on CPU usage.
func (s *MockBtAdminServer) ListHotTablets(ctx context.Context, req *pb.ListHotTabletsRequest) (*pb.ListHotTabletsResponse, error) {
	res, err := s.popRPC(req)
	if err != nil {
		return nil, err
	}
	return res.(*pb.ListHotTabletsResponse), nil
}
