package mockcbt

import (
	"context"
	"testing"

	empty "google.golang.org/protobuf/types/known/emptypb"
	"github.com/stretchr/testify/assert"
	errors "github.com/weathersource/go-errors"
	pb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	v1 "google.golang.org/genproto/googleapis/iam/v1"
	"google.golang.org/genproto/googleapis/longrunning"
)

func TestNewBtAdminServer(t *testing.T) {
	assert := assert.New(t)

	server, err := newBtAdminServer()
	assert.NotNil(server)
	assert.Nil(err)
}

func TestCreateInstance(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.CreateInstanceRequest{},
		&longrunning.Operation{},
	)
	resp, err := srv.CreateInstance(ctx, &pb.CreateInstanceRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.CreateInstanceRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.CreateInstance(ctx, &pb.CreateInstanceRequest{})
	assert.NotNil(err)
}

func TestGetInstance(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.GetInstanceRequest{},
		&pb.Instance{},
	)
	resp, err := srv.GetInstance(ctx, &pb.GetInstanceRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.GetInstanceRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.GetInstance(ctx, &pb.GetInstanceRequest{})
	assert.NotNil(err)
}

func TestListInstances(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.ListInstancesRequest{},
		&pb.ListInstancesResponse{},
	)
	resp, err := srv.ListInstances(ctx, &pb.ListInstancesRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.ListInstancesRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.ListInstances(ctx, &pb.ListInstancesRequest{})
	assert.NotNil(err)
}

func TestUpdateInstance(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.Instance{},
		&pb.Instance{},
	)
	resp, err := srv.UpdateInstance(ctx, &pb.Instance{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.Instance{},
		errors.NewInternalError(""),
	)
	_, err = srv.UpdateInstance(ctx, &pb.Instance{})
	assert.NotNil(err)
}

func TestPartialUpdateInstance(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.PartialUpdateInstanceRequest{},
		&longrunning.Operation{},
	)
	resp, err := srv.PartialUpdateInstance(ctx, &pb.PartialUpdateInstanceRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.PartialUpdateInstanceRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.PartialUpdateInstance(ctx, &pb.PartialUpdateInstanceRequest{})
	assert.NotNil(err)
}

func TestDeleteInstance(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.DeleteInstanceRequest{},
		&empty.Empty{},
	)
	resp, err := srv.DeleteInstance(ctx, &pb.DeleteInstanceRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.DeleteInstanceRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.DeleteInstance(ctx, &pb.DeleteInstanceRequest{})
	assert.NotNil(err)
}

func TestCreateCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.CreateClusterRequest{},
		&longrunning.Operation{},
	)
	resp, err := srv.CreateCluster(ctx, &pb.CreateClusterRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.CreateClusterRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.CreateCluster(ctx, &pb.CreateClusterRequest{})
	assert.NotNil(err)
}

func TestGetCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.GetClusterRequest{},
		&pb.Cluster{},
	)
	resp, err := srv.GetCluster(ctx, &pb.GetClusterRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.GetClusterRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.GetCluster(ctx, &pb.GetClusterRequest{})
	assert.NotNil(err)
}

func TestListClusters(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.ListClustersRequest{},
		&pb.ListClustersResponse{},
	)
	resp, err := srv.ListClusters(ctx, &pb.ListClustersRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.ListClustersRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.ListClusters(ctx, &pb.ListClustersRequest{})
	assert.NotNil(err)
}

func TestUpdateCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.Cluster{},
		&longrunning.Operation{},
	)
	resp, err := srv.UpdateCluster(ctx, &pb.Cluster{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.Cluster{},
		errors.NewInternalError(""),
	)
	_, err = srv.UpdateCluster(ctx, &pb.Cluster{})
	assert.NotNil(err)
}

func TestPartialUpdateCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.PartialUpdateClusterRequest{},
		&longrunning.Operation{},
	)
	resp, err := srv.PartialUpdateCluster(ctx, &pb.PartialUpdateClusterRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.Cluster{},
		errors.NewInternalError(""),
	)
	_, err = srv.PartialUpdateCluster(ctx, &pb.PartialUpdateClusterRequest{})
	assert.NotNil(err)
}

func TestDeleteCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.DeleteClusterRequest{},
		&empty.Empty{},
	)
	resp, err := srv.DeleteCluster(ctx, &pb.DeleteClusterRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.DeleteClusterRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.DeleteCluster(ctx, &pb.DeleteClusterRequest{})
	assert.NotNil(err)
}

func TestCreateAppProfile(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.CreateAppProfileRequest{},
		&pb.AppProfile{},
	)
	resp, err := srv.CreateAppProfile(ctx, &pb.CreateAppProfileRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.CreateAppProfileRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.CreateAppProfile(ctx, &pb.CreateAppProfileRequest{})
	assert.NotNil(err)
}

func TestGetAppProfile(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.GetAppProfileRequest{},
		&pb.AppProfile{},
	)
	resp, err := srv.GetAppProfile(ctx, &pb.GetAppProfileRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.GetAppProfileRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.GetAppProfile(ctx, &pb.GetAppProfileRequest{})
	assert.NotNil(err)
}

func TestListAppProfiles(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.ListAppProfilesRequest{},
		&pb.ListAppProfilesResponse{},
	)
	resp, err := srv.ListAppProfiles(ctx, &pb.ListAppProfilesRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.ListAppProfilesRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.ListAppProfiles(ctx, &pb.ListAppProfilesRequest{})
	assert.NotNil(err)
}

func TestUpdateAppProfile(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.UpdateAppProfileRequest{},
		&longrunning.Operation{},
	)
	resp, err := srv.UpdateAppProfile(ctx, &pb.UpdateAppProfileRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.UpdateAppProfileRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.UpdateAppProfile(ctx, &pb.UpdateAppProfileRequest{})
	assert.NotNil(err)
}

func TestDeleteAppProfile(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.DeleteAppProfileRequest{},
		&empty.Empty{},
	)
	resp, err := srv.DeleteAppProfile(ctx, &pb.DeleteAppProfileRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.DeleteAppProfileRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.DeleteAppProfile(ctx, &pb.DeleteAppProfileRequest{})
	assert.NotNil(err)
}

func TestGetIamPolicy(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&v1.GetIamPolicyRequest{},
		&v1.Policy{},
	)
	resp, err := srv.GetIamPolicy(ctx, &v1.GetIamPolicyRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&v1.GetIamPolicyRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.GetIamPolicy(ctx, &v1.GetIamPolicyRequest{})
	assert.NotNil(err)
}

func TestSetIamPolicy(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&v1.SetIamPolicyRequest{},
		&v1.Policy{},
	)
	resp, err := srv.SetIamPolicy(ctx, &v1.SetIamPolicyRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&v1.SetIamPolicyRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.SetIamPolicy(ctx, &v1.SetIamPolicyRequest{})
	assert.NotNil(err)
}

func TestTestIamPermissions(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&v1.TestIamPermissionsRequest{},
		&v1.TestIamPermissionsResponse{},
	)
	resp, err := srv.TestIamPermissions(ctx, &v1.TestIamPermissionsRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&v1.TestIamPermissionsRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.TestIamPermissions(ctx, &v1.TestIamPermissionsRequest{})
	assert.NotNil(err)
}

func TestListHotTablets(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	_, srv, err := NewAdmin()
	assert.Nil(err)

	// test valid response
	srv.AddRPC(
		&pb.ListHotTabletsRequest{},
		&pb.ListHotTabletsResponse{},
	)
	resp, err := srv.ListHotTablets(ctx, &pb.ListHotTabletsRequest{})
	assert.Nil(err)
	assert.NotNil(resp)

	// test error response
	srv.AddRPC(
		&pb.ListHotTabletsRequest{},
		errors.NewInternalError(""),
	)
	_, err = srv.ListHotTablets(ctx, &pb.ListHotTabletsRequest{})
	assert.NotNil(err)
}
