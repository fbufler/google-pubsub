//go:build integration

package integration_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

func TestReflection_ServicesExposed(t *testing.T) {
	host := emulatorHost()
	ctx := context.Background()

	// 1. Connect via standard gRPC
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	// 2. Use the official Google reflection stub (bypassing third-party wrappers)
	stub := grpc_reflection_v1alpha.NewServerReflectionClient(conn)
	stream, err := stub.ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}

	// 3. Send the raw ListServices request
	err = stream.Send(&grpc_reflection_v1alpha.ServerReflectionRequest{
		Host: host,
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_ListServices{
			ListServices: "", // This empty string acts as the trigger for the list command
		},
	})
	if err != nil {
		t.Fatalf("stream.Send: %v", err)
	}

	// 4. Receive the response
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv: %v", err)
	}

	listResp := resp.GetListServicesResponse()
	if listResp == nil {
		t.Fatalf("expected ListServicesResponse, got %T", resp.MessageResponse)
	}

	// 5. Verify our Pub/Sub services exist
	wantServices := map[string]bool{
		"google.pubsub.v1.Publisher":  false,
		"google.pubsub.v1.Subscriber": false,
	}

	for _, s := range listResp.Service {
		if _, ok := wantServices[s.Name]; ok {
			wantServices[s.Name] = true
		}
	}

	for s, found := range wantServices {
		if !found {
			t.Errorf("expected reflection to expose service %q, got: %v", s, listResp.Service)
		}
	}
}
