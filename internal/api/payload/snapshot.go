package payload

import (
	"context"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
)

type CreateSnapshotRequest struct {
	payload *pubsubpb.CreateSnapshotRequest
}

func NewCreateSnapshotRequest(ctx context.Context, req *connect.Request[pubsubpb.CreateSnapshotRequest]) *CreateSnapshotRequest {
	return &CreateSnapshotRequest{payload: req.Msg}
}

func (r *CreateSnapshotRequest) Payload() *pubsubpb.CreateSnapshotRequest { return r.payload }

type CreateSnapshotResponse struct {
	payload *pubsubpb.Snapshot
}

func NewCreateSnapshotResponse(ctx context.Context, snap *pubsubpb.Snapshot) *CreateSnapshotResponse {
	return &CreateSnapshotResponse{payload: snap}
}

func (r *CreateSnapshotResponse) Encode() *connect.Response[pubsubpb.Snapshot] {
	return connect.NewResponse(r.payload)
}

type GetSnapshotRequest struct {
	payload *pubsubpb.GetSnapshotRequest
}

func NewGetSnapshotRequest(ctx context.Context, req *connect.Request[pubsubpb.GetSnapshotRequest]) *GetSnapshotRequest {
	return &GetSnapshotRequest{payload: req.Msg}
}

func (r *GetSnapshotRequest) Payload() *pubsubpb.GetSnapshotRequest { return r.payload }

type GetSnapshotResponse struct {
	payload *pubsubpb.Snapshot
}

func NewGetSnapshotResponse(ctx context.Context, snap *pubsubpb.Snapshot) *GetSnapshotResponse {
	return &GetSnapshotResponse{payload: snap}
}

func (r *GetSnapshotResponse) Encode() *connect.Response[pubsubpb.Snapshot] {
	return connect.NewResponse(r.payload)
}

type UpdateSnapshotRequest struct {
	payload *pubsubpb.UpdateSnapshotRequest
}

func NewUpdateSnapshotRequest(ctx context.Context, req *connect.Request[pubsubpb.UpdateSnapshotRequest]) *UpdateSnapshotRequest {
	return &UpdateSnapshotRequest{payload: req.Msg}
}

func (r *UpdateSnapshotRequest) Payload() *pubsubpb.UpdateSnapshotRequest { return r.payload }

type UpdateSnapshotResponse struct {
	payload *pubsubpb.Snapshot
}

func NewUpdateSnapshotResponse(ctx context.Context, snap *pubsubpb.Snapshot) *UpdateSnapshotResponse {
	return &UpdateSnapshotResponse{payload: snap}
}

func (r *UpdateSnapshotResponse) Encode() *connect.Response[pubsubpb.Snapshot] {
	return connect.NewResponse(r.payload)
}

type DeleteSnapshotRequest struct {
	payload *pubsubpb.DeleteSnapshotRequest
}

func NewDeleteSnapshotRequest(ctx context.Context, req *connect.Request[pubsubpb.DeleteSnapshotRequest]) *DeleteSnapshotRequest {
	return &DeleteSnapshotRequest{payload: req.Msg}
}

func (r *DeleteSnapshotRequest) Payload() *pubsubpb.DeleteSnapshotRequest { return r.payload }

type ListSnapshotsRequest struct {
	payload *pubsubpb.ListSnapshotsRequest
}

func NewListSnapshotsRequest(ctx context.Context, req *connect.Request[pubsubpb.ListSnapshotsRequest]) *ListSnapshotsRequest {
	return &ListSnapshotsRequest{payload: req.Msg}
}

func (r *ListSnapshotsRequest) Payload() *pubsubpb.ListSnapshotsRequest { return r.payload }

type ListSnapshotsResponse struct {
	payload *pubsubpb.ListSnapshotsResponse
}

func NewListSnapshotsResponse(ctx context.Context, resp *pubsubpb.ListSnapshotsResponse) *ListSnapshotsResponse {
	return &ListSnapshotsResponse{payload: resp}
}

func (r *ListSnapshotsResponse) Encode() *connect.Response[pubsubpb.ListSnapshotsResponse] {
	return connect.NewResponse(r.payload)
}

type SeekRequest struct {
	payload *pubsubpb.SeekRequest
}

func NewSeekRequest(ctx context.Context, req *connect.Request[pubsubpb.SeekRequest]) *SeekRequest {
	return &SeekRequest{payload: req.Msg}
}

func (r *SeekRequest) Payload() *pubsubpb.SeekRequest { return r.payload }

type SeekResponse struct {
	payload *pubsubpb.SeekResponse
}

func NewSeekResponse(ctx context.Context, resp *pubsubpb.SeekResponse) *SeekResponse {
	return &SeekResponse{payload: resp}
}

func (r *SeekResponse) Encode() *connect.Response[pubsubpb.SeekResponse] {
	return connect.NewResponse(r.payload)
}
