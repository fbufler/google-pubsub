package payload

import (
	"context"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
)

type CreateTopicRequest struct {
	payload *pubsubpb.Topic
}

func NewCreateTopicRequest(ctx context.Context, req *connect.Request[pubsubpb.Topic]) *CreateTopicRequest {
	return &CreateTopicRequest{payload: req.Msg}
}

func (r *CreateTopicRequest) Payload() *pubsubpb.Topic { return r.payload }

type CreateTopicResponse struct {
	payload *pubsubpb.Topic
}

func NewCreateTopicResponse(ctx context.Context, topic *pubsubpb.Topic) *CreateTopicResponse {
	return &CreateTopicResponse{payload: topic}
}

func (r *CreateTopicResponse) Encode() *connect.Response[pubsubpb.Topic] {
	return connect.NewResponse(r.payload)
}

type GetTopicRequest struct {
	payload *pubsubpb.GetTopicRequest
}

func NewGetTopicRequest(ctx context.Context, req *connect.Request[pubsubpb.GetTopicRequest]) *GetTopicRequest {
	return &GetTopicRequest{payload: req.Msg}
}

func (r *GetTopicRequest) Payload() *pubsubpb.GetTopicRequest { return r.payload }

type GetTopicResponse struct {
	payload *pubsubpb.Topic
}

func NewGetTopicResponse(ctx context.Context, topic *pubsubpb.Topic) *GetTopicResponse {
	return &GetTopicResponse{payload: topic}
}

func (r *GetTopicResponse) Encode() *connect.Response[pubsubpb.Topic] {
	return connect.NewResponse(r.payload)
}

type UpdateTopicRequest struct {
	payload *pubsubpb.UpdateTopicRequest
}

func NewUpdateTopicRequest(ctx context.Context, req *connect.Request[pubsubpb.UpdateTopicRequest]) *UpdateTopicRequest {
	return &UpdateTopicRequest{payload: req.Msg}
}

func (r *UpdateTopicRequest) Payload() *pubsubpb.UpdateTopicRequest { return r.payload }

type UpdateTopicResponse struct {
	payload *pubsubpb.Topic
}

func NewUpdateTopicResponse(ctx context.Context, topic *pubsubpb.Topic) *UpdateTopicResponse {
	return &UpdateTopicResponse{payload: topic}
}

func (r *UpdateTopicResponse) Encode() *connect.Response[pubsubpb.Topic] {
	return connect.NewResponse(r.payload)
}

type DeleteTopicRequest struct {
	payload *pubsubpb.DeleteTopicRequest
}

func NewDeleteTopicRequest(ctx context.Context, req *connect.Request[pubsubpb.DeleteTopicRequest]) *DeleteTopicRequest {
	return &DeleteTopicRequest{payload: req.Msg}
}

func (r *DeleteTopicRequest) Payload() *pubsubpb.DeleteTopicRequest { return r.payload }

type ListTopicsRequest struct {
	payload *pubsubpb.ListTopicsRequest
}

func NewListTopicsRequest(ctx context.Context, req *connect.Request[pubsubpb.ListTopicsRequest]) *ListTopicsRequest {
	return &ListTopicsRequest{payload: req.Msg}
}

func (r *ListTopicsRequest) Payload() *pubsubpb.ListTopicsRequest { return r.payload }

type ListTopicsResponse struct {
	payload *pubsubpb.ListTopicsResponse
}

func NewListTopicsResponse(ctx context.Context, resp *pubsubpb.ListTopicsResponse) *ListTopicsResponse {
	return &ListTopicsResponse{payload: resp}
}

func (r *ListTopicsResponse) Encode() *connect.Response[pubsubpb.ListTopicsResponse] {
	return connect.NewResponse(r.payload)
}

type ListTopicSubscriptionsRequest struct {
	payload *pubsubpb.ListTopicSubscriptionsRequest
}

func NewListTopicSubscriptionsRequest(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSubscriptionsRequest]) *ListTopicSubscriptionsRequest {
	return &ListTopicSubscriptionsRequest{payload: req.Msg}
}

func (r *ListTopicSubscriptionsRequest) Payload() *pubsubpb.ListTopicSubscriptionsRequest {
	return r.payload
}

type ListTopicSubscriptionsResponse struct {
	payload *pubsubpb.ListTopicSubscriptionsResponse
}

func NewListTopicSubscriptionsResponse(ctx context.Context, resp *pubsubpb.ListTopicSubscriptionsResponse) *ListTopicSubscriptionsResponse {
	return &ListTopicSubscriptionsResponse{payload: resp}
}

func (r *ListTopicSubscriptionsResponse) Encode() *connect.Response[pubsubpb.ListTopicSubscriptionsResponse] {
	return connect.NewResponse(r.payload)
}

type ListTopicSnapshotsRequest struct {
	payload *pubsubpb.ListTopicSnapshotsRequest
}

func NewListTopicSnapshotsRequest(ctx context.Context, req *connect.Request[pubsubpb.ListTopicSnapshotsRequest]) *ListTopicSnapshotsRequest {
	return &ListTopicSnapshotsRequest{payload: req.Msg}
}

func (r *ListTopicSnapshotsRequest) Payload() *pubsubpb.ListTopicSnapshotsRequest { return r.payload }

type ListTopicSnapshotsResponse struct {
	payload *pubsubpb.ListTopicSnapshotsResponse
}

func NewListTopicSnapshotsResponse(ctx context.Context, resp *pubsubpb.ListTopicSnapshotsResponse) *ListTopicSnapshotsResponse {
	return &ListTopicSnapshotsResponse{payload: resp}
}

func (r *ListTopicSnapshotsResponse) Encode() *connect.Response[pubsubpb.ListTopicSnapshotsResponse] {
	return connect.NewResponse(r.payload)
}

type PublishRequest struct {
	payload *pubsubpb.PublishRequest
}

func NewPublishRequest(ctx context.Context, req *connect.Request[pubsubpb.PublishRequest]) *PublishRequest {
	return &PublishRequest{payload: req.Msg}
}

func (r *PublishRequest) Payload() *pubsubpb.PublishRequest { return r.payload }

type PublishResponse struct {
	payload *pubsubpb.PublishResponse
}

func NewPublishResponse(ctx context.Context, resp *pubsubpb.PublishResponse) *PublishResponse {
	return &PublishResponse{payload: resp}
}

func (r *PublishResponse) Encode() *connect.Response[pubsubpb.PublishResponse] {
	return connect.NewResponse(r.payload)
}

type DetachSubscriptionRequest struct {
	payload *pubsubpb.DetachSubscriptionRequest
}

func NewDetachSubscriptionRequest(ctx context.Context, req *connect.Request[pubsubpb.DetachSubscriptionRequest]) *DetachSubscriptionRequest {
	return &DetachSubscriptionRequest{payload: req.Msg}
}

func (r *DetachSubscriptionRequest) Payload() *pubsubpb.DetachSubscriptionRequest { return r.payload }

type DetachSubscriptionResponse struct {
	payload *pubsubpb.DetachSubscriptionResponse
}

func NewDetachSubscriptionResponse(ctx context.Context, resp *pubsubpb.DetachSubscriptionResponse) *DetachSubscriptionResponse {
	return &DetachSubscriptionResponse{payload: resp}
}

func (r *DetachSubscriptionResponse) Encode() *connect.Response[pubsubpb.DetachSubscriptionResponse] {
	return connect.NewResponse(r.payload)
}
