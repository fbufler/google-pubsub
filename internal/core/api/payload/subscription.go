package payload

import (
	"context"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

type CreateSubscriptionRequest struct {
	payload *pubsubpb.Subscription
}

func NewCreateSubscriptionRequest(ctx context.Context, req *connect.Request[pubsubpb.Subscription]) *CreateSubscriptionRequest {
	return &CreateSubscriptionRequest{payload: req.Msg}
}

func (r *CreateSubscriptionRequest) Payload() *pubsubpb.Subscription { return r.payload }

type CreateSubscriptionResponse struct {
	payload *pubsubpb.Subscription
}

func NewCreateSubscriptionResponse(ctx context.Context, sub *pubsubpb.Subscription) *CreateSubscriptionResponse {
	return &CreateSubscriptionResponse{payload: sub}
}

func (r *CreateSubscriptionResponse) Encode() *connect.Response[pubsubpb.Subscription] {
	return connect.NewResponse(r.payload)
}

type GetSubscriptionRequest struct {
	payload *pubsubpb.GetSubscriptionRequest
}

func NewGetSubscriptionRequest(ctx context.Context, req *connect.Request[pubsubpb.GetSubscriptionRequest]) *GetSubscriptionRequest {
	return &GetSubscriptionRequest{payload: req.Msg}
}

func (r *GetSubscriptionRequest) Payload() *pubsubpb.GetSubscriptionRequest { return r.payload }

type GetSubscriptionResponse struct {
	payload *pubsubpb.Subscription
}

func NewGetSubscriptionResponse(ctx context.Context, sub *pubsubpb.Subscription) *GetSubscriptionResponse {
	return &GetSubscriptionResponse{payload: sub}
}

func (r *GetSubscriptionResponse) Encode() *connect.Response[pubsubpb.Subscription] {
	return connect.NewResponse(r.payload)
}

type UpdateSubscriptionRequest struct {
	payload *pubsubpb.UpdateSubscriptionRequest
}

func NewUpdateSubscriptionRequest(ctx context.Context, req *connect.Request[pubsubpb.UpdateSubscriptionRequest]) *UpdateSubscriptionRequest {
	return &UpdateSubscriptionRequest{payload: req.Msg}
}

func (r *UpdateSubscriptionRequest) Payload() *pubsubpb.UpdateSubscriptionRequest { return r.payload }

type UpdateSubscriptionResponse struct {
	payload *pubsubpb.Subscription
}

func NewUpdateSubscriptionResponse(ctx context.Context, sub *pubsubpb.Subscription) *UpdateSubscriptionResponse {
	return &UpdateSubscriptionResponse{payload: sub}
}

func (r *UpdateSubscriptionResponse) Encode() *connect.Response[pubsubpb.Subscription] {
	return connect.NewResponse(r.payload)
}

type DeleteSubscriptionRequest struct {
	payload *pubsubpb.DeleteSubscriptionRequest
}

func NewDeleteSubscriptionRequest(ctx context.Context, req *connect.Request[pubsubpb.DeleteSubscriptionRequest]) *DeleteSubscriptionRequest {
	return &DeleteSubscriptionRequest{payload: req.Msg}
}

func (r *DeleteSubscriptionRequest) Payload() *pubsubpb.DeleteSubscriptionRequest { return r.payload }

type ListSubscriptionsRequest struct {
	payload *pubsubpb.ListSubscriptionsRequest
}

func NewListSubscriptionsRequest(ctx context.Context, req *connect.Request[pubsubpb.ListSubscriptionsRequest]) *ListSubscriptionsRequest {
	return &ListSubscriptionsRequest{payload: req.Msg}
}

func (r *ListSubscriptionsRequest) Payload() *pubsubpb.ListSubscriptionsRequest { return r.payload }

type ListSubscriptionsResponse struct {
	payload *pubsubpb.ListSubscriptionsResponse
}

func NewListSubscriptionsResponse(ctx context.Context, resp *pubsubpb.ListSubscriptionsResponse) *ListSubscriptionsResponse {
	return &ListSubscriptionsResponse{payload: resp}
}

func (r *ListSubscriptionsResponse) Encode() *connect.Response[pubsubpb.ListSubscriptionsResponse] {
	return connect.NewResponse(r.payload)
}

type PullRequest struct {
	payload *pubsubpb.PullRequest
}

func NewPullRequest(ctx context.Context, req *connect.Request[pubsubpb.PullRequest]) *PullRequest {
	return &PullRequest{payload: req.Msg}
}

func (r *PullRequest) Payload() *pubsubpb.PullRequest { return r.payload }

type PullResponse struct {
	payload *pubsubpb.PullResponse
}

func NewPullResponse(ctx context.Context, resp *pubsubpb.PullResponse) *PullResponse {
	return &PullResponse{payload: resp}
}

func (r *PullResponse) Encode() *connect.Response[pubsubpb.PullResponse] {
	return connect.NewResponse(r.payload)
}

type AcknowledgeRequest struct {
	payload *pubsubpb.AcknowledgeRequest
}

func NewAcknowledgeRequest(ctx context.Context, req *connect.Request[pubsubpb.AcknowledgeRequest]) *AcknowledgeRequest {
	return &AcknowledgeRequest{payload: req.Msg}
}

func (r *AcknowledgeRequest) Payload() *pubsubpb.AcknowledgeRequest { return r.payload }

type ModifyAckDeadlineRequest struct {
	payload *pubsubpb.ModifyAckDeadlineRequest
}

func NewModifyAckDeadlineRequest(ctx context.Context, req *connect.Request[pubsubpb.ModifyAckDeadlineRequest]) *ModifyAckDeadlineRequest {
	return &ModifyAckDeadlineRequest{payload: req.Msg}
}

func (r *ModifyAckDeadlineRequest) Payload() *pubsubpb.ModifyAckDeadlineRequest { return r.payload }

type ModifyPushConfigRequest struct {
	payload *pubsubpb.ModifyPushConfigRequest
}

func NewModifyPushConfigRequest(ctx context.Context, req *connect.Request[pubsubpb.ModifyPushConfigRequest]) *ModifyPushConfigRequest {
	return &ModifyPushConfigRequest{payload: req.Msg}
}

func (r *ModifyPushConfigRequest) Payload() *pubsubpb.ModifyPushConfigRequest { return r.payload }

// EmptyResponse is a reusable response wrapper for endpoints returning emptypb.Empty.
type EmptyResponse struct{}

func NewEmptyResponse(_ context.Context) *EmptyResponse { return &EmptyResponse{} }

func (r *EmptyResponse) Encode() *connect.Response[emptypb.Empty] {
	return connect.NewResponse(&emptypb.Empty{})
}
