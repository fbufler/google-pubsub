package handler

import (
	"context"
	"fmt"
	"log/slog"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fbufler/google-pubsub/internal/api/mapper"
	"github.com/fbufler/google-pubsub/internal/api/payload"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

func NewCreateSnapshot(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.CreateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
		request := payload.NewCreateSnapshotRequest(ctx, req)

		snap, err := mapper.CreateSnapshotRequestToSnapshot(request.Payload())
		if err != nil {
			return nil, toConnectError(err)
		}

		if err := uc.CreateSnapshot(ctx, snap); err != nil {
			logErr(err, "create snapshot failed", "snapshot", request.Payload().Name)
			return nil, toConnectError(err)
		}
		slog.Debug("snapshot created", "snapshot", request.Payload().Name, "subscription", request.Payload().Subscription)

		return payload.NewCreateSnapshotResponse(ctx, mapper.SnapshotToProto(snap)).Encode(), nil
	}
}

func NewGetSnapshot(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.GetSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
		request := payload.NewGetSnapshotRequest(ctx, req)

		snap, err := uc.GetSnapshot(ctx, types.FQDN(request.Payload().Snapshot))
		if err != nil {
			logErr(err, "get snapshot failed", "snapshot", request.Payload().Snapshot)
			return nil, toConnectError(err)
		}
		slog.Debug("snapshot retrieved", "snapshot", request.Payload().Snapshot)

		return payload.NewGetSnapshotResponse(ctx, mapper.SnapshotToProto(snap)).Encode(), nil
	}
}

func NewUpdateSnapshot(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.UpdateSnapshotRequest]) (*connect.Response[pubsubpb.Snapshot], error) {
		request := payload.NewUpdateSnapshotRequest(ctx, req)
		msg := request.Payload()

		if msg.Snapshot == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("snapshot is required"))
		}

		existing, err := uc.GetSnapshot(ctx, types.FQDN(msg.Snapshot.Name))
		if err != nil {
			logErr(err, "update snapshot: get failed", "snapshot", msg.Snapshot.Name)
			return nil, toConnectError(err)
		}

		for _, path := range msg.UpdateMask.GetPaths() {
			switch path {
			case "labels":
				if err := existing.SetLabels(types.Labels(msg.Snapshot.Labels)); err != nil {
					return nil, toConnectError(err)
				}
			case "expire_time":
				if msg.Snapshot.ExpireTime != nil {
					if err := existing.RestoreExpireTime(msg.Snapshot.ExpireTime.AsTime()); err != nil {
						return nil, toConnectError(err)
					}
				}
			}
		}

		if err := uc.UpdateSnapshot(ctx, existing); err != nil {
			logErr(err, "update snapshot failed", "snapshot", msg.Snapshot.Name)
			return nil, toConnectError(err)
		}
		slog.Debug("snapshot updated", "snapshot", msg.Snapshot.Name)

		return payload.NewUpdateSnapshotResponse(ctx, mapper.SnapshotToProto(existing)).Encode(), nil
	}
}

func NewDeleteSnapshot(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.DeleteSnapshotRequest]) (*connect.Response[emptypb.Empty], error) {
		request := payload.NewDeleteSnapshotRequest(ctx, req)

		if err := uc.DeleteSnapshot(ctx, types.FQDN(request.Payload().Snapshot)); err != nil {
			logErr(err, "delete snapshot failed", "snapshot", request.Payload().Snapshot)
			return nil, toConnectError(err)
		}
		slog.Debug("snapshot deleted", "snapshot", request.Payload().Snapshot)

		return payload.NewEmptyResponse(ctx).Encode(), nil
	}
}

func NewListSnapshots(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.ListSnapshotsRequest]) (*connect.Response[pubsubpb.ListSnapshotsResponse], error) {
		request := payload.NewListSnapshotsRequest(ctx, req)

		snaps, err := uc.ListSnapshots(ctx, projectID(request.Payload().Project))
		if err != nil {
			logErr(err, "list snapshots failed", "project", request.Payload().Project)
			return nil, toConnectError(err)
		}

		resp := &pubsubpb.ListSnapshotsResponse{}
		for _, snap := range snaps {
			resp.Snapshots = append(resp.Snapshots, mapper.SnapshotToProto(snap))
		}
		slog.Debug("snapshots listed", "project", request.Payload().Project, "count", len(resp.Snapshots))

		return payload.NewListSnapshotsResponse(ctx, resp).Encode(), nil
	}
}

func NewSeek(_ context.Context, uc *usecases.SnapshotUsecase) func(context.Context, *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error) {
	return func(ctx context.Context, req *connect.Request[pubsubpb.SeekRequest]) (*connect.Response[pubsubpb.SeekResponse], error) {
		request := payload.NewSeekRequest(ctx, req)
		msg := request.Payload()

		subName := types.FQDN(msg.Subscription)
		switch target := msg.Target.(type) {
		case *pubsubpb.SeekRequest_Time:
			if err := uc.SeekToTime(ctx, subName, target.Time.AsTime()); err != nil {
				logErr(err, "seek to time failed", "subscription", msg.Subscription)
				return nil, toConnectError(err)
			}
			slog.Debug("subscription seeked to time", "subscription", msg.Subscription, "time", target.Time.AsTime())
		case *pubsubpb.SeekRequest_Snapshot:
			if err := uc.SeekToSnapshot(ctx, subName, types.FQDN(target.Snapshot)); err != nil {
				logErr(err, "seek to snapshot failed", "subscription", msg.Subscription, "snapshot", target.Snapshot)
				return nil, toConnectError(err)
			}
			slog.Debug("subscription seeked to snapshot", "subscription", msg.Subscription, "snapshot", target.Snapshot)
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("seek target is required"))
		}

		return payload.NewSeekResponse(ctx, &pubsubpb.SeekResponse{}).Encode(), nil
	}
}
