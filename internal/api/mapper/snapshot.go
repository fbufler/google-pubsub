package mapper

import (
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

func CreateSnapshotRequestToSnapshot(req *pubsubpb.CreateSnapshotRequest) (*entities.Snapshot, error) {
	snap := new(entities.Snapshot)
	if err := snap.SetName(types.FQDN(req.Name)); err != nil {
		return nil, err
	}
	if err := snap.SetSubscriptionName(types.FQDN(req.Subscription)); err != nil {
		return nil, err
	}
	if len(req.Labels) > 0 {
		if err := snap.SetLabels(types.Labels(req.Labels)); err != nil {
			return nil, err
		}
	}
	return snap, nil
}

func SnapshotToProto(snap *entities.Snapshot) *pubsubpb.Snapshot {
	p := &pubsubpb.Snapshot{
		Name:   snap.Name().String(),
		Topic:  snap.TopicName().String(),
		Labels: map[string]string(snap.Labels()),
	}
	if !snap.ExpireTime().IsZero() {
		p.ExpireTime = timestamppb.New(snap.ExpireTime())
	}
	return p
}

func PendingMessageToProto(pm *entities.PendingMessage) *pubsubpb.ReceivedMessage {
	return &pubsubpb.ReceivedMessage{
		AckId:           pm.AckID(),
		DeliveryAttempt: pm.DeliveryAttempt(),
		Message: &pubsubpb.PubsubMessage{
			MessageId:   pm.Message().ID(),
			Data:        pm.Message().Data(),
			Attributes:  pm.Message().Attributes(),
			OrderingKey: pm.Message().OrderingKey(),
			PublishTime: timestamppb.New(pm.Message().PublishTime()),
		},
	}
}

func ProtoToMessage(m *pubsubpb.PubsubMessage) (*entities.Message, error) {
	msg := new(entities.Message)
	if err := msg.SetData(m.Data); err != nil {
		return nil, err
	}
	if err := msg.SetAttributes(m.Attributes); err != nil {
		return nil, err
	}
	if err := msg.SetOrderingKey(m.OrderingKey); err != nil {
		return nil, err
	}
	return msg, nil
}
