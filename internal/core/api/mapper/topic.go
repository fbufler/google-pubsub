package mapper

import (
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

func TopicToProto(t *entities.Topic) (*pubsubpb.Topic, error) {
	labels := make(map[string]string, len(t.Labels()))
	for k, v := range t.Labels() {
		labels[k] = v
	}

	pt := new(pubsubpb.Topic)
	pt.Name = t.Name().String()
	pt.Labels = labels
	pt.KmsKeyName = t.KmsKeyName().String()
	pt.MessageRetentionDuration = durationpb.New(t.MessageRetention())
	return pt, nil
}

func ProtoToTopic(pt *pubsubpb.Topic) (*entities.Topic, error) {
	t := entities.NewTopic()
	if err := t.SetName(types.FQDN(pt.Name)); err != nil {
		return nil, err
	}

	if err := t.SetLabels(pt.Labels); err != nil {
		return nil, err
	}

	if err := t.SetKmsKeyName(types.CMEK(pt.KmsKeyName)); err != nil {
		return nil, err
	}

	if pt.MessageRetentionDuration != nil {
		if err := t.SetMessageRetention(pt.MessageRetentionDuration.AsDuration()); err != nil {
			return nil, err
		}
	}
	return t, nil
}
