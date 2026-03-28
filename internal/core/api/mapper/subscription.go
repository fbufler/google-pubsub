package mapper

import (
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
)

func ProtoToSubscription(p *pubsubpb.Subscription) (*entities.Subscription, error) {
	sub := new(entities.Subscription)

	if err := sub.SetName(types.FQDN(p.Name)); err != nil {
		return nil, err
	}
	if err := sub.SetTopicName(types.FQDN(p.Topic)); err != nil {
		return nil, err
	}
	if len(p.Labels) > 0 {
		if err := sub.SetLabels(types.Labels(p.Labels)); err != nil {
			return nil, err
		}
	}

	ackDeadline := 10 * time.Second
	if p.AckDeadlineSeconds > 0 {
		ackDeadline = time.Duration(p.AckDeadlineSeconds) * time.Second
	}
	if err := sub.SetAckDeadline(ackDeadline); err != nil {
		return nil, err
	}

	if err := sub.SetRetainAckedMessages(p.RetainAckedMessages); err != nil {
		return nil, err
	}

	msgRetention := 7 * 24 * time.Hour
	if p.MessageRetentionDuration != nil {
		msgRetention = p.MessageRetentionDuration.AsDuration()
	}
	if err := sub.SetMessageRetention(msgRetention); err != nil {
		return nil, err
	}

	if err := sub.SetFilter(p.Filter); err != nil {
		return nil, err
	}
	if err := sub.SetEnableMessageOrdering(p.EnableMessageOrdering); err != nil {
		return nil, err
	}
	if err := sub.SetEnableExactlyOnceDelivery(p.EnableExactlyOnceDelivery); err != nil {
		return nil, err
	}

	if p.DeadLetterPolicy != nil {
		if err := sub.SetDeadLetterPolicy(&types.DeadLetterPolicy{
			DeadLetterTopic:     p.DeadLetterPolicy.DeadLetterTopic,
			MaxDeliveryAttempts: p.DeadLetterPolicy.MaxDeliveryAttempts,
		}); err != nil {
			return nil, err
		}
	}
	if p.RetryPolicy != nil {
		rp := &types.RetryPolicy{}
		if p.RetryPolicy.MinimumBackoff != nil {
			rp.MinimumBackoff = p.RetryPolicy.MinimumBackoff.AsDuration()
		}
		if p.RetryPolicy.MaximumBackoff != nil {
			rp.MaximumBackoff = p.RetryPolicy.MaximumBackoff.AsDuration()
		}
		if err := sub.SetRetryPolicy(rp); err != nil {
			return nil, err
		}
	}
	if p.PushConfig != nil {
		if err := sub.SetPushConfig(&types.PushConfig{
			Endpoint:   p.PushConfig.PushEndpoint,
			Attributes: p.PushConfig.Attributes,
		}); err != nil {
			return nil, err
		}
	}

	return sub, nil
}

func SubscriptionToProto(sub *entities.Subscription) *pubsubpb.Subscription {
	p := &pubsubpb.Subscription{
		Name:                      sub.Name().String(),
		Topic:                     sub.TopicName().String(),
		Labels:                    map[string]string(sub.Labels()),
		AckDeadlineSeconds:        int32(sub.AckDeadline().Seconds()),
		RetainAckedMessages:       sub.RetainAckedMessages(),
		MessageRetentionDuration:  durationpb.New(sub.MessageRetention()),
		Filter:                    sub.Filter(),
		EnableMessageOrdering:     sub.EnableMessageOrdering(),
		EnableExactlyOnceDelivery: sub.EnableExactlyOnceDelivery(),
	}
	if sub.DeadLetterPolicy() != nil {
		p.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     sub.DeadLetterPolicy().DeadLetterTopic,
			MaxDeliveryAttempts: sub.DeadLetterPolicy().MaxDeliveryAttempts,
		}
	}
	if sub.RetryPolicy() != nil {
		p.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(sub.RetryPolicy().MinimumBackoff),
			MaximumBackoff: durationpb.New(sub.RetryPolicy().MaximumBackoff),
		}
	}
	if sub.PushConfig() != nil {
		p.PushConfig = &pubsubpb.PushConfig{
			PushEndpoint: sub.PushConfig().Endpoint,
			Attributes:   sub.PushConfig().Attributes,
		}
	}
	return p
}
