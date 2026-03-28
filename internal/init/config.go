// Package init provides startup configuration for pre-creating PubSub resources.
package init

import (
	"context"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/fbufler/google-pubsub/internal/core/entities"
	"github.com/fbufler/google-pubsub/internal/core/types"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
)

// Config is the top-level init configuration file format.
//
// Example:
//
//	projects:
//	  - id: my-project
//	    topics:
//	      - name: my-topic
//	        subscriptions:
//	          - name: my-sub
//	            ack_deadline_seconds: 20
type Config struct {
	Projects []ProjectConfig `yaml:"projects"`
}

type ProjectConfig struct {
	ID     string        `yaml:"id"`
	Topics []TopicConfig `yaml:"topics"`
}

type TopicConfig struct {
	Name                    string               `yaml:"name"`
	Labels                  map[string]string    `yaml:"labels"`
	MessageRetentionSeconds int64                `yaml:"message_retention_seconds"`
	Subscriptions           []SubscriptionConfig `yaml:"subscriptions"`
}

type SubscriptionConfig struct {
	Name                string            `yaml:"name"`
	Labels              map[string]string `yaml:"labels"`
	AckDeadlineSeconds  int               `yaml:"ack_deadline_seconds"`
	RetainAckedMessages bool              `yaml:"retain_acked_messages"`
	Filter              string            `yaml:"filter"`
}

// Load reads a YAML init config from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read init config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse init config %q: %w", path, err)
	}
	return &cfg, nil
}

// Apply creates all resources defined in cfg, ignoring already-existing ones.
func Apply(ctx context.Context, cfg *Config, topicUC *usecases.TopicUsecase, subUC *usecases.SubscriberUsecase) error {
	for _, proj := range cfg.Projects {
		for _, tc := range proj.Topics {
			topicName := fmt.Sprintf("projects/%s/topics/%s", proj.ID, tc.Name)

			topic := new(entities.Topic)
			if err := topic.SetName(types.FQDN(topicName)); err != nil {
				return fmt.Errorf("invalid topic name %q: %w", topicName, err)
			}
			if len(tc.Labels) > 0 {
				if err := topic.SetLabels(types.Labels(tc.Labels)); err != nil {
					return fmt.Errorf("invalid labels for topic %q: %w", topicName, err)
				}
			}
			if tc.MessageRetentionSeconds > 0 {
				if err := topic.SetMessageRetention(time.Duration(tc.MessageRetentionSeconds) * time.Second); err != nil {
					return fmt.Errorf("invalid message retention for topic %q: %w", topicName, err)
				}
			}

			if _, err := topicUC.CreateTopic(ctx, topic); err != nil && !types.IsUsecaseKind(err, types.UsecaseAlreadyExists) {
				return fmt.Errorf("create topic %q: %w", topicName, err)
			}

			for _, sc := range tc.Subscriptions {
				subName := fmt.Sprintf("projects/%s/subscriptions/%s", proj.ID, sc.Name)

				ackDeadline := 10 * time.Second
				if sc.AckDeadlineSeconds > 0 {
					ackDeadline = time.Duration(sc.AckDeadlineSeconds) * time.Second
				}

				sub := new(entities.Subscription)
				if err := sub.SetName(types.FQDN(subName)); err != nil {
					return fmt.Errorf("invalid subscription name %q: %w", subName, err)
				}
				if err := sub.SetTopicName(types.FQDN(topicName)); err != nil {
					return fmt.Errorf("invalid topic name %q: %w", topicName, err)
				}
				if len(sc.Labels) > 0 {
					if err := sub.SetLabels(types.Labels(sc.Labels)); err != nil {
						return fmt.Errorf("invalid labels for subscription %q: %w", subName, err)
					}
				}
				if err := sub.SetAckDeadline(ackDeadline); err != nil {
					return fmt.Errorf("invalid ack deadline for subscription %q: %w", subName, err)
				}
				if err := sub.SetRetainAckedMessages(sc.RetainAckedMessages); err != nil {
				return fmt.Errorf("invalid retain_acked_messages for subscription %q: %w", subName, err)
			}
				if err := sub.SetMessageRetention(7 * 24 * time.Hour); err != nil {
					return fmt.Errorf("invalid message retention for subscription %q: %w", subName, err)
				}
				if err := sub.SetFilter(sc.Filter); err != nil {
					return fmt.Errorf("invalid filter for subscription %q: %w", subName, err)
				}

				if err := subUC.CreateSubscription(ctx, sub); err != nil && !types.IsUsecaseKind(err, types.UsecaseAlreadyExists) {
					return fmt.Errorf("create subscription %q: %w", subName, err)
				}
			}
		}
	}
	return nil
}
