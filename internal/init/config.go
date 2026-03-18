// Package init provides startup configuration for pre-creating PubSub resources.
package init

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/fbufler/google-pubsub/internal/domain"
	"github.com/fbufler/google-pubsub/internal/storage"
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

// Apply creates all resources defined in cfg into store, ignoring already-existing ones.
func Apply(cfg *Config, store *storage.Store) error {
	for _, proj := range cfg.Projects {
		for _, tc := range proj.Topics {
			topicName := fmt.Sprintf("projects/%s/topics/%s", proj.ID, tc.Name)
			topic := &domain.Topic{
				Name:      topicName,
				Labels:    tc.Labels,
				CreatedAt: time.Now(),
			}
			if tc.MessageRetentionSeconds > 0 {
				topic.MessageRetention = time.Duration(tc.MessageRetentionSeconds) * time.Second
			}
			if err := store.CreateTopic(topic); err != nil && err != domain.ErrAlreadyExists {
				return fmt.Errorf("create topic %q: %w", topicName, err)
			}

			for _, sc := range tc.Subscriptions {
				subName := fmt.Sprintf("projects/%s/subscriptions/%s", proj.ID, sc.Name)
				ackDeadline := 10 * time.Second
				if sc.AckDeadlineSeconds > 0 {
					ackDeadline = time.Duration(sc.AckDeadlineSeconds) * time.Second
				}
				sub := &domain.Subscription{
					Name:                subName,
					TopicName:           topicName,
					Labels:              sc.Labels,
					AckDeadline:         ackDeadline,
					RetainAckedMessages: sc.RetainAckedMessages,
					MessageRetention:    7 * 24 * time.Hour,
					Filter:              sc.Filter,
					CreatedAt:           time.Now(),
				}
				if err := store.CreateSubscription(sub); err != nil && err != domain.ErrAlreadyExists {
					return fmt.Errorf("create subscription %q: %w", subName, err)
				}
			}
		}
	}
	return nil
}
