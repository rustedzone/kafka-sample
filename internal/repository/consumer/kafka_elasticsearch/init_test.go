package kafkaelasticsearch

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	repo "github.com/rustedzone/kafka-sample/internal/repository/consumer"
	"github.com/stretchr/testify/assert"
	elasticV6 "gopkg.in/olivere/elastic.v6"
)

func TestNewRepoConsumer(t *testing.T) {
	type args struct {
		opt InitOption
	}
	tests := []struct {
		name                 string
		args                 args
		want                 repo.Repository
		wantPanic            bool
		mockNewConsumerGroup func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)
	}{
		{
			name: "positive case-1: success create new repository",
			args: args{
				opt: InitOption{
					BrokerList: []string{
						"localhost:9092",
						"localhost:9093",
					},
					UseLog: true,
					Bulk: BulkAttr{
						Hop:             10,
						Max:             500,
						Min:             600,
						WaitTimeMS:      250,
						WaitTimeTotalMS: 200,
					},
					EsClient:  &elasticV6.Client{},
					Index:     "someIndex",
					TypeIndex: "someTypeIndex",
				},
			},
			mockNewConsumerGroup: func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, nil
			},
		},
		{
			name: "negative case-1: error while creating new consumer group",
			args: args{
				opt: InitOption{
					BrokerList: []string{
						"localhost:9092",
						"localhost:9093",
					},
					UseLog: true,
					Bulk: BulkAttr{
						Hop:             10,
						Max:             500,
						Min:             600,
						WaitTimeMS:      250,
						WaitTimeTotalMS: 200,
					},
					EsClient:  &elasticV6.Client{},
					Index:     "someIndex",
					TypeIndex: "someTypeIndex",
				},
			},
			mockNewConsumerGroup: func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, errors.New("some error")
			},
			wantPanic: true,
		},
		{
			name: "negative case-2: got empty broker list",
			args: args{
				opt: InitOption{
					BrokerList: []string{},
					UseLog:     true,
					Bulk: BulkAttr{
						Hop:             10,
						Max:             500,
						Min:             600,
						WaitTimeMS:      250,
						WaitTimeTotalMS: 200,
					},
					EsClient:  &elasticV6.Client{},
					Index:     "someIndex",
					TypeIndex: "someTypeIndex",
				},
			},
			mockNewConsumerGroup: func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, nil
			},
			wantPanic: true,
		},
		{
			name: "negative case-3: receive nil es client from param",
			args: args{
				opt: InitOption{
					BrokerList: []string{},
					UseLog:     true,
					Bulk: BulkAttr{
						Hop:             10,
						Max:             500,
						Min:             600,
						WaitTimeMS:      250,
						WaitTimeTotalMS: 200,
					},
					EsClient:  nil,
					Index:     "someIndex",
					TypeIndex: "someTypeIndex",
				},
			},
			mockNewConsumerGroup: func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error) {
				return nil, nil
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fnNewConsumerGroup = tt.mockNewConsumerGroup
			if tt.wantPanic {
				assert.Panics(t, func() {
					NewRepoConsumer(tt.args.opt)
				})
			} else {
				got := NewRepoConsumer(tt.args.opt)
				assert.NotNil(t, got)
			}
		})
	}
}
