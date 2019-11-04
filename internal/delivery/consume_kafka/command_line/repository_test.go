package commandline

import (
	"errors"
	"testing"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
	rConsumerKafkaES "github.com/rustedzone/kafka-sample/internal/repository/consumer/kafka_elasticsearch"
	"github.com/stretchr/testify/assert"
	elastic "gopkg.in/olivere/elastic.v6"
)

func Test_newRepoConsumer(t *testing.T) {
	type args struct {
		f flagAttr
	}
	tests := []struct {
		name                         string
		args                         args
		wantPanic                    bool
		mockFnElasticNewClient       func(opt ...elastic.ClientOptionFunc) (*elastic.Client, error)
		mockFnNewRepoConsumerKafkaES func(opt rConsumerKafkaES.InitOption) rConsumer.Repository
	}{
		{
			name: "positive case-1:succes creating repository",
			mockFnElasticNewClient: func(opt ...elastic.ClientOptionFunc) (*elastic.Client, error) {
				return &elastic.Client{}, nil
			},
			mockFnNewRepoConsumerKafkaES: func(opt rConsumerKafkaES.InitOption) rConsumer.Repository {
				return &rConsumer.RepositoryMock{}
			},
		},
		{
			name: "negative case-1:fail creating new elastic client",
			mockFnElasticNewClient: func(opt ...elastic.ClientOptionFunc) (*elastic.Client, error) {
				return &elastic.Client{}, errors.New("some error")
			},
			mockFnNewRepoConsumerKafkaES: func(opt rConsumerKafkaES.InitOption) rConsumer.Repository {
				return &rConsumer.RepositoryMock{}
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fnElasticNewClient = tt.mockFnElasticNewClient
			fnConsumerKafkaESNewRepo = tt.mockFnNewRepoConsumerKafkaES

			if tt.wantPanic {
				assert.Panics(t, func() {
					newRepoConsumer(tt.args.f)
				})
			} else {
				got := newRepoConsumer(tt.args.f)
				assert.NotNil(t, got)
			}
		})
	}
}
