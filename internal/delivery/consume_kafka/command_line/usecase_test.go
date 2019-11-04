package commandline

import (
	"testing"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
	uConsumeKafka "github.com/rustedzone/kafka-sample/internal/usecase/consume_kafka"
	"github.com/stretchr/testify/assert"
)

func Test_newUsecaseConsumeKafka(t *testing.T) {
	type args struct {
		f flagAttr
	}
	tests := []struct {
		name                         string
		args                         args
		mockFnNewRepoConsumer        func(f flagAttr) rConsumer.Repository
		mockFnConsumeKafkaNewUsecase func(rConsumer.Repository) uConsumeKafka.Usecase
		wantPanic                    bool
	}{
		{
			name: "positive case-1:success creating new usecase for consume kafka",
			mockFnNewRepoConsumer: func(flagAttr) rConsumer.Repository {
				return &rConsumer.RepositoryMock{}
			},
			mockFnConsumeKafkaNewUsecase: func(rConsumer.Repository) uConsumeKafka.Usecase {
				return &uConsumeKafka.UsecaseMock{}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fnNewRepoConsumer = tt.mockFnNewRepoConsumer
			fnConsumeKafkaNewUsecase = tt.mockFnConsumeKafkaNewUsecase
			if tt.wantPanic {
				assert.Panics(t, func() {
					newUsecaseConsumeKafka(tt.args.f)
				})
			} else {
				got := newUsecaseConsumeKafka(tt.args.f)
				assert.NotNil(t, got)
			}
		})
	}
}
