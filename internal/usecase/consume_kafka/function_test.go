package consumekafkaproduct

import (
	"testing"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
)

func TestUseCase_ConsumeAndStoreToES(t *testing.T) {
	type fields struct {
		repo repoAttr
	}
	type args struct {
		topic string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "positive case-1:consume from topic",
			fields: fields{
				repo: repoAttr{
					consumerKafkaES: &rConsumer.RepositoryMock{
						ConsumeFunc: func(string) {
							return
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &UseCase{
				repo: tt.fields.repo,
			}
			u.ConsumeAndStoreToES(tt.args.topic)
		})
	}
}
