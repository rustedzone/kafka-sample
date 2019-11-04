package consumekafkaproduct

import (
	"testing"

	"github.com/stretchr/testify/assert"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
)

func TestNewUseCase(t *testing.T) {
	type args struct {
		rConsumerKafkaES rConsumer.Repository
	}
	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "positive case-1:success create usecase",
			args: args{
				rConsumerKafkaES: &rConsumer.RepositoryMock{},
			},
		},
		{
			name: "negative case-1:missing repo consumer",
			args: args{
				rConsumerKafkaES: nil,
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				assert.Panics(t, func() {
					NewUseCase(tt.args.rConsumerKafkaES)
				})
			} else {
				got := NewUseCase(tt.args.rConsumerKafkaES)
				assert.NotNil(t, got)
			}

		})
	}
}
