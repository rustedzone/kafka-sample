// github.com/matryer/moq

package consumekafkaproduct

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUsecaseMock_ConsumeAndStoreToESCalls(t *testing.T) {
	type fields struct {
		ConsumeAndStoreToESFunc func(topic string)
		calls                   struct{ ConsumeAndStoreToES []struct{ Topic string } }
	}
	tests := []struct {
		name   string
		fields fields
		want   []struct{ Topic string }
	}{
		{
			name: "do nothing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &UsecaseMock{
				ConsumeAndStoreToESFunc: tt.fields.ConsumeAndStoreToESFunc,
				calls:                   tt.fields.calls,
			}
			if got := mock.ConsumeAndStoreToESCalls(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UsecaseMock.ConsumeAndStoreToESCalls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUsecaseMock_ConsumeAndStoreToES(t *testing.T) {
	type fields struct {
		ConsumeAndStoreToESFunc func(topic string)
		calls                   struct{ ConsumeAndStoreToES []struct{ Topic string } }
	}
	type args struct {
		topic string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
	}{
		{
			name: "positive case-1: sucess call",
			fields: fields{
				ConsumeAndStoreToESFunc: func(string) {
					return
				},
			},
		},
		{
			name:      "negative case-1: panic",
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &UsecaseMock{
				ConsumeAndStoreToESFunc: tt.fields.ConsumeAndStoreToESFunc,
				calls:                   tt.fields.calls,
			}
			if tt.wantPanic {
				assert.Panics(t, func() {
					mock.ConsumeAndStoreToES(tt.args.topic)
				})
			} else {
				mock.ConsumeAndStoreToES(tt.args.topic)
			}
		})
	}
}
