// github.com/matryer/moq

package consumer

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRepositoryMock_ConsumeCalls(t *testing.T) {
	type fields struct {
		ConsumeFunc func(topic string)
		calls       struct{ Consume []struct{ Topic string } }
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
			mock := &RepositoryMock{
				ConsumeFunc: tt.fields.ConsumeFunc,
				calls:       tt.fields.calls,
			}
			if got := mock.ConsumeCalls(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RepositoryMock.ConsumeCalls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepositoryMock_Consume(t *testing.T) {
	type fields struct {
		ConsumeFunc func(topic string)
		calls       struct{ Consume []struct{ Topic string } }
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
			name: "positive case-1:success call mock function",
			fields: fields{
				ConsumeFunc: func(string) {
					return
				},
			},
		},
		{
			name:      "negative case-1:panic",
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &RepositoryMock{
				ConsumeFunc: tt.fields.ConsumeFunc,
				calls:       tt.fields.calls,
			}
			if tt.wantPanic {
				assert.Panics(t, func() {
					mock.Consume(tt.args.topic)
				})
			} else {
				mock.Consume(tt.args.topic)
			}
		})
	}
}
