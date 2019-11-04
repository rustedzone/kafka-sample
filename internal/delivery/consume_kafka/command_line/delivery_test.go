package commandline

import (
	"testing"

	uConsumeKafka "github.com/rustedzone/kafka-sample/internal/usecase/consume_kafka"
)

func Test_validateFlag(t *testing.T) {
	type args struct {
		f flagAttr
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "postiive case-1:valid flag",
			args: args{
				f: flagAttr{
					broker:    "someBroker",
					esHost:    "someESHost",
					esIndex:   "someEsIndex",
					esType:    "someEsType",
					groupName: "someGroupName",
					topic:     "someTopic",
				},
			},
		},
		{
			name: "negative case-1:missing topic",
			args: args{
				f: flagAttr{
					broker:    "someBroker",
					esHost:    "someESHost",
					esIndex:   "someEsIndex",
					esType:    "someEsType",
					groupName: "someGroupName",
					topic:     "",
				},
			},
			wantErr: true,
		},
		{
			name: "negative case-2:missing es type",
			args: args{
				f: flagAttr{
					broker:    "someBroker",
					esHost:    "someESHost",
					esIndex:   "someEsIndex",
					esType:    "",
					groupName: "someGroupName",
					topic:     "someTopic",
				},
			},
			wantErr: true,
		},
		{
			name: "negative case-2:missing es index",
			args: args{
				f: flagAttr{
					broker:    "someBroker",
					esHost:    "someESHost",
					esType:    "someEsType",
					groupName: "someGroupName",
					topic:     "someTopic",
				},
			},
			wantErr: true,
		},
		{
			name: "negative case-3:missings es host",
			args: args{
				f: flagAttr{
					broker:    "someBroker",
					esType:    "someEsType",
					groupName: "someGroupName",
					topic:     "someTopic",
				},
			},
			wantErr: true,
		},
		{
			name: "negative case-4:missings kafka group name",
			args: args{
				f: flagAttr{
					broker: "someBroker",
				},
			},
			wantErr: true,
		},
		{
			name: "negative case-5:missings kafka broker address",
			args: args{
				f: flagAttr{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateFlag(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("validateFlag() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeliver(t *testing.T) {
	tests := []struct {
		name                         string
		mockFnNewUsecaseConsumeKafka func(f flagAttr) uConsumeKafka.Usecase
		mockFlag                     flagAttr
	}{
		{
			name: "positive case-1: command line delivery running well",
			mockFnNewUsecaseConsumeKafka: func(f flagAttr) uConsumeKafka.Usecase {
				return &uConsumeKafka.UsecaseMock{
					ConsumeAndStoreToESFunc: func(string) {
						return
					},
				}
			},
			mockFlag: flagAttr{
				broker:    "someBroker",
				esHost:    "someESHost",
				esIndex:   "someEsIndex",
				esType:    "someEsType",
				groupName: "someGroupName",
				topic:     "someTopic",
			},
		},
		{
			name:     "negative case-1: invalid flag",
			mockFlag: flagAttr{},
		},
	}
	for _, tt := range tests {
		fnNewUsecaseConsumeKafka = tt.mockFnNewUsecaseConsumeKafka
		f = tt.mockFlag
		t.Run(tt.name, func(t *testing.T) {
			Deliver()
		})
	}
}
