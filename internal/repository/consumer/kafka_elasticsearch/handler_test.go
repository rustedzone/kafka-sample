package kafkaelasticsearch

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	elastic "gopkg.in/olivere/elastic.v6"
)

type (
	mockConsumerGroupSesssion struct{}
	mockConsumerGroupClaim    struct{}
)

// InitialOffset mock consumer group claim
func (mock mockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

// HighWaterMarkOffset mock consumer group claim
func (mock mockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 3)
	defer close(chann)
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"update","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
	}
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"update"}`),
	}
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"delete","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
	}
	return chann
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) messageCreate() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 1)
	defer close(chann)
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"create","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
	}
	return chann
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) messageIndex() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 1)
	defer close(chann)
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"index","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
	}
	return chann
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) messageDelete() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 1)
	defer close(chann)
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"delete","target":"","key":"15235258"},"body":{"id":15235258,"is_preorder":false}}`),
	}
	return chann
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) messageInvalid() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 1)
	defer close(chann)
	chann <- &sarama.ConsumerMessage{
		Value: []byte(`{"header":{"action":"`),
	}
	return chann
}

// Messages mock consumer group claim
func (mock mockConsumerGroupClaim) messageNil() <-chan *sarama.ConsumerMessage {
	chann := make(chan *sarama.ConsumerMessage, 1)
	defer close(chann)
	chann <- nil
	return chann
}

// Partition mock consumer group claim
func (mock mockConsumerGroupClaim) Partition() int32 {
	return 0
}

// Topic mock consumer group claim
func (mock mockConsumerGroupClaim) Topic() string {
	return ""
}

// Claims mock consumer group session
func (mock mockConsumerGroupSesssion) Claims() map[string][]int32 {
	return make(map[string][]int32)
}

// MemberID mock consumer group session
func (mock mockConsumerGroupSesssion) MemberID() string {
	return ""
}

// GenerationID mock consumer group session
func (mock mockConsumerGroupSesssion) GenerationID() int32 {
	return 0
}

// MarkOffset mock consumer group session
func (mock mockConsumerGroupSesssion) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

// ResetOffset mock consumer group session
func (mock mockConsumerGroupSesssion) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

// MarkMessage mock consumer group session
func (mock mockConsumerGroupSesssion) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}

// Context mock consumer group session
func (mock mockConsumerGroupSesssion) Context() context.Context {
	return context.Background()
}

func TestConsumerRepo_Cleanup(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
	}
	type args struct {
		in0 sarama.ConsumerGroupSession
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "positive case-1:do nothing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
			}
			if err := repo.Cleanup(tt.args.in0); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerRepo.Cleanup() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsumerRepo_Setup(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
	}
	type args struct {
		in0 sarama.ConsumerGroupSession
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "positive case-1:do nothing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
			}
			if err := repo.Setup(tt.args.in0); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerRepo.Setup() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsumerRepo_bulkHandler(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
	}
	type args struct {
		session     sarama.ConsumerGroupSession
		bulkService *elastic.BulkService
		bulkOffset  map[string]map[int32]int64
		msgChan     <-chan *sarama.ConsumerMessage
		ticker      *time.Ticker
		timeStamp   time.Time
	}
	tests := []struct {
		name                 string
		fields               fields
		args                 args
		wantOkBreak          bool
		wantBulkservice      *elastic.BulkService
		wantBulkoffset       map[string]map[int32]int64
		wantTick             *time.Ticker
		wantflushedTimeStamp *time.Ticker
		wantErr              bool
	}{
		{
			name: "positive case-1: triggered by ticker",
			args: args{
				bulkOffset: map[string]map[int32]int64{
					"topic": map[int32]int64{
						1: 2,
					},
				},
				ticker:      time.NewTicker(250 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      250,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-2: triggered by wait time total ms",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(2500 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.Messages(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 0,
					actual:          10,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-3: receive update message",
			args: args{
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.Messages(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-4: receive create message",
			args: args{
				bulkOffset: map[string]map[int32]int64{
					"topic": map[int32]int64{
						1: 2,
					},
				},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageCreate(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-5: receive index message",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageIndex(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-6: receive Delete message",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageDelete(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "positive case-7: collect message",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageDelete(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return nil
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
					actual:          10,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{
				"": map[int32]int64{
					0: 0,
				},
			},
		},
		{
			name: "negative case-1: triggered by ticker total and failed to push",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(2500 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return errors.New("some error")
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 250,
				},
			},
			wantOkBreak:    true,
			wantErr:        true,
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "negative case-2: triggered by ticker and failed to push",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(250 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return errors.New("some error")
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      250,
					WaitTimeTotalMS: 2500,
				},
			},
			wantOkBreak:    true,
			wantErr:        true,
			wantBulkoffset: map[string]map[int32]int64{},
		},
		{
			name: "negative case-3: receive message and fail to push",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.Messages(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return errors.New("some error")
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantOkBreak: true,
			wantErr:     true,
			wantBulkoffset: map[string]map[int32]int64{
				"": map[int32]int64{
					0: 0,
				},
			},
		},
		{
			name: "negative case-4: receive invalid message",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageInvalid(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return errors.New("some error")
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantBulkoffset: map[string]map[int32]int64{
				"": map[int32]int64{
					0: 0,
				},
			},
		},
		{
			name: "negative case-5: receive nil message",
			args: args{
				bulkOffset:  map[string]map[int32]int64{},
				ticker:      time.NewTicker(25000 * time.Millisecond),
				bulkService: &elastic.BulkService{},
				session:     mockConsumerGroupSesssion{},
				msgChan:     mockConsumerGroupClaim{}.messageNil(),
			},
			fields: fields{
				fn: fnAttr{
					pushBulk: func(*elastic.BulkService, int) error {
						return errors.New("some error")
					},
				},
				bulk: BulkAttr{
					WaitTimeMS:      2500,
					WaitTimeTotalMS: 2500,
				},
			},
			wantOkBreak:    true,
			wantBulkoffset: map[string]map[int32]int64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
				bulk:          tt.fields.bulk,
			}
			gotOkBreak, gotBulkservice, gotBulkoffset, _, _, err := repo.bulkHandler(tt.args.session, tt.args.bulkService, tt.args.bulkOffset, tt.args.msgChan, tt.args.ticker, time.Now())
			if (gotOkBreak) != tt.wantOkBreak {
				t.Errorf("ConsumerRepo.bulkHandler() okbreak = %+v, wantbreak %+v", gotOkBreak, tt.wantOkBreak)
				return
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsumerRepo.bulkHandler() err = %+v, wanterr %+v", err, tt.wantErr)
				return
			}

			if gotOkBreak != tt.wantOkBreak {
				t.Errorf("ConsumerRepo.bulkHandler() gotOkBreak = %+v, want %+v", gotOkBreak, tt.wantOkBreak)
			}
			if tt.wantBulkservice = tt.args.bulkService; !reflect.DeepEqual(gotBulkservice.NumberOfActions(), tt.wantBulkservice.NumberOfActions()) {
				t.Errorf("ConsumerRepo.bulkHandler() gotBulkservice = %+v, want %+v", gotBulkservice.NumberOfActions(), tt.wantBulkservice.NumberOfActions())
			}
			if !reflect.DeepEqual(gotBulkoffset, tt.wantBulkoffset) {
				t.Errorf("ConsumerRepo.bulkHandler() gotBulkoffset = %+v, want %+v", gotBulkoffset, tt.wantBulkoffset)
			}
			// if tt.wantTick = tt.args.ticker; !reflect.DeepEqual(gotTick, tt.wantTick) {
			// 	t.Errorf("ConsumerRepo.bulkHandler() gotTick = %+v, want %+v", gotTick, tt.wantTick)
			// }
			// if tt.wantflushedTimeStamp = tt.args.timeStamp; !reflect.DeepEqual(gotflushedTimeStamp, tt.wantflushedTimeStamp) {
			// 	t.Errorf("ConsumerRepo.bulkHandler() gotflushedTimeStamp = %+v, want %+v", gotflushedTimeStamp, tt.wantflushedTimeStamp)
			// }
		})
	}
}

func TestConsumerRepo_setBulkHop(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
	}
	type args struct {
		isIncrease   bool
		countMessage int
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantActual int
	}{
		{
			name: "positive case-1: hop higher than maximum bulk",
			fields: fields{
				bulk: BulkAttr{
					Hop: 100000,
					Max: 10,
				},
			},
			args: args{
				isIncrease: true,
			},
			wantActual: 10,
		},
		{
			name: "positive case-2: hop lower than maximum bulk",
			fields: fields{
				bulk: BulkAttr{
					Min: 10,
				},
			},
			args: args{
				isIncrease:   false,
				countMessage: 9,
			},
			wantActual: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
				bulk:          tt.fields.bulk,
			}
			repo.setBulkHop(tt.args.isIncrease, tt.args.countMessage)
			assert.Equal(t, tt.wantActual, repo.bulk.actual)
		})
	}
}

func TestConsumerRepo_bulkConsume(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
	}
	type args struct {
		session sarama.ConsumerGroupSession
		claim   sarama.ConsumerGroupClaim
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "positive case-1: breaking from infinite loop",
			fields: fields{
				bulk: BulkAttr{
					WaitTimeMS:      10,
					WaitTimeTotalMS: 100,
				},
				fn: fnAttr{
					bulkHandler: func(session sarama.ConsumerGroupSession,
						bulkService *elastic.BulkService,
						bulkOffset map[string]map[int32]int64,
						msgChan <-chan *sarama.ConsumerMessage,
						ticker *time.Ticker,
						timeStamp time.Time) (okBreak bool,
						bulkservice *elastic.BulkService,
						bulkoffset map[string]map[int32]int64,
						tick *time.Ticker,
						flushedTimeStamp time.Time,
						err error) {
						return true, bulkService, bulkoffset, ticker, timeStamp, err
					},
				},
			},
			args: args{
				claim:   mockConsumerGroupClaim{},
				session: mockConsumerGroupSesssion{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
				bulk:          tt.fields.bulk,
			}
			if err := repo.bulkConsume(tt.args.session, tt.args.claim); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerRepo.bulkConsume() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
