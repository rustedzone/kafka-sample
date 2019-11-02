package kafkaelasticsearch

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	elasticV6 "gopkg.in/olivere/elastic.v6"
)

type (
	// ElasticMock : Represent Elastic Search Mock Data
	elasticMock struct {
		ElasticClient *elasticV6.Client
		Server        *httptest.Server
	}
)

func newMockElastic(bodyJSON *string, wantedResult string) (mockedElastic elasticMock, err error) {
	mockedServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		body, _ := ioutil.ReadAll(req.Body)
		*bodyJSON = string(body)
		if wantedResult == "" {
			wantedResult = `{"result" : "created"}`
		}

		io.WriteString(rw, wantedResult)
	}))

	mockedElastic.Server = mockedServer
	mockedElastic.ElasticClient, err = elasticV6.NewSimpleClient(elasticV6.SetURL(mockedServer.URL))
	return
}

func TestConsumerRepo_pushBulk(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
	}
	type args struct {
		bulkService  *elasticV6.BulkService
		countMessage int
	}
	mockBulkDo := func(bulkService *elasticV6.BulkService, countMessage int) error {
		if countMessage == 2 {
			return errors.New("some error")
		}
		return nil
	}

	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPanic bool
	}{
		{
			name: "positive case-1:sucessfully push product",
			fields: fields{
				fn: fnAttr{
					bulkDo: mockBulkDo,
				},
			},
		},
		{
			name: "negative case-1:sucessfully push product",
			args: args{
				countMessage: 2,
			},
			fields: fields{
				fn: fnAttr{
					bulkDo: mockBulkDo,
				},
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
				bulk:          tt.fields.bulk,
			}
			if tt.wantPanic {
				assert.Panics(t, func() {
					repo.pushBulk(tt.args.bulkService, tt.args.countMessage)
				})
			} else {
				if err := repo.pushBulk(tt.args.bulkService, tt.args.countMessage); (err != nil) != tt.wantErr {
					t.Errorf("ConsumerRepo.pushBulk() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestConsumerRepo_bulkDo(t *testing.T) {
	type fields struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
		es            elasticSearchAttr
	}
	type args struct {
		bulk      *elasticV6.BulkService
		lenAction int
	}

	var bodyESReceived string
	dummyES, err := newMockElastic(&bodyESReceived, "")
	if err != nil {
		t.Errorf("NewESRepoProductSearch.PushToElasticSearch error %v", err)
	}

	var bodyESError string
	dummyESFail, err := newMockElastic(&bodyESError,
		`{
			  "took": 1,
			  "errors": true,
			  "items": [
			    {
			      "update": {
			        "_index": "product_v17",
			        "_type": "default",
			        "_id": "1",
			        "status": 429,
			        "error": {
			          "type": "should_error",
			          "reason": "[default][1]: should error dude",
			          "index_uuid": "G19zXiPORS2__4-Nkc2G2Q",
			          "shard": "0",
					  "index": "product_v17",
					  "caused_by":{
						  "reason":"the cause that make this should error"
					  }
			        }
			      }
			    }
			  ]
			}`)
	if err != nil {
		t.Errorf("NewESRepoProductSearch.PushToElasticSearch error %v", err)
	}

	tests := []struct {
		name       string
		fields     fields
		args       args
		wantErr    bool
		mockResult string
	}{
		{
			name: "positive case-1:sucess push to elastic",
			args: args{
				bulk: dummyES.ElasticClient.Bulk().Add(elasticV6.NewBulkIndexRequest().
					Index("some_index").
					Type("_doc").
					Id("someID").
					Doc(`{"some":"doc"}`)),
			},
		},
		{
			name: "positive case-2: no document to push",
			args: args{
				bulk: dummyES.ElasticClient.Bulk(),
			},
		},
		{
			name: "negative case-1: got unexpected error from elastic",
			args: args{
				bulk: dummyESFail.ElasticClient.Bulk().Add(elasticV6.NewBulkIndexRequest().
					Index("some_index").
					Type("_doc").
					Id("someID").
					Doc(`{"some":"doc"}`)),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ConsumerRepo{
				consumerGroup: tt.fields.consumerGroup,
				fn:            tt.fields.fn,
				bulk:          tt.fields.bulk,
				es:            tt.fields.es,
			}
			if err := repo.bulkDo(tt.args.bulk, tt.args.lenAction); (err != nil) != tt.wantErr {
				t.Errorf("ConsumerRepo.bulkDo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
