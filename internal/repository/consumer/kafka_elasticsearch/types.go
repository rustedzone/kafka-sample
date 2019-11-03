package kafkaelasticsearch

import (
	"time"

	"github.com/Shopify/sarama"
	elastic "gopkg.in/olivere/elastic.v6"
)

type (
	// ConsumerRepo repo producer with kafka & elastic adapter
	ConsumerRepo struct {
		consumerGroup sarama.ConsumerGroup
		fn            fnAttr
		bulk          BulkAttr
		es            elasticSearchAttr
	}

	elasticSearchAttr struct {
		client    *elastic.Client
		index     string
		typeIndex string
	}

	fnAttr struct {
		pushBulk    func(bulkService *elastic.BulkService, countMessage int) (err error)
		bulkDo      func(bulk *elastic.BulkService, lenAction int) (err error)
		bulkHandler func(session sarama.ConsumerGroupSession,
			bulkService *elastic.BulkService,
			bulkOffset map[string]map[int32]int64,
			msgChan <-chan *sarama.ConsumerMessage,
			ticker *time.Ticker,
			timeStamp time.Time) (okBreak bool,
			bulkservice *elastic.BulkService,
			bulkoffset map[string]map[int32]int64,
			tick *time.Ticker,
			flushTimeStamp time.Time,
			err error)
	}

	// InitOption option to init repository
	InitOption struct {
		EsClient   *elastic.Client
		Index      string
		TypeIndex  string
		Bulk       BulkAttr
		BrokerList []string
		GroupName  string
		UseLog     bool
	}

	// BulkAttr bulk attribute
	BulkAttr struct {
		Min             int
		Max             int
		Hop             int
		WaitTimeMS      int
		WaitTimeTotalMS int
		actual          int
	}
)
