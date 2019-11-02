package kafkaelasticsearch

import (
	"time"

	"github.com/Shopify/sarama"
	elasticV6 "gopkg.in/olivere/elastic.v6"
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
		client    *elasticV6.Client
		index     string
		typeIndex string
	}

	fnAttr struct {
		pushBulk    func(bulkService *elasticV6.BulkService, countMessage int) (err error)
		bulkDo      func(bulk *elasticV6.BulkService, lenAction int) (err error)
		bulkHandler func(session sarama.ConsumerGroupSession,
			bulkService *elasticV6.BulkService,
			bulkOffset map[string]map[int32]int64,
			msgChan <-chan *sarama.ConsumerMessage,
			ticker, tickerTotal *time.Ticker) (okBreak bool,
			bulkservice *elasticV6.BulkService,
			bulkoffset map[string]map[int32]int64,
			tick, tickTotal *time.Ticker,
			err error)
	}

	// InitOption option to init repository
	InitOption struct {
		EsClient   *elasticV6.Client
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
