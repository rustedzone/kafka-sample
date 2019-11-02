package kafkaelasticsearch

import (
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	repo "github.com/rustedzone/kafka-sample/internal/repository/consumer"
)

var (
	defaultBulk = BulkAttr{
		Hop:             100,
		Max:             1000,
		Min:             100,
		WaitTimeMS:      250,  // millisecond
		WaitTimeTotalMS: 1000, // millisecond
	}

	// mocked function
	fnNewConsumerGroup func(addrs []string, groupID string, config *sarama.Config) (sarama.ConsumerGroup, error)
)

const (
	errTypeDocumentMissingException string = "document_missing_exception"
	errReasonDocumentExist          string = "document already exists"
)

func init() {
	fnNewConsumerGroup = sarama.NewConsumerGroup
}

// NewRepoConsumer new repo consumer with kafka and es adapter
func NewRepoConsumer(opt InitOption) repo.Repository {
	if opt.EsClient == nil || opt.Index == "" || opt.TypeIndex == "" {
		log.Panic("[NewRepoConsumer] bad init")
	}

	cg, err := NewConsumerGroup(opt.BrokerList, opt.GroupName, opt.Index, opt.UseLog)
	if err != nil {
		log.Panic("[NewRepoConsumer] new consumer group:", err)
	}

	new := &ConsumerRepo{
		consumerGroup: cg,
		es: elasticSearchAttr{
			client:    opt.EsClient,
			index:     opt.Index,
			typeIndex: opt.TypeIndex,
		},
	}
	new.setFn()
	new.setBulk(opt.Bulk)
	return new
}

// NewConsumerGroup create new object of consumer
func NewConsumerGroup(brokerList []string, groupName, clientID string, useLog bool) (sarama.ConsumerGroup, error) {
	if useLog {
		sarama.Logger = log.New(os.Stdout, "[kafka-consumer]", log.LstdFlags)
	}
	if err := checkInit(brokerList); err != nil {
		return nil, err
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_3_0_0
	cfg.Consumer.MaxWaitTime = 5 * time.Second
	cfg.Consumer.MaxProcessingTime = 5 * time.Second
	cfg.Consumer.Fetch.Default = 1024 * 10
	cfg.ClientID = clientID + "-" + strconv.FormatInt(time.Now().Unix(), 10)
	cg, err := fnNewConsumerGroup(brokerList, groupName, cfg)
	if err != nil {
		return nil, err
	}

	return cg, nil
}

func checkInit(brokerList []string) (err error) {
	if len(brokerList) < 1 {
		return errors.New("[checkProducerInit]broker list is empty")
	}
	return nil
}

func (repo *ConsumerRepo) setFn() {
	repo.fn = fnAttr{
		pushBulk:    repo.pushBulk,
		bulkDo:      repo.bulkDo,
		bulkHandler: repo.bulkHandler,
	}
}

func (repo *ConsumerRepo) setBulk(prop BulkAttr) {
	bulk := defaultBulk
	if prop.Hop != 0 {
		bulk.Hop = prop.Hop
	}
	if prop.Max != 0 {
		bulk.Max = prop.Max
	}
	if prop.Min != 0 {
		bulk.Min = prop.Min
	}
	if prop.WaitTimeMS != 0 {
		bulk.WaitTimeMS = prop.WaitTimeMS
	}
	if prop.WaitTimeTotalMS != 0 {
		bulk.WaitTimeTotalMS = prop.WaitTimeTotalMS
	}
	if bulk.WaitTimeMS > bulk.WaitTimeTotalMS {
		bulk.WaitTimeMS, bulk.WaitTimeTotalMS = bulk.WaitTimeTotalMS, bulk.WaitTimeMS
	}
	if bulk.Max < bulk.Min {
		bulk.Max, bulk.Min = bulk.Min, bulk.Max
	}
	bulk.actual = bulk.Min
	repo.bulk = bulk
}
