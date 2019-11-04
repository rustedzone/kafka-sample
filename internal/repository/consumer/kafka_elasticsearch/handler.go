package kafkaelasticsearch

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rustedzone/kafka-sample/common"
	elastic "gopkg.in/olivere/elastic.v6"
)

// Setup is run at the beginning of a new session, before ConsumeClaim
func (repo *ConsumerRepo) Setup(session sarama.ConsumerGroupSession) error {
	log.Println("setup consumer kafka")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (repo *ConsumerRepo) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("clean up consumer kafka")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (repo *ConsumerRepo) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	return repo.bulkConsume(session, claim)
}

func (repo *ConsumerRepo) bulkConsume(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	msgChan := claim.Messages()
	ticker := time.NewTicker(time.Duration(repo.bulk.WaitTimeMS) * time.Millisecond)

	// map of topic of partition of kafka message
	bulkOffset := make(map[string]map[int32]int64)
	bulkService := repo.es.client.Bulk()
	timeStamp := time.Now()
	isBreak := false

	for {
		if isBreak, bulkService, bulkOffset, ticker, timeStamp, err = repo.fn.bulkHandler(session, bulkService, bulkOffset, msgChan, ticker, timeStamp); isBreak {
			break
		}
	}
	return
}

func (repo *ConsumerRepo) bulkHandler(session sarama.ConsumerGroupSession,
	bulkService *elastic.BulkService,
	bulkOffset map[string]map[int32]int64,
	msgChan <-chan *sarama.ConsumerMessage,
	ticker *time.Ticker,
	timeStamp time.Time) (okBreak bool,
	bulkservice *elastic.BulkService,
	bulkoffset map[string]map[int32]int64,
	tick *time.Ticker,
	flushTimeStamp time.Time,
	err error) {
	select {
	case msg := <-msgChan:
		// msg == nil means connection to kafka is closed
		// rebalancing also send nil value since kafka will close all consumer connection for rebalancing
		// done consuming immediately
		if msg == nil {
			return true, bulkService, bulkOffset, ticker, timeStamp, nil
		}

		// partition
		if bulkOffset == nil {
			bulkOffset = make(map[string]map[int32]int64)
		}
		if bulkOffset[msg.Topic] == nil {
			bulkOffset[msg.Topic] = make(map[int32]int64)
		}
		bulkOffset[msg.Topic][msg.Partition] = msg.Offset

		// validate message
		if doc, action, id, err := prepareDoc(msg.Value); err == nil {
			// build service
			switch action {
			case common.ActionCreate:
				repo.appendBulkCreate(bulkService, id, doc)
			case common.ActionIndex:
				repo.appendBulkIndex(bulkService, id, doc)
			case common.ActionUpdate:
				repo.appendBulkUpdate(bulkService, id, doc)
			case common.ActionDelete:
				repo.appendBulkDelete(bulkService, id)
			}

			// will push to elastic, if the bulk size reached --
			// or the bulk is already wait for defined duration since the last time it pushed
			enoughWaiting := int(time.Since(timeStamp).Seconds()*1000) >= repo.bulk.WaitTimeTotalMS
			if bulkService.NumberOfActions() >= repo.bulk.actual || enoughWaiting {
				if enoughWaiting {
					repo.setBulkHop(false, bulkService.NumberOfActions())
				} else {
					repo.setBulkHop(true, bulkService.NumberOfActions())
				}
				if err = repo.fn.pushBulk(bulkService, bulkService.NumberOfActions()); err != nil {
					return true, bulkService, bulkOffset, ticker, timeStamp, err
				}
				bulkOffset = repo.markOffsetAndReset(session, bulkOffset)
				bulkService.Reset()

				// timestamp need to be getting reset when the bulk is flushed
				timeStamp = time.Now()
			}
		} else {
			log.Println("[bulkHandler] prepare doc:", err)
		}

		// reset ticker
		// to prevent memory leak need to stop the old ticker before reset
		ticker.Stop()
		ticker = time.NewTicker(time.Duration(repo.bulk.WaitTimeMS) * time.Millisecond)

	case <-ticker.C:
		repo.setBulkHop(false, bulkService.NumberOfActions())
		if err = repo.fn.pushBulk(bulkService, bulkService.NumberOfActions()); err != nil {
			return true, bulkService, bulkOffset, ticker, timeStamp, err
		}

		bulkOffset = repo.markOffsetAndReset(session, bulkOffset)
		bulkService.Reset()

		// timestamp need to be getting reset when the bulk is flushed
		timeStamp = time.Now()

		// reset ticker
		// to prevent memory leak need to stop the old ticker before reset
		ticker.Stop()
		ticker = time.NewTicker(time.Duration(repo.bulk.WaitTimeMS) * time.Millisecond)
	}

	return false, bulkService, bulkOffset, ticker, timeStamp, err
}

func (repo *ConsumerRepo) appendBulkCreate(bulkService *elastic.BulkService, id string, doc json.RawMessage) {
	bulkService.Add(elastic.NewBulkIndexRequest().
		OpType("create").
		Index(repo.es.index).
		Type(repo.es.typeIndex).
		Id(id).
		Doc(doc))
}

func (repo *ConsumerRepo) appendBulkIndex(bulkService *elastic.BulkService, id string, doc json.RawMessage) {
	bulkService.Add(elastic.NewBulkIndexRequest().
		Index(repo.es.index).
		Type(repo.es.typeIndex).
		Id(id).
		Doc(doc))
}

func (repo *ConsumerRepo) appendBulkUpdate(bulkService *elastic.BulkService, id string, doc json.RawMessage) {
	bulkService.Add(elastic.NewBulkUpdateRequest().
		Index(repo.es.index).
		Type(repo.es.typeIndex).
		Id(id).
		Doc(doc))
}

func (repo *ConsumerRepo) appendBulkDelete(bulkService *elastic.BulkService, id string) {
	bulkService.Add(elastic.NewBulkDeleteRequest().
		Index(repo.es.index).
		Type(repo.es.typeIndex).
		Id(id))
}

func (repo *ConsumerRepo) markOffsetAndReset(session sarama.ConsumerGroupSession, bulkOffset map[string]map[int32]int64) (resetBulk map[string]map[int32]int64) {
	// mark message and clear offset bulk map
	for topic, val := range bulkOffset {
		for partition, offset := range val {
			session.MarkOffset(topic, partition, offset, "")
		}
	}
	return make(map[string]map[int32]int64)
}

// hopping bulk size
func (repo *ConsumerRepo) setBulkHop(isIncrease bool, countMessage int) {
	if isIncrease {
		if repo.bulk.actual += repo.bulk.Hop; repo.bulk.actual > repo.bulk.Max {
			repo.bulk.actual = repo.bulk.Max
		}
	} else {
		if repo.bulk.actual = countMessage; repo.bulk.actual < repo.bulk.Min {
			repo.bulk.actual = repo.bulk.Min
		}
	}
}
