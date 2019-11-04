package commandline

import (
	"log"
	"strings"

	rConsumer "github.com/rustedzone/kafka-sample/internal/repository/consumer"
	rConsumerKafkaES "github.com/rustedzone/kafka-sample/internal/repository/consumer/kafka_elasticsearch"
	elastic "gopkg.in/olivere/elastic.v6"
)

func newRepoConsumer(f flagAttr) rConsumer.Repository {
	var (
		esClient *elastic.Client
		err      error
	)
	// elastic search
	if esClient, err = fnElasticNewClient(
		elastic.SetURL(f.esHost),
		elastic.SetMaxRetries(10),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false)); err != nil {
		log.Panic("Fail to create elastic client :", err)
	}

	brokerList := strings.Split(f.broker, ",")
	return fnConsumerKafkaESNewRepo(rConsumerKafkaES.InitOption{
		BrokerList: brokerList,
		GroupName:  f.groupName,
		UseLog:     f.saramalog,

		EsClient:  esClient,
		Index:     f.esIndex,
		TypeIndex: f.esType,
	})
}
