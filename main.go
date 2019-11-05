package main

import (
	"flag"
	"log"

	"github.com/google/gops/agent"
	deliveryConsumeKafkaCMD "github.com/rustedzone/kafka-sample/internal/delivery/consume_kafka/command_line"
)

var (
	fnDeliveryConsumeKafkaCMD func()
	fnGopsAgentList           func(agent.Options) error
)

func init() {
	// mock function
	fnDeliveryConsumeKafkaCMD = deliveryConsumeKafkaCMD.Deliver
	fnGopsAgentList = agent.Listen

	// include file name in log
	log.SetFlags(log.LstdFlags | log.Llongfile)
}

func main() {
	flag.Parse()
	if err := fnGopsAgentList(agent.Options{}); err != nil {
		log.Panic(err)
	}
	fnDeliveryConsumeKafkaCMD()
}
