package main

import (
	"flag"
	"log"

	"github.com/google/gops/agent"
	deliveryConsumeKafkaCMD "github.com/rustedzone/kafka-sample/internal/delivery/consume_kafka/command_line"
)

var (
	flogsarama bool
)

func init() {
	// include file name in log
	log.SetFlags(log.LstdFlags | log.Llongfile)
}

func main() {
	flag.Parse()
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	deliveryConsumeKafkaCMD.Deliver()
}
