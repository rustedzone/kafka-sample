## usage
```
 -enable-log-sarama
        print out sarama log
  -es-host string
        elastic search host
  -es-index string
        targeted index of elastic search
  -es-type string
        targeted type of elastic search
  -kafka-broker string
        kafka broker host
  -kafka-group-name string
        targeted kafka group name 
  -kafka-topic string
        targeted kafka topic
```

example : `go run main.go --es-host http://localhost:9200 --es-index someIndex --es-type _doc --kafka-broker localhost:9092 --kafka-group-name cg-something --kafka-topic some-topic --enable-log-sarama`