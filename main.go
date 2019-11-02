package main

import (
	"flag"
	"log"
)

var (
	flogsarama bool
)

func init() {
	flag.BoolVar(&flogsarama, "log-sarama", false, "print out sarama log")
}

func main() {
	flag.Parse()
	log.Println("fsaramalog :", flogsarama) // trashlog

	log.Println("hello world")
}
