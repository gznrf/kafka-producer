package main

import (
	"fmt"
	k "github.com/gznrf/kafka-producer/internal/kafka"
	"github.com/sirupsen/logrus"
)

const (
	topic = "my-topic"
)

var address = []string{
	"localhost:9091",
	"localhost:9092",
	"localhost:9093",
}

func main() {
	p, err := k.NewProducer(address)
	if err != nil {
		logrus.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("kafka message %d", i)
		if err := p.Produce(msg, topic); err != nil {
			logrus.Error(err)
		}

	}
}
