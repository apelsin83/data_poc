package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func producer() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "g1",
		"auto.offset.reset": "earliest",
		"isolation.level":   "read_committed",
	})
	defer c.Close()

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"etld", "^aRegex.*[Tt]opic"}, nil)

	for {
		msg, err := c.ReadMessage(10 * time.Second)
		if err == nil {
			fmt.Println(string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v). Time %v\n", err, msg, time.Now())
		}
	}

}

func main() {

	producer()

}
