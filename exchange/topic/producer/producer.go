package main

import (
	"log"

	"rabbit/exchange/common"

	"github.com/streadway/amqp"
)

func main() {
	defer common.Conn.Close()
	defer common.Ch.Close()
	publisMsg()
}

func publisMsg() {
	var exchangeType string
	common.ExchangeName = "hello_topic"
	routing_key := "log.waring"
	exchangeType = "topic"
	common.Exchange(common.ExchangeName, exchangeType)
	queue := common.Queue(common.QueueName, nil)
	common.BindQueue(queue.Name, routing_key, common.ExchangeName)
	for i := 1; i < 3; i++ {
		publish(common.ExchangeName, routing_key)
	}

}

func publish(exchange_name string, routing_key string) {
	body := "hello world"
	agrs := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	}
	err := common.Ch.Publish(
		exchange_name, // exchange
		routing_key,   // routing key
		false,
		false,
		agrs,
	)
	if err != nil {
		log.Fatalf("%s: %s", "failed to publish a message", err)
	}
	log.Printf(" [x] Sent %s", body)
}
