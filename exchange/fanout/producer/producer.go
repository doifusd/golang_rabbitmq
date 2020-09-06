package main

import (
	"log"

	"rabbit/exchange/common"

	"github.com/streadway/amqp"
)

func main() {
	defer common.Conn.Close()
	defer common.Ch.Close()
	producer()
}

func producer() {
	var exchangeType string
	exchangeType = "fanout"
	common.ExchangeName = "exchange_fanout"
	common.Exchange(common.ExchangeName, exchangeType)
	for i := 1; i < 3; i++ {
		publish(common.ExchangeName)
	}

}

func publish(exchange_name string) {
	body := "hello world"
	agrs := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	}
	err := common.Ch.Publish(
		exchange_name, // exchange
		"",            // routing key
		false,
		false,
		agrs,
	)
	if err != nil {
		log.Fatalf("%s: %s", "failed to publish a message", err)
	}
	log.Printf(" [x] Sent %s", body)
}
