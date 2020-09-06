package main

import (
	"fmt"
	"log"
	"time"

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
	exchangeType = "headers"
	common.ExchangeName = "exchange_headers"
	common.QueueName = "task:headers"
	common.Exchange(common.ExchangeName, exchangeType)
	queue := common.Queue(common.QueueName, nil)
	headers := amqp.Table{}
	headers["user"] = "tom"
	headers["pwd"] = "123"
	headers["version"] = "1.0"
	headers["api"] = "login"
	common.BindQueueHeaders(queue.Name, common.ExchangeName, headers)
	for i := 1; i < 3; i++ {
		publish(common.ExchangeName, headers)
	}

}

func publish(exchange_name string, headers amqp.Table) {
	body := "hihi china!!!"
	agrs := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Unix(time.Now().UnixNano(), 0),
		ContentType:  "text/plain",
		Headers:      headers,
		Body:         []byte(body),
	}
	fmt.Println(agrs)
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
