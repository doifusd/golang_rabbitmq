package common

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var Ch *amqp.Channel
var err error
var Conn *amqp.Connection
var ExchangeName string = "hello"
var QueueName string = "task:hello"

func init() {
	NewRabbit()
}

func NewRabbit() {
	Conn, err = amqp.Dial("amqp://sky:sky@localhost:5672")
	if err != nil {
		log.Fatalf("%s: %s", "failed to conn", err)
	}

	Ch, err = Conn.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "failed to open a channel", err)
	}
}

func Exchange(exchange string, exchangeType string) {
	err := Ch.ExchangeDeclare(exchange, exchangeType, false, false, false, false, nil)
	if err != nil {
		fmt.Println("declare normal exchange error:", err)
		return
	}
	return
}

func Queue(queue string, args map[string]interface{}) amqp.Queue {
	q, err := Ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		args,  // arguments
	)
	if err != nil {
		log.Fatalf("%s: %s", "failed to declarer queue", err)
	}
	return q
}

func BindQueue(queue, routing_key, exchange_name string) {
	err := Ch.QueueBind(
		queue,         // queue name
		routing_key,   // routing key
		exchange_name, // exchange
		false,
		nil)

	if err != nil {
		log.Fatalf("%s: %s", "failed to bind", err)
	}
}

/**
* exchange headers
*
* @Param    queue
* @Param    string
*
* @Return
 */
func BindQueueHeaders(queue, exchange_name string, args amqp.Table) {

	err := Ch.QueueBind(
		queue,         // queue name
		"",            // routing key
		exchange_name, // exchange
		false,
		args)

	if err != nil {
		log.Fatalf("%s: %s", "failed to bind", err)
	}
}
