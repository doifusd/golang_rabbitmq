package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"rabbit/delay_queue/conn"

	"github.com/streadway/amqp"
)

var args map[string]interface{}

func init() {
	args = make(map[string]interface{})
}

func main() {
	testDelayQueuePerMsgTTL()
	testDelayQueuePerQueueTTL()
}

/**
* 每条信息不同有效时间
*
* @Return
 */
func testDelayQueuePerMsgTTL() {
	//创建真实消费exchange
	delayExchange()
	//创建过期exhange
	perQueueTTLExchange()
	//创建真实消费queue
	delayProcessQueue()
	var i uint
	for i = 1; i <= 20; i++ {
		delayQueuePerMessageTTL(i * conn.QUEUE_EXPRIATION)
	}
}

/**
* 多有信息同一个有效时间
*
* @Return
 */
func testDelayQueuePerQueueTTL() {
	//创建真实消费exchange
	delayExchange()
	//创建过期exhange
	perQueueTTLExchange()
	//创建真实消费queue
	delayProcessQueue()
	for i := 1; i <= 20; i++ {
		//过期queue
		delayQueuePerQueueTTL()
	}
}

/**
* 创建exchange
*
* @Return
 */
func delayExchange() {
	DirectExchange(conn.DELAY_EXCHANGE)
}

//创建per_queue
func perQueueTTLExchange() {
	DirectExchange(conn.PER_QUEUE_TTL_EXCHANGE)
}

/**
* 创建test_delay_queue_per_message_ttl队列
*
* @Return
 */
func delayQueuePerMessageTTL(expiration uint) {
	//设置死信交换器
	args["x-dead-letter-exchange"] = conn.DELAY_EXCHANGE
	//设置死信交换器Key
	args["x-dead-letter-routing-key"] = conn.DELAY_TASK_QUEUE
	queue := commonQueue(conn.DELAY_QUEUE_PER_MESSAGE_TTL, args)
	//publish
	Bind(queue.Name, conn.DELAY_QUEUE_PER_MESSAGE_TTL, conn.PER_QUEUE_TTL_EXCHANGE)
	//publish
	expir := fmt.Sprintf("%d", expiration)
	Publish(conn.PER_QUEUE_TTL_EXCHANGE, conn.DELAY_QUEUE_PER_MESSAGE_TTL, expir)
}

/**
* 创建test_delay_queue_per_queue_ttl队列
* todo　现在是 test_delay_queue_per_queue_ttl过期放入到test_delay_queue_per_message_ttl
* @Return
 */
func delayQueuePerQueueTTL() {
	//设置队列的过期时间
	args["x-message-ttl"] = int(conn.QUEUE_EXPRIATION)
	//设置死信交换器
	args["x-dead-letter-exchange"] = conn.DELAY_EXCHANGE
	//设置死信交换器Key
	args["x-dead-letter-routing-key"] = conn.DELAY_TASK_QUEUE
	queue := commonQueue(conn.DELAY_QUEUE_PER_QUEUE_TTL, args)
	//bind
	Bind(queue.Name, conn.DELAY_QUEUE_PER_QUEUE_TTL, conn.PER_QUEUE_TTL_EXCHANGE)
	//publish
	Publish(conn.PER_QUEUE_TTL_EXCHANGE, conn.DELAY_QUEUE_PER_QUEUE_TTL, "")
}

//创建test_delay_process_queue队列，实际消费队列
func delayProcessQueue() {
	queue := commonQueue(conn.DELAY_TASK_QUEUE, nil)
	Bind(queue.Name, conn.DELAY_TASK_QUEUE, conn.DELAY_EXCHANGE)
}

func commonQueue(queue string, args map[string]interface{}) amqp.Queue {
	q, err := conn.Ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		args,  // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return q
}

func Bind(queue string, routing_key string, exchange_name string) {
	err := conn.Ch.QueueBind(
		queue,         // queue name
		routing_key,   // routing key
		exchange_name, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")
}

func Publish(exchange_name string, routing_key string, expiration string) {
	body := bodyFrom(os.Args)
	agrs := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	}
	if len(expiration) > 1 {
		agrs.Expiration = expiration
	}
	err := conn.Ch.Publish(
		exchange_name, // exchange
		routing_key,   // routing key
		false,         // mandatory
		false,
		agrs,
	)
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func DirectExchange(exchange string) {
	err := conn.Ch.ExchangeDeclare(exchange, "direct", false, false, false, false, nil)
	if err != nil {
		fmt.Println("declare normal exchange error:", err)
		return
	}
	return
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
