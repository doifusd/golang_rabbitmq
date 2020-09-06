package main

import (
	"log"
	"rabbit/exchange/common"
)

func main() {
	worker()
}

func worker() {
	routing_key := "log.#"
	common.ExchangeName = "hello_topic"
	common.Exchange(common.ExchangeName, "topic")

	queue := common.Queue(common.QueueName, nil)
	common.BindQueue(queue.Name, routing_key, common.ExchangeName)
	// 将预取计数器设置为1
	// 在并行处理中将消息分配给不同的工作进程
	err := common.Ch.Qos(
		100,   // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("%s: %s", "failed to set QoS", err)
	}

	msgs, err := common.Ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Fatalf("%s: %s", "failed to register consumer", err)
	}
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			log.Printf("Done")
			d.Ack(true)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
