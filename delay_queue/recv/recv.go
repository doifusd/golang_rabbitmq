package main

import (
	"log"
	"rabbit/delay_queue/conn"
)

func main() {
	q, err := conn.Ch.QueueDeclare(
		conn.DELAY_TASK_QUEUE, // queue name
		false,                 // durable
		false,                 // delete when unused
		false,                 // exclusive
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare a queue ", err)
	}
	// 将预取计数器设置为1
	// 在并行处理中将消息分配给不同的工作进程
	err = conn.Ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to set QoS ", err)
	}

	msgs, err := conn.Ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("%s: %s", "Failed to register a consumer ", err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			log.Printf("Done")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
