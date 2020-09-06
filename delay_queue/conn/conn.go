package conn

import (
	"log"

	"github.com/streadway/amqp"
)

const (
	//延迟exchange
	DELAY_EXCHANGE = "test_delay_exchange"
	//路由到test_per_queue_ttl_exchange
	PER_QUEUE_TTL_EXCHANGE = "test_per_queue_ttl_exchange"
	//发送到该队列的message会在一段时间后过期进入到test_delay_process_queque
	//每个message可以控制自己的失效时间
	DELAY_QUEUE_PER_MESSAGE_TTL = "test_delay_queue_per_message_ttl"
	//发送到该队列的message会在一段时间后过期进入到test_delay_process_queue
	//队列里所有的message都有统一的失效时间
	DELAY_QUEUE_PER_QUEUE_TTL = "test_delay_queue_per_queue_ttl"
	//过期时间毫秒
	QUEUE_EXPRIATION uint = 5000
	//message失效后进入的队列，也就是实际的消息队列
	DELAY_TASK_QUEUE = "test_delay_process_queue"
)

var Ch *amqp.Channel
var err error

func init() {
	newRabbitmq()
}

/**
* 连接
*
* @Return
 */
func newRabbitmq() {
	conn, err := amqp.Dial("amqp://sky:sky@localhost:5672/")
	if err != nil {
		FailOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()
	}

	Ch, err = conn.Channel()
	if err != nil {
		defer Ch.Close()
		FailOnError(err, "Failed to open a channel")
	}
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
