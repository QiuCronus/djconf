package main

import (
	// "crypto/rc4"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gconf/gconf"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

const (
	RC4_Key = ""
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// func replace_file(raw []byte) (ret bool, err error) {

// }

func listening() {

	// cipher, _ := rc4.NewCipher([]byte(RC4_Key))

	exchange := "Conf"
	hostname, err := os.Hostname()
	if err != nil {
		failOnError(err, "Failed to get hostname.")
	}

	// TODO: 配置信息要解密
	url := "amqp://guest:guest@localhost:5672/"

	// 连接 RabbitMQ
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 创建 Channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 创建 Exchange
	if err = ch.ExchangeDeclare(exchange, "direct", true, false, false, true, nil); err != nil {
		failOnError(err, "Failed to declare a Exchange")
	}

	// 创建 Queue
	q, err := ch.QueueDeclare(hostname, true, false, false, false, nil)
	failOnError(err, "Failed to declare a queue.")

	err = ch.QueueBind(hostname, hostname, exchange, false, nil)
	failOnError(err, fmt.Sprintf("Failed to bind queue(%s) to exchange(%s).", hostname, exchange))

	// 设置一次消费一条数据
	err = ch.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS")

	// 设置消费者
	msgs, err := ch.Consume(q.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {

		response := "done"

		pbmsg := &gconf.PBMessage{}
		err = proto.Unmarshal(d.Body, pbmsg)
		if err != nil {
			fmt.Printf("unmarshal PBMessage failed.")
		}

		pbfile := &gconf.PBFile{}
		err = proto.Unmarshal(pbmsg.GetRaw(), pbfile)
		if err != nil {
			fmt.Printf("unmarshal PBFile failed.")
		}

		if _, err := os.Stat(pbfile.GetPath()); os.IsExist(err) {
			// backup
			backup_path := fmt.Sprintf("%s.bak", pbfile.GetPath())
			os.Rename(pbfile.GetPath(), backup_path)
		}
		err := ioutil.WriteFile(pbfile.GetPath(), pbfile.GetContent(), 0644)
		if err != nil {
			// 写文件失败
			response = fmt.Sprintf("write failed. %s", err)
		}

		err = ch.Publish(
			exchange,  // exchange
			d.ReplyTo, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: d.CorrelationId,
				Body:          []byte(response),
			})
		failOnError(err, "Failed to publish a message")

		d.Ack(false)
	}
}

func main() {
	forever := make(chan bool)
	go listening()

	log.Printf(" [*] start listening")
	<-forever
}
