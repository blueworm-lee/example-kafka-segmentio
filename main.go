package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "message-log"
	brokerAddress = "<yourip>:9092" //Modify kafka ip address
)

func createTopic(context context.Context) {
	conn, err := kafka.DialLeader(context, "tcp", brokerAddress, topic, 0)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
}

func producer(ctx context.Context) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerAddress},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,
		BatchTimeout: 100 * time.Millisecond, //Control Sending messages time
	})
	defer writer.Close()

	for i := 0; i < 10; i++ {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte("Key=" + strconv.Itoa(i)),
			Value: []byte("Value=" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}
	}

	fmt.Println("Producer finished...")
}

func consumer(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		msg, err := reader.ReadMessage(ctx) //Block and Wait
		if err != nil {
			panic("could not read message " + err.Error())
		}

		fmt.Println("Received: Key=", string(msg.Key), ", Value=", string(msg.Value))
	}
}

func main() {
	context := context.Background()

	createTopic(context)
	go producer(context)
	consumer(context)
}
