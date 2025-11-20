package docker

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
	kafka_test "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func Kafka(ctx context.Context, wg *sync.WaitGroup) (kafkaUrl string, err error) {
	c, err := kafka_test.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		return kafkaUrl, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container kafka", c.Terminate(context.Background()))
	}()
	brokers, err := c.Brokers(ctx)
	if err != nil {
		return kafkaUrl, err
	}
	return brokers[0], err
}

func tryKafkaConn(kafkaUrl string) error {
	log.Println("try kafka connection to " + kafkaUrl + "...")
	conn, err := kafka.Dial("tcp", kafkaUrl)
	if err != nil {
		log.Println(err)
		return err
	}
	defer conn.Close()
	brokers, err := conn.Brokers()
	if err != nil {
		log.Println(err)
		return err
	}
	if len(brokers) == 0 {
		err = errors.New("missing brokers")
		log.Println(err)
		return err
	}
	log.Println("kafka connection ok")
	return nil
}
