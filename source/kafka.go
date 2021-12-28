package source

import (
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/y7ut/logtransfer/conf"
)

func InitReader(topic string, groupId string) []*kafka.Reader {
	// // 先去创建一下这个分组
	// make a writer that produces to topic-A, using the least-bytes distribution
	var readers []*kafka.Reader
	for i := 0; i < 10; i++ {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:   strings.Split(conf.APPConfig.Kafka.Address, ","),
			Topic:     topic,
			GroupID:   groupId,
			// Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}))
	}
	// readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
	// 	Brokers:   strings.Split(conf.APPConfig.Kafka.Address, ","),
	// 	Topic:     topic,
	// 	GroupID:   groupId,
	// 	Partition: 0,
	// 	MinBytes:  10e3, // 10KB
	// 	MaxBytes:  10e6, // 10MB
	// }))
	return readers
}

func CreateCustomerGroup(topic string, groupId string) {
	config := kafka.ConsumerGroupConfig{
		ID:          groupId,
		Brokers:     strings.Split(conf.APPConfig.Kafka.Address, ","),
		Topics:      []string{topic},
		StartOffset: kafka.LastOffset,
	}
	_, err := kafka.NewConsumerGroup(config)
	if err != nil {
		log.Println("create CustomerGroup error:", err)
	}
}
