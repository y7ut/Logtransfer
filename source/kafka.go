package source

import (
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/y7ut/logtransfer/conf"
)

const GroupSuffix = "_group"

func InitReader(topic string) []*kafka.Reader {
	// // 先去创建一下这个分组
	// make a writer that produces to topic-A, using the least-bytes distribution
	var readers []*kafka.Reader
	for i := 0; i < 10; i++ {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:   strings.Split(conf.APPConfig.Kafka.Address, ","),
			Topic:     topic,
			GroupID:   topic+GroupSuffix,
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

func CreateCustomerGroup(topic string) error {
	config := kafka.ConsumerGroupConfig{
		ID:          topic+GroupSuffix,
		Brokers:     strings.Split(conf.APPConfig.Kafka.Address, ","),
		Topics:      []string{topic},
		StartOffset: kafka.LastOffset,
	}
	_, err := kafka.NewConsumerGroup(config)
	log.Printf("Customer group [%s] created success!", topic+GroupSuffix)
	if err != nil {
		return err
	}
	return nil
}
