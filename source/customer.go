package source

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/plugin"
)

var (
	CustomerManger = make(map[string]*Customer)
	mu             sync.Mutex
)

// 消费处理器
type Customer struct {
	Readers        []*kafka.Reader // 特定消费者的专属Kafka Reader Slice (我从哪里来)
	Topic          *Topic
	HandlePipeline *plugin.PipeLine // 从Topic中构建的Piepline (要到那里去)
	Format         entity.Formater  // 解析元数据的格式器 （变形记。。）
	done           chan struct{}    // 结束标志
}

// 结束一个消费处理器
func (c Customer) Exit() {
	c.done <- struct{}{}
}

// 结束信号监听
func (c Customer) Listen() <-chan struct{} {
	return c.done
}

// 初始化一个消费处理器
func InitCustomer(topic *Topic) *Customer {
	r := InitReader(topic.Name)
	log.Printf("Check Customer group of [%s] success!", topic.Name)
	return &Customer{Topic: topic, Readers: r, done: make(chan struct{}), HandlePipeline: topic.PipeLine, Format: topic.Format}
}

// 全局的注册当前工作的消费处理器
func RegisterManger(c *Customer) {
	mu.Lock()
	CustomerManger[c.Topic.Name] = c
	mu.Unlock()
}

func UnstallManger(topic string){
	mu.Lock()
	delete(CustomerManger, topic)
	mu.Unlock()
}

// 根据topic快速获取消费处理器 目前用于关闭消费处理器
func GetCustomer(topic string) (customer *Customer, ok bool) {
	mu.Lock()
	customer, ok = CustomerManger[topic]
	mu.Unlock()

	return customer, ok
}

// 获取全部的注册过的消费处理器 所使用的的Topic名字
func GetRegisterTopics() (topics []string) {
	mu.Lock()
	for topic := range CustomerManger {
		topics = append(topics, topic)
	}
	mu.Unlock()
	return topics
}

// 從Kafka中消費消息，注意这里会提交commit offset
func ReadingMessage(ctx context.Context, c *Customer) {

	readyToRead := make(chan *kafka.Reader)

	go func(ctx context.Context, c *Customer) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			select {
			case <-c.Listen():
				return
			case <-ctx.Done():
				c.Exit()
				return
			case reader := <-readyToRead:
				
				go func(ctx context.Context, c *Customer) {
					defer reader.Close()

					var errMessage strings.Builder
					var matedata entity.Matedata

					for {
						select {
						case <- ctx.Done():
							return
						default:
							m, err := reader.ReadMessage(ctx)
							if err != nil {
								switch err {
								case context.Canceled:
									// 监听主上下文信号
									log.Println("Closing Kafka Conection!")
									return
								default:
									errMessage.Reset()
									errMessage.WriteString("Reader Error")
									errMessage.WriteString(err.Error())
									log.Println(errMessage.String())
	
									continue
								}
							
							}

							matedata, err = c.Format(string(reader.Config().Topic), string(m.Value))
							if err != nil {
								errMessage.Reset()
								errMessage.WriteString("Format Error")
								errMessage.WriteString(err.Error())
								log.Println(errMessage.String())
								continue
							}
							// 流入pipe
							c.HandlePipeline.Enter(matedata)
						}
					}
				}(ctx, c)

			}
		}

	}(ctx, c)

	for _, p := range c.Readers {

		log.Printf("Start Customer Group[%s][%d] success!", p.Config().GroupID, p.Config().Partition)

		readyToRead <- p
	}
}
