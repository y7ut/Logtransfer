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
	Reader         *kafka.Reader      // 特定消费者的专属Kafka Reader (我从哪里来)
	HandlePipeline *plugin.PipeLine   // 从Topic中构建的Piepline (要到那里去)
	Format         entity.Formater   // 解析元数据的格式器 （变形记。。）
	done           chan struct{}	// 结束标志
}

// 结束一个消费处理器
func (c Customer) Exit() {
	go func() {
		c.done <- struct{}{}
	}()
}

// 结束信号监听
func (c Customer) Listen() chan struct{} {
	return c.done
}

// 初始化一个消费处理器
func InitCustomer(topic *Topic) *Customer{
	GroupID := topic.Name + "_group"
	r := InitReader(topic.Name, GroupID)
	log.Printf("Check Customer group of [%s] success!", GroupID)
	return &Customer{Reader: r, done: make(chan struct{}), HandlePipeline: topic.PipeLine, Format: topic.Format}
}

// 全局的注册当前工作的消费处理器
func RegisterManger(c *Customer) {
	mu.Lock()
	CustomerManger[c.Reader.Config().Topic] = c
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
	defer c.Reader.Close()

	log.Printf("Start Customer Group[%s] success!", c.Reader.Config().GroupID)

	// var trycount int
	// var cstSh, _ = time.LoadLocation("Asia/Shanghai") //上海时区
	var errMessage strings.Builder


	var matedata entity.Matedata
	for {
		select {
		case <-c.Listen():
			// 监听需要关闭的信号

			c.Exit()
			log.Println("Close customer of Topic :", c.Reader.Config().Topic)
			return
		default:
			// // 使用超时上下文, 但是这样是非阻塞，超过deadline就报错了
			// // timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			// // 这里使用阻塞的上下文

			m, err := c.Reader.ReadMessage(ctx)

			if err != nil {
				// 退出
				// c.Exit()
				errMessage.Reset()
				errMessage.WriteString("Reader Error")
				errMessage.WriteString(err.Error())
				log.Println(errMessage.String())

				continue
			}

			matedata, err = c.Format(string(c.Reader.Config().Topic), string(m.Value))
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
}
