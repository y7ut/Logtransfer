package transfer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	elastic "github.com/olivere/elastic/v7"
	"github.com/y7ut/logtransfer/conf"
	"github.com/y7ut/logtransfer/queue"
)

var (
	Start          = make(chan *Customer)
	Close          = make(chan string)
	CustomerManger = make(map[string]*Customer)
	MaxRetryTime   = 10
	mu             sync.Mutex
	closeWg        sync.WaitGroup
	messages       = make(chan *Matedate, conf.APPConfig.Es.BulkSize)
)

func getRegisterTopics() (topics []string) {
	mu.Lock()
	for topic := range CustomerManger {
		topics = append(topics, topic)
	}
	mu.Unlock()
	return topics
}

// 核心启动
func Run(confPath string) {
	// 加载配置
	conf.Init(confPath)
	// 初始化ES客户端
	esClient, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(conf.APPConfig.Es.Address))
	if err != nil {
		fmt.Println("connect es error", err)
		panic(err)
	}

	// 做一个master的上下文
	ctx, cancel := context.WithCancel(context.Background())

	//
	for i := 0; i < 3; i++ {
		go MatedateSender(ctx, esClient)
	}

	go func() {
		for {
			select {
			case customer := <-Start:
				registerManger(customer)
				go ReadingMessage(ctx, customer)

			case closer := <-Close:
				c, ok := getCustomer(closer)
				if !ok {
					log.Printf(" Customer %s unstall Failed ", closer)

				}
				c.Exit()

				closeWg.Done()

			}
		}
	}()

	for topic := range ChooseTopic() {

		GroupID := topic.Name + "_group"
		r := queue.InitReader(topic.Name, GroupID)
		currentCustomer := &Customer{Reader: r, done: make(chan struct{}), HandlePipeline: topic.PipeLine, Format: topic.Format}
		log.Printf("Check Customer group of [%s] success!", GroupID)
		Start <- currentCustomer
	}

	for sign := range sign() {
		switch sign {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
			log.Println("Safe Exit with:", sign)

			currentTopics := getRegisterTopics()

			for _, topic := range currentTopics {

				closeWg.Add(1)
				Close <- topic
				log.Printf(" Customer %s unstalling...", topic)
				closeWg.Wait()
			}
			close(messages)

			log.Printf(" Success unstall %d Transfer", len(currentTopics))
			os.Exit(0)
		}
	}
	defer cancel()

}

func sign() <-chan os.Signal {
	c := make(chan os.Signal, 2)
	// 监听信号
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	return c
}

// 從Kafka中消費消息，注意这里会提交commit offset
func ReadingMessage(ctx context.Context, c *Customer) {
	defer c.Reader.Close()

	log.Printf("Start Customer Group[%s] success!", c.Reader.Config().GroupID)

	// var trycount int
	// var cstSh, _ = time.LoadLocation("Asia/Shanghai") //上海时区
	var errMessage strings.Builder
	// log.Println(c.HandlePipeline.pipe)

	var matedata Matedate
	for {
		select {
		case <-c.Listen():
			// 监听需要关闭的信号
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

func MatedateSender(ctx context.Context, esClient *elastic.Client) {

	tick := time.NewTicker(3 * time.Second)
	var (
		SenderMu sync.Mutex
	)

	bulkRequest := esClient.Bulk()
	for {
		select {
		case m := <-messages:
			indexRequest := elastic.NewBulkIndexRequest().Index(m.Index).Doc(m.data)
			bulkRequest.Add(indexRequest)

		case <-tick.C:
			// Do sends the bulk requests to Elasticsearch
			SenderMu.Lock()
			count := bulkRequest.NumberOfActions()
			if count > 0 {
				log.Printf("Send message to Es: %d : \n", bulkRequest.NumberOfActions())
				_, err := bulkRequest.Do(ctx)
				if err != nil {
					log.Println("Save Es Error:", err)
				}
				bulkRequest.Reset()
			}
			SenderMu.Unlock()

		case <-ctx.Done():
			// Do sends the bulk requests to Elasticsearch
			SenderMu.Lock()
			_, err := bulkRequest.Do(ctx)
			if err != nil {
				log.Println("Save Es Error:", err)
			}
			bulkRequest.Reset()
			SenderMu.Unlock()
			log.Println("Exiting...")
			return
		}
	}
}
