package transfer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	elastic "github.com/olivere/elastic/v7"

	"github.com/y7ut/logtransfer/conf"
	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/source"
)

var (
	Start          = make(chan *source.Customer)
	Close          = make(chan string)
	closeWg        sync.WaitGroup
)

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

	// 启动es消息发送器
	for i := 0; i < 3; i++ {
		go entity.MatedateSender(ctx, esClient)
	}

	// 用于处理启动与关闭消费处理器的信号通知
	go func() {
		for {
			select {
			case customer := <-Start:
				source.RegisterManger(customer)
				go source.ReadingMessage(ctx, customer)

			case closer := <-Close:
				c, ok := source.GetCustomer(closer)
				if !ok {
					log.Printf(" Customer %s unstall Failed ", closer)

				}
				c.Exit()
				closeWg.Done()
			}
		}
	}()

	// TODO: 动态的注册customer，目前仅支持初始化的时候来加载
	for topic := range source.ChooseTopic() {
		currentCustomer := source.InitCustomer(topic)
		Start <- currentCustomer
	}

	// TODO: 还要监听Topic的配置变更
	// 目前是通过topic的name来注册所有的消费处理器
	// 所以直接给对应的topic中的customer重启就可以杀了就可以了

	for sign := range sign() {
		switch sign {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
			log.Println("Safe Exit with:", sign)

			currentTopics := source.GetRegisterTopics()

			for _, topic := range currentTopics {

				closeWg.Add(1)
				Close <- topic
				log.Printf(" Customer %s unstalling...", topic)
				closeWg.Wait()
			}
			entity.CloseMessageChan()

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
