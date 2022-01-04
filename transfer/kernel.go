package transfer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/y7ut/logtransfer/conf"
	"github.com/y7ut/logtransfer/entity"
	"github.com/y7ut/logtransfer/source"
)

var (
	Start = make(chan *source.Customer)
	Close = make(chan string)
)

// 核心启动
func Run(confPath string) {
	// 加载配置
	conf.Init(confPath)

	// 做一个master的上下文
	ctx, cancel := context.WithCancel(context.Background())

	go entity.MatedateSender(ctx)

	// 用于处理启动与关闭消费处理器的信号通知
	go CollectorRegister(ctx)

	initTopic, err := source.ChooseTopic()
	if err != nil {
		panic(fmt.Sprintf("init topic fail: %s", err))
	}

	for topic := range initTopic {
		currentCustomer := source.InitCustomer(topic)
		Start <- currentCustomer
	}

	go TopicWatcherHandle()

	// 监听 Agent Collector 变更
	go source.WatchConfigs()

	// 还要监听 Topic 的配置变更
	go source.WatchTopics()
	// 还要监听 Status 的配置变更
	go source.WatchStatus()

	for sign := range sign() {
		switch sign {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM:
			log.Println("Safe Exit with:", sign)

			currentTopics := source.GetRegisterTopics()

			for _, topic := range currentTopics {

				Close <- topic
				log.Printf(" Customer %s unstalling...", topic)

			}
			cancel()
			entity.CloseMessageChan()
			time.Sleep(1 * time.Second)
			log.Printf(" Success unstall %d Transfer", len(currentTopics))
			os.Exit(0)
		}
	}
	defer cancel()

}

func CollectorRegister(ctx context.Context) {
	for {
		select {
		case customer := <-Start:
			source.RegisterManger(customer)
			go source.ReadingMessage(ctx, customer)

		case closer := <-Close:
			c, ok := source.GetCustomer(closer)
			if !ok {
				log.Printf(" Customer %s unstall Failed ", closer)
				break
			}
			source.UnstallManger(closer)
			c.Exit()
			// closeWg.Done()
		}
	}
}

// 监控topic的变动, 只处理更新, 若删除topic的话，不会触发配置的重新载入行为
func TopicWatcherHandle() {
	// restart
	go func() {
		for topic := range source.TopicChangeListener() {

			var checkUsed bool

			collectors, err := source.LoadCollectors()

			if err != nil {
				log.Println("Load Collector error:", err)
				continue
			}

			// 检查是否使用
			for _, item := range collectors {
				if item.Topic == topic.Name {
					checkUsed = true
				}
			}
			if !checkUsed {
				log.Println("Put topic but not used")
				err := source.CreateCustomerGroup(topic.Name)
				if err != nil {
					log.Printf(" Create Topic Kafka customer group Failed : %s", err)
					continue
				}
				continue
			}

			Close <- topic.Name
			log.Printf(" Customer %s restart...", topic.Name)

			currentCustomer := source.InitCustomer(topic)
			Start <- currentCustomer
		}
	}()
	// close
	go func() {
		for deleteTopic := range source.TopicDeleteListener() {
			// closeWg.Add(1)

			Close <- deleteTopic
			log.Printf(" Customer %s deleting...", deleteTopic)
			// closeWg.Wait()
		}
	}()
	// start
	go func() {
		for topic := range source.TopicStartListener() {
			currentCustomer := source.InitCustomer(topic)
			Start <- currentCustomer
		}
	}()
}

func sign() <-chan os.Signal {
	c := make(chan os.Signal, 2)
	// 监听信号
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	return c
}
