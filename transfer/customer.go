package transfer

import "github.com/segmentio/kafka-go"

type Customer struct {
	Reader         *kafka.Reader
	HandlePipeline *PipeLine
	Format         Formater
	done           chan struct{}
}

func (c Customer) Exit() {
	go func() {
		c.done <- struct{}{}
	}()
}

func (c Customer) Listen() chan struct{} {
	return c.done
}

func registerManger(c *Customer) {
	mu.Lock()
	CustomerManger[c.Reader.Config().Topic] = c
	mu.Unlock()
}

func getCustomer(topic string) (customer *Customer, ok bool) {
	mu.Lock()
	customer, ok = CustomerManger[topic]
	mu.Unlock()

	return customer, ok
}

func uninstallCustomer(topic string) {
	mu.Lock()
	delete(CustomerManger, topic)
	mu.Unlock()
}
