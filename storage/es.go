package storage

import (
	"fmt"

	"github.com/olivere/elastic/v7"
	"github.com/y7ut/logtransfer/conf"
)

func GetClient() *elastic.Client {
	var err error
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(conf.APPConfig.Es.Address))
	if err != nil {
		fmt.Println("connect es error", err)
		return nil
	}

	return client
}
