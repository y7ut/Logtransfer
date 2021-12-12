package plugin

import (
	"github.com/y7ut/logtransfer/entity"
)

// 警报与监测
type Alarm Plugin

func (alarm *Alarm) HandleFunc(m *entity.Matedata) error {
	return nil
}

func (alarm *Alarm) SetParams(params string) error {

	paramsValue, err := checkParams(params, "hit", "idle_time")
	if err != nil {
		return err
	}
	alarm.params = &paramsValue

	return err
}
