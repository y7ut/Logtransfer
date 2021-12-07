package transfer

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	contentRegexp       = regexp.MustCompile(`\[(?s:(.*?))\]`)
	serviceWfLogKeyWord = []string{"errno", "logId", "uri", "refer", "cookie", "ua", "host", "clientIp", "optime", "request_params", "errmsg"}
	MatePool            = sync.Pool{New: func() interface{} { return &Matedate{data: make(map[string]interface{})} }}
)

type Formater func(string, string) (Matedate, error)

// service错误日志的处理
func FormatServiceWfLog(sourceKey string, message string) (Matedate, error) {
	// vMateItem := MatePool.Get()
	mateItem := MatePool.Get().(*Matedate)

	levelIndex := strings.Index(message, ":")
	if levelIndex == -1 {
		return *mateItem, fmt.Errorf("message format error")
	}

	mateItem.Topic= sourceKey
	mateItem.Level = message[:levelIndex]
	message = message[levelIndex:]
	loc, _ := time.LoadLocation("Local")
	logTime, _ := time.ParseInLocation(": 06-01-02 15:04:05 ", message[:strings.Index(message, "[")], loc)
	mateItem.create = logTime
	keyword := serviceWfLogKeyWord
	for _, word := range keyword {
		flysnowRegexp := regexp.MustCompile(fmt.Sprintf(`%s\[(?s:(.*?))\]`, word))
		logContent := flysnowRegexp.FindString(message)
		curentSub := contentRegexp.FindStringSubmatch(logContent)
		if len(curentSub) < 1 {
			continue
		}
		mateItem.data[word] = contentRegexp.FindStringSubmatch(logContent)[1]
	}

	result := *mateItem
	mateItem.reset()
	MatePool.Put(mateItem)
	return result, nil
}

// service错误日志的处理
func DefaultLog(sourceKey string, message string) (Matedate, error) {

	vMateItem := MatePool.Get()
	mateItem := vMateItem.(*Matedate)

	mateItem.Topic = sourceKey
	mateItem.Index = sourceKey
	mateItem.data = map[string]interface{}{"message":message}
	
	result := *mateItem
	MatePool.Put(vMateItem)
	return result, nil
}

// Json 格式的错误日志处理
func DefaultJsonLog(sourceKey string, message string) (Matedate, error) {

	vMateItem := MatePool.Get()
	mateItem := vMateItem.(*Matedate)

	data := mateItem.data
	err := json.Unmarshal([]byte(message), &data)
	if err != nil {
		return *mateItem, err
	}
	mateItem.data = data

	result := *mateItem
	MatePool.Put(vMateItem)
	return result, nil
}
