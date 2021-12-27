package entity

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Formater func(string, string) (Matedata, error)

// service错误日志的处理
func FormatServiceWfLog(sourceKey string, message string) (Matedata, error) {
	// vMateItem := MatePool.Get()
	mateItem := MatePool.Get().(*Matedata)

	levelIndex := strings.Index(message, ":")
	if levelIndex == -1 {
		return *mateItem, fmt.Errorf("message format error")
	}

	mateItem.Topic = sourceKey
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
		if word == "errmsg" {

			mateItem.Data["message"] = strings.Replace(contentRegexp.FindStringSubmatch(logContent)[1], `"`, "", -1)
		} else {
			mateItem.Data[word] = contentRegexp.FindStringSubmatch(logContent)[1]
		}

	}
	mateItem.Data["timestamp"] = mateItem.create.Format("2006-01-02 15:04:05")
	result := *mateItem
	mateItem.reset()
	MatePool.Put(mateItem)
	return result, nil
}

// service错误日志的处理
func DefaultLog(sourceKey string, message string) (Matedata, error) {

	vMateItem := MatePool.Get()
	mateItem := vMateItem.(*Matedata)

	mateItem.Topic = sourceKey
	mateItem.Index = sourceKey
	mateItem.Data = map[string]interface{}{"message": message}

	result := *mateItem
	MatePool.Put(vMateItem)
	return result, nil
}

// Json 格式的错误日志处理
func DefaultJsonLog(sourceKey string, message string) (Matedata, error) {

	vMateItem := MatePool.Get()
	mateItem := vMateItem.(*Matedata)

	data := mateItem.Data
	err := json.Unmarshal([]byte(message), &data)
	if err != nil {
		return *mateItem, err
	}
	mateItem.Data = data

	result := *mateItem
	MatePool.Put(vMateItem)
	return result, nil
}
