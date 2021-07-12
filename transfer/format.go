package transfer

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	contentRegexp       = regexp.MustCompile(`\[(?s:(.*?))\]`)
	serviceWfLogKeyWord = []string{"errno", "logId", "uri", "refer", "cookie", "ua", "host", "clientIp", "optime", "request_params", "errmsg"}
)

type Formater func(string, string) (Matedate, error)

// service错误日志的处理
func FormatServiceWfLog(sourceKey string, message string) (Matedate, error) {
	result := make(Matedate, 0)
	levelIndex := strings.Index(message, ":")
	if levelIndex == -1 {
		return result, fmt.Errorf("message format error.")
	}
	// log.Println(message)
	result["topic"] = sourceKey
	result["level"] = message[:levelIndex]
	result["project"] = "jiwei-service"
	message = message[levelIndex:]

	loc, _ := time.LoadLocation("Local")
	logTime, _ := time.ParseInLocation(": 06-01-02 15:04:05 ", message[:strings.Index(message, "[")], loc)

	result["created_at"] = logTime

	keyword := serviceWfLogKeyWord

	for _, word := range keyword {
		flysnowRegexp := regexp.MustCompile(fmt.Sprintf(`%s\[(?s:(.*?))\]`, word))
		logContent := flysnowRegexp.FindString(message)
		curentSub := contentRegexp.FindStringSubmatch(logContent)
		if len(curentSub) < 1 {
			continue
		}
		result[word] = contentRegexp.FindStringSubmatch(logContent)[1]
	}

	return result, nil
}

// service错误日志的处理
func DefaultLog(sourceKey string, message string) (Matedate, error) {
	result := make(Matedate, 0)
	result["topic"] = sourceKey
	result["message"] = message

	return result, nil
}
