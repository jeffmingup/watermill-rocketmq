package rocketmq

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type Log struct {
	logger watermill.LoggerAdapter
}

func (m Log) Debug(msg string, fields map[string]interface{}) {
	m.logger.Debug(msg, fields)
}

func (m Log) Info(msg string, fields map[string]interface{}) {
	m.logger.Info(msg, fields)
}

func (m Log) Warning(msg string, fields map[string]interface{}) {
	m.logger.Error(msg, nil, fields)
}

func (m Log) Error(msg string, fields map[string]interface{}) {
	m.logger.Error(msg, nil, fields)
}

func (m Log) Fatal(msg string, fields map[string]interface{}) {
	m.logger.Error(msg, nil, fields)
}

func (m Log) Level(_ string) {
}

func (m Log) OutputPath(_ string) (err error) {
	return nil
}

func SetRocketMQLogger(logger watermill.LoggerAdapter) {
	rlog.SetLogger(Log{logger: logger}) // 设置rocketMQ日志输出
}
