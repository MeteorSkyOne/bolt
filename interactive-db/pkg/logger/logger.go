package logger

import (
	"fmt"
	"sync"
	"time"
)

type LogLevel int

const (
	INFO LogLevel = iota
	ERROR
)

type LogMessage struct {
	Level     LogLevel
	Timestamp time.Time
	Component string
	Message   string
}

type SimpleLogger struct {
	logChan   chan LogMessage
	done      chan struct{}
	wg        sync.WaitGroup
	component string // 注册的组件名称
}

var (
	globalLogger *SimpleLogger
	once         sync.Once
)

// Init 初始化全局日志器
func Init() {
	once.Do(func() {
		globalLogger = &SimpleLogger{
			logChan:   make(chan LogMessage, 1000),
			done:      make(chan struct{}),
			component: "UNKNOWN", // 默认组件名
		}
		globalLogger.start()
	})
}

// SetComponent 设置日志组件名称
func SetComponent(component string) {
	if globalLogger != nil {
		globalLogger.component = component
	}
}

// Close 关闭日志器
func Close() {
	if globalLogger != nil {
		close(globalLogger.done)
		globalLogger.wg.Wait()
	}
}

func (l *SimpleLogger) start() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			select {
			case msg := <-l.logChan:
				timestamp := msg.Timestamp.Format("2006-01-02 15:04:05.000")
				level := "INFO"
				if msg.Level == ERROR {
					level = "ERROR"
				}
				fmt.Printf("[%s] [%s] %s: %s\n", msg.Component, timestamp, level, msg.Message)
			case <-l.done:
				// 处理剩余消息
				for {
					select {
					case msg := <-l.logChan:
						timestamp := msg.Timestamp.Format("2006-01-02 15:04:05.000")
						level := "INFO"
						if msg.Level == ERROR {
							level = "ERROR"
						}
						fmt.Printf("[%s] [%s] %s: %s\n", msg.Component, timestamp, level, msg.Message)
					default:
						return
					}
				}
			}
		}
	}()
}

// Info 记录信息日志
func Info(format string, args ...interface{}) {
	InfoWithComponent(globalLogger.component, format, args...)
}

// Error 记录错误日志
func Error(format string, args ...interface{}) {
	ErrorWithComponent(globalLogger.component, format, args...)
}

// InfoWithComponent 记录信息日志（带组件名）
func InfoWithComponent(component, format string, args ...interface{}) {
	if globalLogger == nil {
		// 如果日志器未初始化，直接打印
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		message := fmt.Sprintf(format, args...)
		fmt.Printf("[%s] [%s] INFO: %s\n", component, timestamp, message)
		return
	}

	message := fmt.Sprintf(format, args...)
	select {
	case globalLogger.logChan <- LogMessage{
		Level:     INFO,
		Timestamp: time.Now(),
		Component: component,
		Message:   message,
	}:
	default:
		// 缓冲区满时直接打印
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		fmt.Printf("[%s] [%s] INFO: %s (buffer full)\n", component, timestamp, message)
	}
}

// ErrorWithComponent 记录错误日志（带组件名）
func ErrorWithComponent(component, format string, args ...interface{}) {
	if globalLogger == nil {
		// 如果日志器未初始化，直接打印
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		message := fmt.Sprintf(format, args...)
		fmt.Printf("[%s] [%s] ERROR: %s\n", component, timestamp, message)
		return
	}

	message := fmt.Sprintf(format, args...)
	select {
	case globalLogger.logChan <- LogMessage{
		Level:     ERROR,
		Timestamp: time.Now(),
		Component: component,
		Message:   message,
	}:
	default:
		// 缓冲区满时直接打印
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		fmt.Printf("[%s] [%s] ERROR: %s (buffer full)\n", component, timestamp, message)
	}
}
