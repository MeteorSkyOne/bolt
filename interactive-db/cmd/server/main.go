package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meteorsky/bolt"
)

const (
	defaultPort   = "8080"
	defaultDBFile = "interactive.db"
	bucketName    = "kv_store"
)

// logWithTimestamp 带时间戳的日志输出函数
func logWithTimestamp(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] %s\n", timestamp, message)
}

// logErrorWithTimestamp 带时间戳的错误日志输出函数
func logErrorWithTimestamp(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] ERROR: %s\n", timestamp, message)
}

// TransactionOperation 事务操作类型
type TransactionOperation struct {
	Type  string // "PUT", "GET", "DEL"
	Key   string
	Value string
}

// ClientSession 客户端会话
type ClientSession struct {
	ID             string
	conn           net.Conn
	inTransaction  bool
	boltTx         *bolt.Tx // 直接使用BoltDB事务提供隔离
	transactionOps []TransactionOperation
	mu             sync.RWMutex
}

// DatabaseServer 数据库服务器
type DatabaseServer struct {
	db       *bolt.DB
	clients  map[string]*ClientSession
	clientMu sync.RWMutex
	port     string
}

// NewDatabaseServer 创建新的数据库服务器
func NewDatabaseServer(dbFile string) (*DatabaseServer, error) {
	// 打开BoltDB数据库
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// 创建bucket如果不存在
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %v", err)
	}

	return &DatabaseServer{
		db:      db,
		clients: make(map[string]*ClientSession),
	}, nil
}

// Close 关闭服务器
func (ds *DatabaseServer) Close() error {
	if ds.db != nil {
		return ds.db.Close()
	}
	return nil
}

// Start 启动服务器
func (ds *DatabaseServer) Start(port string) error {
	ds.port = port
	serverAddr := ":" + port

	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	defer listener.Close()

	logWithTimestamp("Database server started on port %s", port)
	logWithTimestamp("Waiting for clients...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			logErrorWithTimestamp("Failed to accept connection: %v", err)
			continue
		}

		// 为每个客户端创建会话并启动goroutine处理
		clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())
		session := &ClientSession{
			ID:             clientID,
			conn:           conn,
			inTransaction:  false,
			boltTx:         nil,
			transactionOps: make([]TransactionOperation, 0),
		}

		ds.clientMu.Lock()
		ds.clients[clientID] = session
		ds.clientMu.Unlock()

		logWithTimestamp("Client %s connected from %s", clientID, conn.RemoteAddr())
		go ds.handleClient(session)
	}
}

// handleClient 处理客户端连接
func (ds *DatabaseServer) handleClient(session *ClientSession) {
	defer func() {
		session.conn.Close()
		ds.clientMu.Lock()
		delete(ds.clients, session.ID)
		ds.clientMu.Unlock()
		logWithTimestamp("Client %s disconnected", session.ID)
	}()

	// 发送欢迎消息
	ds.sendResponse(session, "Connected to Interactive Database Server")
	ds.sendResponse(session, "Type 'HELP' for available commands, 'EXIT' to quit")

	scanner := bufio.NewScanner(session.conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		// 记录收到的命令
		logWithTimestamp("Client %s executed: %s", session.ID, command)

		response := ds.executeCommand(session, command)
		ds.sendResponse(session, response)

		// 检查是否是退出命令
		if strings.ToUpper(strings.Fields(command)[0]) == "EXIT" ||
			strings.ToUpper(strings.Fields(command)[0]) == "QUIT" {
			break
		}
	}
}

// sendResponse 发送响应给客户端
func (ds *DatabaseServer) sendResponse(session *ClientSession, response string) {
	_, err := session.conn.Write([]byte(response + "\n"))
	if err != nil {
		logErrorWithTimestamp("Failed to send response to client %s: %v", session.ID, err)
	}
}

// executeCommand 执行命令
func (ds *DatabaseServer) executeCommand(session *ClientSession, command string) string {
	parts := strings.Fields(strings.TrimSpace(command))
	if len(parts) == 0 {
		return ""
	}

	cmd := strings.ToUpper(parts[0])
	session.mu.Lock()
	defer session.mu.Unlock()

	switch cmd {
	case "BEGIN":
		return ds.beginTransaction(session)

	case "COMMIT":
		return ds.commitTransaction(session)

	case "ABORT":
		return ds.abortTransaction(session)

	case "PUT":
		if len(parts) < 3 {
			return "Error: PUT requires key and value"
		}
		key := parts[1]
		value := strings.Join(parts[2:], " ")
		return ds.put(session, key, value)

	case "GET":
		if len(parts) != 2 {
			return "Error: GET requires exactly one key"
		}
		key := parts[1]
		return ds.get(session, key)

	case "DEL":
		if len(parts) != 2 {
			return "Error: DEL requires exactly one key"
		}
		key := parts[1]
		return ds.delete(session, key)

	case "HELP":
		return ds.getHelpMessage()

	case "EXIT", "QUIT":
		return "Goodbye!"

	case "SHOW":
		return ds.showAll(session)

	case "OPEN":
		return "OPEN -> Database system is always running on server"

	case "CLOSE":
		return "CLOSE -> Use EXIT to disconnect from server"

	default:
		return fmt.Sprintf("Unknown command: %s\nType 'HELP' for available commands", cmd)
	}
}

// beginTransaction 开始事务
func (ds *DatabaseServer) beginTransaction(session *ClientSession) string {
	if session.inTransaction {
		return "Error: Already in transaction"
	}

	// 使用BoltDB原生事务隔离 - 开始只读事务获得一致性快照
	tx, err := ds.db.Begin(false)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err)
	}

	session.inTransaction = true
	session.boltTx = tx
	session.transactionOps = make([]TransactionOperation, 0)
	return "BEGIN -> transaction started"
}

// commitTransaction 提交事务
func (ds *DatabaseServer) commitTransaction(session *ClientSession) string {
	if !session.inTransaction {
		return "Error: No active transaction"
	}

	// 首先关闭只读事务
	if err := session.boltTx.Rollback(); err != nil {
		return fmt.Sprintf("Error closing read transaction: %v", err)
	}

	// 执行所有缓存的操作
	err := ds.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		for _, op := range session.transactionOps {
			switch op.Type {
			case "PUT":
				if err := b.Put([]byte(op.Key), []byte(op.Value)); err != nil {
					return err
				}
			case "DEL":
				if err := b.Delete([]byte(op.Key)); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Sprintf("Error committing transaction: %v", err)
	}

	// 清理事务状态
	session.inTransaction = false
	session.boltTx = nil
	session.transactionOps = make([]TransactionOperation, 0)
	return "COMMIT -> transaction committed"
}

// abortTransaction 中止事务
func (ds *DatabaseServer) abortTransaction(session *ClientSession) string {
	if !session.inTransaction {
		return "Error: No active transaction"
	}

	// 关闭只读事务
	if err := session.boltTx.Rollback(); err != nil {
		logErrorWithTimestamp("Error rolling back transaction for client %s: %v", session.ID, err)
	}

	// 清理事务状态，不执行任何操作
	session.inTransaction = false
	session.boltTx = nil
	session.transactionOps = make([]TransactionOperation, 0)
	return "ABORT -> transaction aborted"
}

// getValueFromCacheOrDB 从缓存或数据库获取值（使用BoltDB原生事务隔离）
func (ds *DatabaseServer) getValueFromCacheOrDB(session *ClientSession, key string) (string, bool) {
	if session.inTransaction {
		// 使用同一个BoltDB事务确保一致性读取
		b := session.boltTx.Bucket([]byte(bucketName))
		data := b.Get([]byte(key))
		if data != nil {
			return string(data), true
		}
		return "", false
	}

	// 非事务模式，直接从数据库获取最新值
	var value string
	var found bool
	err := ds.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		data := b.Get([]byte(key))
		if data != nil {
			value = string(data)
			found = true
		}
		return nil
	})

	if err != nil {
		return "", false
	}
	return value, found
}

// put 存储或更新键值对
func (ds *DatabaseServer) put(session *ClientSession, key, value string) string {
	// 检查value是否是表达式格式 (variable +/- const)
	exprPattern := regexp.MustCompile(`^\(([a-zA-Z_][a-zA-Z0-9_]*)\s*([+-])\s*(\d+)\)$`)
	matches := exprPattern.FindStringSubmatch(strings.TrimSpace(value))

	var finalValue string

	if len(matches) == 4 {
		// 处理表达式
		varName := matches[1]
		operator := matches[2]
		constStr := matches[3]

		constVal, err := strconv.Atoi(constStr)
		if err != nil {
			return fmt.Sprintf("Error: Invalid constant value: %s", constStr)
		}

		// 获取变量当前值
		currentValStr, found := ds.getValueFromCacheOrDB(session, varName)
		if !found {
			return fmt.Sprintf("Error: Variable %s does not exist", varName)
		}

		currentVal, err := strconv.Atoi(currentValStr)
		if err != nil {
			return fmt.Sprintf("Error: Variable %s is not a number", varName)
		}

		// 计算新值
		var newVal int
		if operator == "+" {
			newVal = currentVal + constVal
		} else { // operator == "-"
			newVal = currentVal - constVal
		}

		finalValue = strconv.Itoa(newVal)
	} else {
		// 尝试将value解析为整数
		_, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Sprintf("Error: Invalid value format: %s", value)
		}
		finalValue = value
	}

	if session.inTransaction {
		// 在事务中，添加到操作列表
		session.transactionOps = append(session.transactionOps, TransactionOperation{
			Type:  "PUT",
			Key:   key,
			Value: finalValue,
		})
		return fmt.Sprintf("PUT %s %s -> staged in transaction", key, value)
	} else {
		// 立即执行
		err := ds.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			return b.Put([]byte(key), []byte(finalValue))
		})

		if err != nil {
			return fmt.Sprintf("Error storing value: %v", err)
		}
		return fmt.Sprintf("PUT %s %s -> stored", key, value)
	}
}

// get 查询键对应的值
func (ds *DatabaseServer) get(session *ClientSession, key string) string {
	value, found := ds.getValueFromCacheOrDB(session, key)

	if found {
		return fmt.Sprintf("GET %s -> %s", key, value)
	} else {
		return fmt.Sprintf("GET %s -> Key not found", key)
	}
}

// delete 删除键值对
func (ds *DatabaseServer) delete(session *ClientSession, key string) string {
	if session.inTransaction {
		// 检查key是否存在于事务视图中
		_, exists := ds.getValueFromCacheOrDB(session, key)
		if !exists {
			return fmt.Sprintf("DEL %s -> Key not found", key)
		}

		// 在事务中，添加到操作列表
		session.transactionOps = append(session.transactionOps, TransactionOperation{
			Type: "DEL",
			Key:  key,
		})
		return fmt.Sprintf("DEL %s -> staged in transaction", key)
	} else {
		// 立即执行
		var found bool

		// 首先检查key是否存在
		err := ds.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			data := b.Get([]byte(key))
			if data != nil {
				found = true
			}
			return nil
		})

		if err != nil {
			return fmt.Sprintf("Error checking key: %v", err)
		}

		if found {
			// 删除键值对
			err = ds.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucketName))
				return b.Delete([]byte(key))
			})

			if err != nil {
				return fmt.Sprintf("Error deleting key: %v", err)
			}
			return fmt.Sprintf("DEL %s -> deleted", key)
		} else {
			return fmt.Sprintf("DEL %s -> Key not found", key)
		}
	}
}

// showAll 显示所有存储的数据
func (ds *DatabaseServer) showAll(session *ClientSession) string {
	var result strings.Builder
	var count int

	err := ds.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		result.WriteString("Current database contents:\n")
		for k, v := c.First(); k != nil; k, v = c.Next() {
			result.WriteString(fmt.Sprintf("  %s: %s\n", string(k), string(v)))
			count++
		}
		return nil
	})

	if err != nil {
		return fmt.Sprintf("Error reading database: %v", err)
	}

	if count == 0 {
		result.WriteString("Database is empty\n")
	}

	// 如果在事务中，显示事务状态
	if session.inTransaction {
		result.WriteString(fmt.Sprintf("Transaction status: %d operations pending", len(session.transactionOps)))
	}

	return strings.TrimSuffix(result.String(), "\n")
}

// getHelpMessage 获取帮助信息
func (ds *DatabaseServer) getHelpMessage() string {
	return `
Available commands:
  BEGIN                 - Start a new transaction
  COMMIT                - Commit the current transaction
  ABORT                 - Abort the current transaction
  PUT <key> <value>     - Store or update a key-value pair
                         Value can be integer or expression like (key+1)
  GET <key>            - Retrieve value for a key
  DEL <key>            - Delete a key-value pair
  SHOW                 - Show all stored data
  HELP                 - Show this help message
  EXIT/QUIT            - Disconnect from server

Transaction Example:
  BEGIN
  PUT A 1
  PUT B 2
  COMMIT

Expression Example:
  PUT A 3
  PUT B (A+1)

Client-Server Architecture:
  - Multiple clients can connect simultaneously
  - Each client has independent transaction state
  - Server manages the shared database
  - Use './client' to connect to server`
}

func main() {
	// 解析命令行参数
	port := flag.String("port", defaultPort, "Port to listen on")
	dbFile := flag.String("db", defaultDBFile, "Database file path")
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Println("Interactive Database Server")
		fmt.Println("Usage: server [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	// 启动时打印服务器信息
	logWithTimestamp("Interactive Database Server starting...")
	logWithTimestamp("Using database file: %s", *dbFile)

	server, err := NewDatabaseServer(*dbFile)
	if err != nil {
		logErrorWithTimestamp("Failed to initialize database server: %v", err)
		os.Exit(1)
	}
	defer server.Close()

	// 启动服务器
	if err := server.Start(*port); err != nil {
		logErrorWithTimestamp("Server error: %v", err)
		os.Exit(1)
	}
}
