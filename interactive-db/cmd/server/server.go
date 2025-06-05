package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"interactive-db/pkg/logger"
	cnpb "interactive-db/proto/coordinator"
	pb "interactive-db/proto/server"

	"github.com/meteorsky/bolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultPort       = "8080"
	defaultDBFile     = "interactive.db"
	bucketName        = "kv_store"
	lsnKey            = "__LSN__"        // LSN存储的特殊键
	defaultCNAddress  = "localhost:9091" // CN注册端口
	heartbeatInterval = 10 * time.Second // 调整为10秒，与CN保持一致
	heartbeatTimeout  = 20 * time.Second // 相应调整超时时间
	grpcPortOffset    = 100              // gRPC端口偏移量
	backupPortOffset  = 1000             // 备份端口偏移量
)

// NodeRole 节点角色
type NodeRole int

const (
	NodeRoleReplica NodeRole = iota
	NodeRolePrimary
)

func (r NodeRole) String() string {
	switch r {
	case NodeRolePrimary:
		return "Primary"
	case NodeRoleReplica:
		return "Replica"
	default:
		return "Unknown"
	}
}

// RegisterRequest 注册请求
type RegisterRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Port    string `json:"port"`
}

// HeartbeatRequest 心跳请求
type HeartbeatRequest struct {
	NodeID string `json:"node_id"`
	LSN    int64  `json:"lsn"`
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
	pb.UnimplementedDatabaseServiceServer
	db         *bolt.DB
	dbMu       sync.RWMutex // 保护数据库字段的并发访问
	clients    map[string]*ClientSession
	clientMu   sync.RWMutex
	port       string
	grpcPort   string // gRPC服务端口
	nodeID     string
	cnAddress  string
	cnClient   cnpb.CoordinatorServiceClient // CN客户端
	cnConn     *grpc.ClientConn              // CN gRPC连接
	role       NodeRole
	registered bool
	grpcServer *grpc.Server
	backupPort string
	// 新增字段用于连接管理
	connMu       sync.RWMutex
	connHealthy  bool
	lastConnTime time.Time
	atomicLSN    int64 // 原子LSN变量
	// 新增字段用于优雅关闭
	shutdownCh chan struct{}
	activeOps  sync.WaitGroup // 跟踪活跃的数据库操作
}

// NewDatabaseServer 创建新的数据库服务器
func NewDatabaseServer(dbFile, nodeID, cnAddress string) (*DatabaseServer, error) {
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

	server := &DatabaseServer{
		db:         db,
		clients:    make(map[string]*ClientSession),
		nodeID:     nodeID,
		cnAddress:  cnAddress,
		role:       NodeRoleReplica, // 默认从节点
		registered: false,
		grpcServer: grpc.NewServer(),
		shutdownCh: make(chan struct{}),
	}

	// 初始化LSN，如果不存在则设为0
	server.initializeLSN()

	// 如果指定了CN地址，建立gRPC连接
	if cnAddress != "" {
		conn, err := grpc.Dial(cnAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second, // 增加发送keepalive ping的时间间隔，与服务器端匹配
				Timeout:             10 * time.Second, // 增加等待keepalive ping应答的超时时间
				PermitWithoutStream: true,             // 允许在没有活跃流时发送keepalive ping
			}),
		)
		if err != nil {
			logger.Error("Failed to connect to coordinator: %v", err)
			// 不返回错误，允许在没有CN的情况下运行
		} else {
			server.cnClient = cnpb.NewCoordinatorServiceClient(conn)
			server.cnConn = conn
			server.connHealthy = true
			server.lastConnTime = time.Now()
			logger.Info("Connected to coordinator at %s", cnAddress)
		}
	}

	// 注册gRPC服务
	pb.RegisterDatabaseServiceServer(server.grpcServer, server)

	return server, nil
}

// Close 关闭服务器
func (ds *DatabaseServer) Close() error {
	// 发送关闭信号
	close(ds.shutdownCh)

	// 等待所有活跃操作完成
	ds.activeOps.Wait()

	ds.dbMu.Lock()
	defer ds.dbMu.Unlock()

	if ds.db != nil {
		return ds.db.Close()
	}
	return nil
}

// initializeLSN 初始化LSN
func (ds *DatabaseServer) initializeLSN() {
	var currentLSN int64 = 0

	err := ds.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		data := b.Get([]byte(lsnKey))
		if data == nil {
			// LSN不存在，初始化为0
			if err := b.Put([]byte(lsnKey), []byte("0")); err != nil {
				return err
			}
			currentLSN = 0
		} else {
			// LSN存在，解析现有值
			if lsn, err := strconv.ParseInt(string(data), 10, 64); err == nil {
				currentLSN = lsn
			}
		}
		return nil
	})

	if err != nil {
		logger.Error("Failed to initialize LSN: %v", err)
	} else {
		// 设置原子变量
		atomic.StoreInt64(&ds.atomicLSN, currentLSN)
		logger.Info("LSN initialized to: %d", currentLSN)
	}
}

// incrementLSN 递增LSN
func (ds *DatabaseServer) incrementLSN() {
	// 原子递增LSN
	newLSN := atomic.AddInt64(&ds.atomicLSN, 1)

	// 异步更新数据库中的LSN值
	go func() {
		err := ds.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			return b.Put([]byte(lsnKey), []byte(strconv.FormatInt(newLSN, 10)))
		})
		if err != nil {
			logger.Error("Failed to persist LSN %d: %v", newLSN, err)
		}
	}()
}

// getLSN 获取当前LSN
func (ds *DatabaseServer) getLSN() int64 {
	return atomic.LoadInt64(&ds.atomicLSN)
}

// safeDbOperation 安全执行数据库操作，避免与恢复操作冲突
func (ds *DatabaseServer) safeDbOperation(operation func() error) error {
	// 检查是否正在关闭
	select {
	case <-ds.shutdownCh:
		return fmt.Errorf("server is shutting down")
	default:
	}

	// 增加活跃操作计数
	ds.activeOps.Add(1)
	defer ds.activeOps.Done()

	// 获取读锁，允许多个数据库操作并发，但阻止恢复操作
	ds.dbMu.RLock()
	defer ds.dbMu.RUnlock()

	// 再次检查是否正在关闭
	select {
	case <-ds.shutdownCh:
		return fmt.Errorf("server is shutting down")
	default:
	}

	return operation()
}

// registerToCN 注册到协调节点
func (ds *DatabaseServer) registerToCN(port string) error {
	if ds.cnClient == nil {
		return fmt.Errorf("coordinator client not initialized")
	}

	logger.Info("Attempting to register to coordinator via gRPC")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 发送注册请求
	req := &cnpb.RegisterRequest{
		NodeId:  ds.nodeID,
		Address: "localhost", // 可以从配置获取
		Port:    port,
		Lsn:     ds.getLSN(), // 添加当前LSN用于选举决策
	}

	logger.Info("Sending registration request: %+v", req)
	resp, err := ds.cnClient.RegisterServer(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register to coordinator: %v", err)
	}

	logger.Info("Registration response received: %+v", resp)
	if resp.Success {
		ds.registered = true
		ds.port = port // 保存端口信息
		logger.Info("Successfully registered to coordinator as %s", ds.nodeID)
		if resp.Role != "" {
			logger.Info("Assigned role: %s", resp.Role)
		}
		return nil
	}

	return fmt.Errorf("registration failed: %s", resp.Message)
}

// startHeartbeat 启动心跳
func (ds *DatabaseServer) startHeartbeat() {
	if !ds.registered || ds.cnClient == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for range ticker.C {
			ds.sendHeartbeat()
		}
	}()
}

// startLSNMonitor 启动LSN监控，定期打印当前LSN
func (ds *DatabaseServer) startLSNMonitor() {
	go func() {
		ticker := time.NewTicker(3 * time.Second) // 每30秒打印一次LSN
		defer ticker.Stop()

		for range ticker.C {
			currentLSN := ds.getLSN()
			logger.Info("Current LSN: %d", currentLSN)
		}
	}()
}

// sendHeartbeat 发送心跳
func (ds *DatabaseServer) sendHeartbeat() {
	if ds.cnClient == nil {
		return
	}

	// 检查连接健康状态
	if !ds.checkConnectionHealth() {
		logger.Info("Connection unhealthy, attempting to reconnect...")
		if err := ds.reconnectToCN(); err != nil {
			logger.Error("Failed to reconnect to coordinator: %v", err)
			return
		}
		// 重新注册
		if err := ds.registerToCN(ds.port); err != nil {
			logger.Error("Failed to re-register after reconnection: %v", err)
			return
		}
	}

	// 使用更短的初始超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 发送心跳请求
	req := &cnpb.HeartbeatRequest{
		NodeId: ds.nodeID,
		Lsn:    ds.getLSN(),
	}

	resp, err := ds.cnClient.Heartbeat(ctx, req)
	if err != nil {
		// 只记录错误，不立即重试，让下一个心跳周期处理
		logger.Error("Failed to send heartbeat: %v", err)

		// 如果是 context deadline exceeded 错误，标记连接为不健康
		if strings.Contains(err.Error(), "context deadline exceeded") ||
			strings.Contains(err.Error(), "connection refused") {
			logger.Info("LOCK: Acquiring connMu.Lock() for marking connection unhealthy in heartbeat")
			ds.connMu.Lock()
			logger.Info("LOCK: Acquired connMu.Lock() for marking connection unhealthy in heartbeat")
			ds.connHealthy = false
			logger.Info("UNLOCK: Releasing connMu.Unlock() after marking connection unhealthy in heartbeat")
			ds.connMu.Unlock()
			logger.Info("UNLOCK: Released connMu.Unlock() after marking connection unhealthy in heartbeat")
		}
		return
	}

	if !resp.Success {
		logger.Error("Heartbeat rejected by coordinator")
	}
}

// Start 启动服务器
func (ds *DatabaseServer) Start(port string) error {
	ds.port = port
	serverAddr := ":" + port

	// 计算gRPC端口
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port number: %v", err)
	}
	ds.grpcPort = strconv.Itoa(portNum + grpcPortOffset)

	// 启动gRPC服务器
	grpcListener, err := net.Listen("tcp", ":"+ds.grpcPort)
	if err != nil {
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}

	go func() {
		logger.Info("gRPC server started on port %s", ds.grpcPort)
		if err := ds.grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server error: %v", err)
		}
	}()

	// 启动TCP服务器（用于客户端连接）
	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	defer listener.Close()

	logger.Info("Database server started on port %s", port)
	logger.Info("gRPC service available on port %s", ds.grpcPort)
	logger.Info("Waiting for clients...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept connection: %v", err)
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

		logger.Info("Client %s connected from %s", clientID, conn.RemoteAddr())
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
		logger.Info("Client %s disconnected", session.ID)
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

		// 记录收到的命令，对于包含大量数据的命令只记录命令名称
		parts := strings.Fields(strings.TrimSpace(command))
		if len(parts) > 0 {
			cmdName := strings.ToUpper(parts[0])
			if cmdName == "RESTORE" || cmdName == "BACKUP" {
				logger.Info("Client %s executed: %s (data size: %d bytes)", session.ID, cmdName, len(command))
			} else {
				logger.Info("Client %s executed: %s", session.ID, command)
			}
		} else {
			logger.Info("Client %s executed: %s", session.ID, command)
		}

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
		logger.Error("Failed to send response to client %s: %v", session.ID, err)
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
		// 特殊命令：获取LSN
		if key == "__lsn__" {
			return fmt.Sprintf("GET __lsn__ -> %d", ds.getLSN())
		}
		return ds.get(session, key)

	case "DEL":
		if len(parts) != 2 {
			return "Error: DEL requires exactly one key"
		}
		key := parts[1]
		return ds.delete(session, key)

	case "BACKUP":
		return ds.backupDatabase(session)

	case "RESTORE":
		if len(parts) < 2 {
			return "Error: RESTORE requires backup data"
		}
		backupData := strings.Join(parts[1:], " ")
		return ds.restoreDatabase(session, backupData)

	case "EXPORT":
		return ds.exportDatabase(session)

	case "IMPORT":
		if len(parts) < 2 {
			return "Error: IMPORT requires data"
		}
		data := strings.Join(parts[1:], " ")
		return ds.importDatabase(session, data)

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

	// 检查事务状态一致性
	if session.boltTx == nil {
		// 重置不一致的状态
		session.inTransaction = false
		session.transactionOps = make([]TransactionOperation, 0)
		return "Error: Transaction state is inconsistent"
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

	// 递增LSN
	ds.incrementLSN()

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

	// 检查事务状态一致性并关闭只读事务
	if session.boltTx != nil {
		if err := session.boltTx.Rollback(); err != nil {
			logger.Error("Error rolling back transaction for client %s: %v", session.ID, err)
		}
	}

	// 清理事务状态，不执行任何操作
	session.inTransaction = false
	session.boltTx = nil
	session.transactionOps = make([]TransactionOperation, 0)
	return "ABORT -> transaction aborted"
}

// getValueFromCacheOrDB 从缓存或数据库获取值（使用BoltDB原生事务隔离）
func (ds *DatabaseServer) getValueFromCacheOrDB(session *ClientSession, key string) (string, bool) {
	if session.inTransaction && session.boltTx != nil {
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

	if session.inTransaction && session.boltTx != nil {
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

		// 递增LSN
		ds.incrementLSN()
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
	if session.inTransaction && session.boltTx != nil {
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

			// 递增LSN
			ds.incrementLSN()
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
			// 跳过LSN键
			if string(k) != lsnKey {
				result.WriteString(fmt.Sprintf("  %s: %s\n", string(k), string(v)))
				count++
			}
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
	if session.inTransaction && session.boltTx != nil {
		result.WriteString(fmt.Sprintf("Transaction status: %d operations pending", len(session.transactionOps)))
	}

	return strings.TrimSuffix(result.String(), "\n")
}

// exportDatabase 导出数据库
func (ds *DatabaseServer) exportDatabase(session *ClientSession) string {
	var exports []string

	err := ds.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			// 跳过LSN键
			if string(k) != lsnKey {
				exports = append(exports, fmt.Sprintf("%s:%s", string(k), string(v)))
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Sprintf("Error exporting database: %v", err)
	}

	if len(exports) == 0 {
		return "EXPORT -> database is empty"
	}

	return fmt.Sprintf("EXPORT -> %s", strings.Join(exports, ","))
}

// importDatabase 导入数据库
func (ds *DatabaseServer) importDatabase(session *ClientSession, data string) string {
	if data == "" || data == "database is empty" {
		return "IMPORT -> no data to import"
	}

	pairs := strings.Split(data, ",")
	imported := 0

	err := ds.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		for _, pair := range pairs {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				key, value := parts[0], parts[1]
				if err := b.Put([]byte(key), []byte(value)); err != nil {
					return err
				}
				imported++
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Sprintf("Error importing database: %v", err)
	}

	// 递增LSN
	ds.incrementLSN()
	return fmt.Sprintf("IMPORT -> imported %d records", imported)
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

// backupDatabase 使用BoltDB原生备份
func (ds *DatabaseServer) backupDatabase(session *ClientSession) string {
	var buffer bytes.Buffer

	err := ds.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buffer)
		return err
	})

	if err != nil {
		return fmt.Sprintf("Error creating backup: %v", err)
	}

	// 将二进制数据编码为base64字符串传输
	backupData := base64.StdEncoding.EncodeToString(buffer.Bytes())
	return fmt.Sprintf("BACKUP -> %s", backupData)
}

// restoreDatabase 从备份恢复数据库
func (ds *DatabaseServer) restoreDatabase(session *ClientSession, backupData string) string {
	// 解码base64数据
	data, err := base64.StdEncoding.DecodeString(backupData)
	if err != nil {
		return fmt.Sprintf("Error decoding backup data")
	}

	// 创建临时文件
	tempFile := ds.db.Path() + ".temp"
	if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Sprintf("Error writing temporary backup file: %v", err)
	}

	// 保存原始路径
	originalPath := ds.db.Path()

	// 暂停心跳发送，避免在数据库恢复期间出现锁竞争
	logger.Info("Suspending heartbeat during database restore")
	ds.connMu.Lock()
	ds.connHealthy = false // 暂时标记为不健康，避免发送心跳
	ds.connMu.Unlock()

	// 确保无论成功还是失败都恢复心跳
	defer func() {
		ds.connMu.Lock()
		ds.connHealthy = true
		ds.connMu.Unlock()
	}()

	// 获取写锁来保护数据库替换操作
	logger.Info("LOCK: Acquiring dbMu.Lock() for database replacement operation")
	ds.dbMu.Lock()
	logger.Info("LOCK: Acquired dbMu.Lock() for database replacement operation")
	defer func() {
		logger.Info("UNLOCK: Releasing dbMu.Unlock() after database replacement operation (defer)")
		ds.dbMu.Unlock()
		logger.Info("UNLOCK: Released dbMu.Unlock() after database replacement operation (defer)")
	}()

	// 关闭当前数据库
	logger.Info("Closing current database")
	if err := ds.db.ForceClose(); err != nil {
		logger.Error("Failed to close current database: %v", err)
		os.Remove(tempFile)
		return fmt.Sprintf("failed to close database: %v", err)
	}
	logger.Info("Current database closed successfully")

	// 直接替换数据库文件
	logger.Info("Replacing database file")
	if err := os.Rename(tempFile, originalPath); err != nil {
		logger.Error("Failed to replace database file: %v", err)
		os.Remove(tempFile)
		return fmt.Sprintf("failed to replace database: %v", err)
	}
	logger.Info("Database file replaced successfully")

	// 重新打开数据库
	logger.Info("Reopening database")
	db, err := bolt.Open(originalPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		logger.Error("Failed to reopen database: %v", err)
		os.Remove(originalPath) // 删除损坏的文件
		return fmt.Sprintf("failed to reopen database: %v", err)
	}
	logger.Info("Database reopened successfully")

	ds.db = db
	// 重新初始化LSN以同步原子变量与恢复的数据库
	ds.initializeLSN()

	// 确保bucket存在
	err = ds.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})

	if err != nil {
		logger.Error("Failed to create bucket after restore: %v", err)
		return fmt.Sprintf("failed to create bucket: %v", err)
	}
	logger.Info("Bucket created successfully")

	logger.Info("gRPC stream restore completed successfully")
	return "RESTORE -> database restored successfully"
}

// ExecuteCommand 实现gRPC ExecuteCommand方法
func (ds *DatabaseServer) ExecuteCommand(ctx context.Context, req *pb.CommandRequest) (*pb.CommandResponse, error) {
	// 创建临时会话来执行命令
	session := &ClientSession{
		ID:             fmt.Sprintf("grpc_%d", time.Now().UnixNano()),
		conn:           nil,
		inTransaction:  false,
		boltTx:         nil,
		transactionOps: make([]TransactionOperation, 0),
	}

	result := ds.executeCommand(session, req.Command)

	return &pb.CommandResponse{
		Result:  result,
		Success: !strings.HasPrefix(result, "Error:"),
	}, nil
}

// GetLSN 实现gRPC GetLSN方法
func (ds *DatabaseServer) GetLSN(ctx context.Context, _ *pb.Empty) (*pb.LSNResponse, error) {
	return &pb.LSNResponse{
		Lsn: ds.getLSN(),
	}, nil
}

// StreamBackup 实现gRPC StreamBackup方法
func (ds *DatabaseServer) StreamBackup(req *pb.BackupRequest, stream pb.DatabaseService_StreamBackupServer) error {
	logger.Info("Starting gRPC stream backup")

	// 将数据写入临时buffer以获取大小
	var buffer bytes.Buffer
	err := ds.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buffer)
		return err
	})

	if err != nil {
		logger.Error("Failed to create backup: %v", err)
		return fmt.Errorf("failed to create backup: %v", err)
	}

	data := buffer.Bytes()
	totalSize := int64(len(data))
	chunkSize := 64 * 1024 // 64KB chunks

	logger.Info("Backup data size: %d bytes", totalSize)

	// 分块发送数据
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &pb.BackupChunk{
			Data:      data[offset:end],
			TotalSize: totalSize,
			IsLast:    end == len(data),
		}

		if err := stream.Send(chunk); err != nil {
			logger.Error("Failed to send backup chunk: %v", err)
			return err
		}

		logger.Info("Sent backup chunk: offset=%d, size=%d", offset, end-offset)
	}

	logger.Info("gRPC stream backup completed successfully")
	return nil
}

// StreamRestore 实现gRPC StreamRestore方法
func (ds *DatabaseServer) StreamRestore(stream pb.DatabaseService_StreamRestoreServer) error {
	logger.Info("Starting gRPC stream restore")

	var buffer bytes.Buffer
	var totalSize int64
	var bytesReceived int64

	// 接收所有数据块
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("Failed to receive restore chunk: %v", err)
			return err
		}

		// 第一个块包含总大小信息
		if totalSize == 0 {
			totalSize = chunk.TotalSize
			logger.Info("Expected restore data size: %d bytes", totalSize)
		}

		buffer.Write(chunk.Data)
		bytesReceived += int64(len(chunk.Data))

		if chunk.IsLast {
			logger.Info("Received last chunk, total bytes: %d", bytesReceived)
			break
		}
	}

	if bytesReceived != totalSize {
		err := fmt.Errorf("size mismatch: received %d bytes, expected %d bytes", bytesReceived, totalSize)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: err.Error(),
		})
	}

	// 创建临时文件
	tempFile := ds.db.Path() + ".temp"
	if err := ioutil.WriteFile(tempFile, buffer.Bytes(), 0644); err != nil {
		logger.Error("Failed to write temp file: %v", err)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("failed to write temp file: %v", err),
		})
	}

	// 验证数据库文件
	if err := ds.validateDatabaseFile(tempFile); err != nil {
		logger.Error("Invalid database file received: %v", err)
		os.Remove(tempFile)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("invalid database file: %v", err),
		})
	}

	// 保存原始路径
	originalPath := ds.db.Path()

	// 暂停心跳发送，避免在数据库恢复期间出现锁竞争
	logger.Info("Suspending heartbeat during database restore")
	ds.connMu.Lock()
	ds.connHealthy = false // 暂时标记为不健康，避免发送心跳
	ds.connMu.Unlock()

	// 确保无论成功还是失败都恢复心跳
	defer func() {
		ds.connMu.Lock()
		ds.connHealthy = true
		ds.connMu.Unlock()
	}()

	// 获取写锁来保护数据库替换操作
	logger.Info("LOCK: Acquiring dbMu.Lock() for database replacement operation")
	ds.dbMu.Lock()
	logger.Info("LOCK: Acquired dbMu.Lock() for database replacement operation")
	defer func() {
		logger.Info("UNLOCK: Releasing dbMu.Unlock() after database replacement operation (defer)")
		ds.dbMu.Unlock()
		logger.Info("UNLOCK: Released dbMu.Unlock() after database replacement operation (defer)")
	}()

	// 关闭当前数据库
	logger.Info("Closing current database")
	if err := ds.db.Close(); err != nil {
		logger.Error("Failed to close current database: %v", err)
		os.Remove(tempFile)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("failed to close database: %v", err),
		})
	}
	logger.Info("Current database closed successfully")

	// 直接替换数据库文件
	logger.Info("Replacing database file")
	if err := os.Rename(tempFile, originalPath); err != nil {
		logger.Error("Failed to replace database file: %v", err)
		os.Remove(tempFile)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("failed to replace database: %v", err),
		})
	}
	logger.Info("Database file replaced successfully")

	// 重新打开数据库
	logger.Info("Reopening database")
	db, err := bolt.Open(originalPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		logger.Error("Failed to reopen database: %v", err)
		os.Remove(originalPath) // 删除损坏的文件
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("failed to reopen database: %v", err),
		})
	}
	logger.Info("Database reopened successfully")

	ds.db = db
	// 重新初始化LSN以同步原子变量与恢复的数据库
	ds.initializeLSN()

	// 确保bucket存在
	err = ds.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})

	if err != nil {
		logger.Error("Failed to create bucket after restore: %v", err)
		return stream.SendAndClose(&pb.RestoreResponse{
			Success: false,
			Message: fmt.Sprintf("failed to create bucket: %v", err),
		})
	}
	logger.Info("Bucket created successfully")

	logger.Info("gRPC stream restore completed successfully")
	return stream.SendAndClose(&pb.RestoreResponse{
		Success: true,
		Message: "restore completed successfully",
	})
}

// validateDatabaseFile 验证数据库文件是否有效
func (ds *DatabaseServer) validateDatabaseFile(filePath string) error {
	// 尝试打开数据库文件进行验证
	db, err := bolt.Open(filePath, 0600, &bolt.Options{
		Timeout:  5 * time.Second,
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("cannot open database file: %v", err)
	}
	defer db.Close()

	// 验证数据库结构
	err = db.View(func(tx *bolt.Tx) error {
		// 检查是否可以正常读取数据库
		return nil
	})

	if err != nil {
		return fmt.Errorf("database structure validation failed: %v", err)
	}

	return nil
}

// checkConnectionHealth 检查连接健康状态
func (ds *DatabaseServer) checkConnectionHealth() bool {
	ds.connMu.RLock()
	healthy := ds.connHealthy
	ds.connMu.RUnlock()
	return healthy
}

// reconnectToCN 重连到协调节点
func (ds *DatabaseServer) reconnectToCN() error {
	if ds.cnAddress == "" {
		return fmt.Errorf("no coordinator address specified")
	}

	logger.Info("Attempting to reconnect to coordinator at %s", ds.cnAddress)

	// 关闭旧连接
	ds.connMu.Lock()
	if ds.cnConn != nil {
		ds.cnConn.Close()
	}
	ds.connHealthy = false
	ds.connMu.Unlock()

	// 建立新连接
	conn, err := grpc.Dial(ds.cnAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // 增加发送keepalive ping的时间间隔，与服务器端匹配
			Timeout:             10 * time.Second, // 增加等待keepalive ping应答的超时时间
			PermitWithoutStream: true,             // 允许在没有活跃流时发送keepalive ping
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to reconnect to coordinator: %v", err)
	}

	logger.Info("LOCK: Acquiring connMu.Lock() for setting new connection in reconnect")
	ds.connMu.Lock()
	logger.Info("LOCK: Acquired connMu.Lock() for setting new connection in reconnect")
	ds.cnClient = cnpb.NewCoordinatorServiceClient(conn)
	ds.cnConn = conn
	ds.connHealthy = true
	ds.lastConnTime = time.Now()
	logger.Info("UNLOCK: Releasing connMu.Unlock() after setting new connection in reconnect")
	ds.connMu.Unlock()
	logger.Info("UNLOCK: Released connMu.Unlock() after setting new connection in reconnect")

	logger.Info("Successfully reconnected to coordinator")
	return nil
}

func main() {

	// 解析命令行参数
	port := flag.String("port", defaultPort, "Port to listen on")
	dbFile := flag.String("db", defaultDBFile, "Database file path")
	nodeID := flag.String("id", "", "Node ID (required for distributed mode)")
	cnAddress := flag.String("cn", defaultCNAddress, "Coordinator address")
	standalone := flag.Bool("standalone", false, "Run in standalone mode (no coordinator)")
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Println("Distributed Database Server")
		fmt.Println("Usage: server [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	// 生成默认nodeID如果未提供
	if *nodeID == "" {
		*nodeID = fmt.Sprintf("server_%d", time.Now().UnixNano())
	}

	logger.Init()
	logger.SetComponent("SERVER-" + *nodeID)
	defer logger.Close()

	// 启动时打印服务器信息
	logger.Info("Distributed Database Server starting...")
	logger.Info("Node ID: %s", *nodeID)
	logger.Info("Using database file: %s", *dbFile)

	var server *DatabaseServer
	var err error

	if *standalone {
		// 独立模式，不注册到协调节点
		server, err = NewDatabaseServer(*dbFile, *nodeID, "")
		logger.Info("Running in standalone mode")
	} else {
		// 分布式模式，注册到协调节点
		server, err = NewDatabaseServer(*dbFile, *nodeID, *cnAddress)
		logger.Info("Coordinator address: %s", *cnAddress)
	}

	if err != nil {
		logger.Error("Failed to initialize database server: %v", err)
		os.Exit(1)
	}
	defer server.Close()

	// 如果不是独立模式，尝试注册到协调节点
	if !*standalone {
		if err := server.registerToCN(*port); err != nil {
			logger.Error("Failed to register to coordinator: %v", err)
			logger.Info("Starting in standalone mode...")
		} else {
			// 启动心跳
			server.startHeartbeat()
		}
	}

	// 启动LSN监控
	server.startLSNMonitor()

	// 启动服务器
	if err := server.Start(*port); err != nil {
		logger.Error("Server error: %v", err)
		os.Exit(1)
	}
}
