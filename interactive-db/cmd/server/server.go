package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
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
	"time"

	"interactive-db/pkg/logger"

	"github.com/meteorsky/bolt"
)

const (
	defaultPort       = "8080"
	defaultDBFile     = "interactive.db"
	bucketName        = "kv_store"
	lsnKey            = "__LSN__"        // LSN存储的特殊键
	defaultCNAddress  = "localhost:9091" // CN注册端口
	heartbeatInterval = 3 * time.Second
	backupPortOffset  = 1000 // 备份端口偏移量
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
	db         *bolt.DB
	clients    map[string]*ClientSession
	clientMu   sync.RWMutex
	port       string
	backupPort string
	nodeID     string
	cnAddress  string
	role       NodeRole
	registered bool
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
	}

	// 初始化LSN，如果不存在则设为0
	server.initializeLSN()

	return server, nil
}

// Close 关闭服务器
func (ds *DatabaseServer) Close() error {
	if ds.db != nil {
		return ds.db.Close()
	}
	return nil
}

// initializeLSN 初始化LSN
func (ds *DatabaseServer) initializeLSN() {
	err := ds.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		data := b.Get([]byte(lsnKey))
		if data == nil {
			// LSN不存在，初始化为0
			return b.Put([]byte(lsnKey), []byte("0"))
		}
		return nil
	})
	if err != nil {
		logger.Error("Failed to initialize LSN: %v", err)
	}
}

// incrementLSN 递增LSN
func (ds *DatabaseServer) incrementLSN() {
	err := ds.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		data := b.Get([]byte(lsnKey))
		currentLSN := int64(0)
		if data != nil {
			if lsn, err := strconv.ParseInt(string(data), 10, 64); err == nil {
				currentLSN = lsn
			}
		}
		currentLSN++
		return b.Put([]byte(lsnKey), []byte(strconv.FormatInt(currentLSN, 10)))
	})
	if err != nil {
		logger.Error("Failed to increment LSN: %v", err)
	}
}

// getLSN 获取当前LSN
func (ds *DatabaseServer) getLSN() int64 {
	var lsn int64 = 0
	err := ds.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		data := b.Get([]byte(lsnKey))
		if data != nil {
			if parsedLSN, err := strconv.ParseInt(string(data), 10, 64); err == nil {
				lsn = parsedLSN
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("Failed to get LSN: %v", err)
	}
	return lsn
}

// registerToCN 注册到协调节点
func (ds *DatabaseServer) registerToCN(port string) error {
	if ds.cnAddress == "" {
		return fmt.Errorf("coordinator address not specified")
	}

	logger.Info("Attempting to register to coordinator at %s", ds.cnAddress)
	conn, err := net.Dial("tcp", ds.cnAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	// 发送注册请求
	req := RegisterRequest{
		NodeID:  ds.nodeID,
		Address: "localhost", // 可以从配置获取
		Port:    port,
	}

	logger.Info("Sending registration request: %+v", req)
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		return fmt.Errorf("failed to send registration request: %v", err)
	}

	logger.Info("Registration request sent, waiting for response...")
	// 读取响应
	decoder := json.NewDecoder(conn)
	var response map[string]interface{}
	if err := decoder.Decode(&response); err != nil {
		// 尝试读取原始响应内容
		conn.Close()
		conn, err = net.Dial("tcp", ds.cnAddress)
		if err != nil {
			return fmt.Errorf("failed to reconnect to coordinator: %v", err)
		}
		// 重新发送请求
		encoder = json.NewEncoder(conn)
		if err := encoder.Encode(req); err != nil {
			return fmt.Errorf("failed to resend registration request: %v", err)
		}
		// 读取原始响应查看内容
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		logger.Info("Raw response received: %s", string(buf[:n]))
		return fmt.Errorf("failed to decode registration response: %v", err)
	}

	logger.Info("Registration response received: %+v", response)
	if status, ok := response["status"].(string); ok && status == "success" {
		ds.registered = true
		ds.port = port // 保存端口信息
		logger.Info("Successfully registered to coordinator as %s", ds.nodeID)
		if role, ok := response["role"].(string); ok {
			logger.Info("Assigned role: %s", role)
		}
		return nil
	}

	return fmt.Errorf("registration failed: %v", response)
}

// startHeartbeat 启动心跳
func (ds *DatabaseServer) startHeartbeat() {
	if !ds.registered {
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

// sendHeartbeat 发送心跳
func (ds *DatabaseServer) sendHeartbeat() {
	if ds.cnAddress == "" {
		return
	}

	// 连接到CN的心跳端口
	heartbeatAddr := strings.Replace(ds.cnAddress, "9091", "9092", 1) // 心跳端口
	conn, err := net.Dial("tcp", heartbeatAddr)
	if err != nil {
		logger.Error("Failed to connect to coordinator for heartbeat: %v", err)
		return
	}
	defer conn.Close()

	// 发送心跳请求
	heartbeat := HeartbeatRequest{
		NodeID: ds.nodeID,
		LSN:    ds.getLSN(),
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(heartbeat); err != nil {
		logger.Error("Failed to send heartbeat: %v", err)
		return
	}

	logger.Info("Heartbeat sent (LSN: %d)", ds.getLSN())
}

// Start 启动服务器
func (ds *DatabaseServer) Start(port string) error {
	ds.port = port
	serverAddr := ":" + port

	// 计算备份端口
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid port number: %v", err)
	}
	ds.backupPort = strconv.Itoa(portNum + backupPortOffset)

	listener, err := net.Listen("tcp", serverAddr)
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	defer listener.Close()

	// 启动备份服务器
	go ds.startBackupServer()

	logger.Info("Database server started on port %s", port)
	logger.Info("Backup server started on port %s", ds.backupPort)
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

// startBackupServer 启动备份服务器
func (ds *DatabaseServer) startBackupServer() {
	backupAddr := ":" + ds.backupPort
	listener, err := net.Listen("tcp", backupAddr)
	if err != nil {
		logger.Error("Failed to start backup server: %v", err)
		return
	}
	defer listener.Close()

	logger.Info("Backup server listening on port %s", ds.backupPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept backup connection: %v", err)
			continue
		}

		go ds.handleBackupConnection(conn)
	}
}

// handleBackupConnection 处理备份服务连接
func (ds *DatabaseServer) handleBackupConnection(conn net.Conn) {
	defer func() {
		// 确保连接在函数退出时关闭
		if err := conn.Close(); err != nil {
			logger.Error("Failed to close backup connection: %v", err)
		}
	}()

	// 设置连接超时
	conn.SetDeadline(time.Now().Add(10 * time.Minute))

	// 读取请求类型
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		logger.Error("Failed to read backup request")
		return
	}

	request := strings.TrimSpace(scanner.Text())
	logger.Info("Backup request: %s", request)

	switch request {
	case "BACKUP":
		ds.handleStreamBackup(conn)
	case "RESTORE":
		ds.handleStreamRestore(conn)
	default:
		logger.Error("Unknown backup request: %s", request)
		conn.Write([]byte("ERROR Unknown request\n"))
	}
}

// handleStreamBackup 处理流式备份
func (ds *DatabaseServer) handleStreamBackup(conn net.Conn) {
	logger.Info("Starting stream backup")

	// 先将数据写入临时buffer以获取大小
	var buffer bytes.Buffer
	err := ds.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buffer)
		return err
	})

	if err != nil {
		logger.Error("Failed to create backup: %v", err)
		conn.Write([]byte("ERROR Failed to create backup\n"))
		return
	}

	dataSize := buffer.Len()
	logger.Info("Backup data size: %d bytes", dataSize)

	// 发送成功响应和数据大小
	response := fmt.Sprintf("OK %d\n", dataSize)
	_, err = conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to send response: %v", err)
		return
	}

	// 确保响应被发送
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	// 发送实际数据
	bytesWritten, err := io.Copy(conn, &buffer)
	if err != nil {
		logger.Error("Failed to stream backup data: %v", err)
		return
	}

	if bytesWritten != int64(dataSize) {
		logger.Error("Incomplete data transmission: wrote %d bytes, expected %d bytes", bytesWritten, dataSize)
		return
	}

	// 确保所有数据都被发送
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.CloseWrite() // 关闭写端，但保持读端开放以接收确认
	}

	logger.Info("Stream backup completed successfully, sent %d bytes", bytesWritten)

	// 等待客户端确认接收完成（可选）
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	ackBuffer := make([]byte, 10)
	_, err = conn.Read(ackBuffer)
	if err != nil && err != io.EOF {
		logger.Info("No acknowledgment received from client (this is normal): %v", err)
	}
}

// handleStreamRestore 处理流式恢复 - 增强版本
func (ds *DatabaseServer) handleStreamRestore(conn net.Conn) {
	logger.Info("Starting stream restore")

	// 读取数据大小
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		logger.Error("Failed to read data size")
		conn.Write([]byte("ERROR Failed to read data size\n"))
		return
	}

	sizeStr := strings.TrimSpace(scanner.Text())
	expectedSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		logger.Error("Invalid data size: %s", sizeStr)
		conn.Write([]byte("ERROR Invalid data size\n"))
		return
	}

	logger.Info("Expecting %d bytes for restore", expectedSize)

	// 发送确认响应并确保立即发送
	_, err = conn.Write([]byte("OK\n"))
	if err != nil {
		logger.Error("Failed to send OK response: %v", err)
		return
	}

	// 确保响应被立即发送
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	logger.Info("Sent OK response for restore, waiting for data stream")

	// 创建临时文件
	tempFile := ds.db.Path() + ".temp"
	file, err := os.Create(tempFile)
	if err != nil {
		logger.Error("Failed to create temp file: %v", err)
		return
	}

	// 使用分块接收以提高稳定性
	bytesReceived, err := ds.receiveDataInChunks(conn, file, expectedSize)
	file.Close()

	if err != nil {
		logger.Error("Failed to receive backup data: %v", err)
		os.Remove(tempFile)
		return
	}

	if bytesReceived != expectedSize {
		logger.Error("Received %d bytes, expected %d bytes", bytesReceived, expectedSize)
		os.Remove(tempFile)
		return
	}

	logger.Info("Received exactly %d bytes for restore", bytesReceived)

	// 验证接收到的数据库文件是否有效
	if err := ds.validateDatabaseFile(tempFile); err != nil {
		logger.Error("Invalid database file received: %v", err)
		os.Remove(tempFile)
		return
	}

	logger.Info("Database file validation passed")

	// 保存原始路径
	originalPath := ds.db.Path()
	backupPath := originalPath + ".backup"

	// 创建当前数据库的备份
	if err := ds.createBackupFile(originalPath, backupPath); err != nil {
		logger.Error("Failed to create backup of current database: %v", err)
		os.Remove(tempFile)
		return
	}

	// 关闭当前数据库
	if err := ds.db.Close(); err != nil {
		logger.Error("Failed to close current database: %v", err)
		os.Remove(tempFile)
		return
	}

	// 替换数据库文件
	if err := os.Rename(tempFile, originalPath); err != nil {
		logger.Error("Failed to replace database file: %v", err)
		// 尝试恢复原始数据库
		ds.recoverFromBackup(originalPath, backupPath)
		os.Remove(tempFile)
		return
	}

	// 重新打开数据库
	db, err := bolt.Open(originalPath, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		logger.Error("Failed to reopen database: %v", err)
		// 尝试恢复原始数据库
		ds.recoverFromBackup(originalPath, backupPath)
		return
	}

	ds.db = db

	// 确保bucket存在并验证数据完整性
	err = ds.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})

	if err != nil {
		logger.Error("Failed to create bucket after restore: %v", err)
		ds.db.Close()
		ds.recoverFromBackup(originalPath, backupPath)
		return
	}

	// 删除备份文件（成功恢复后）
	os.Remove(backupPath)
	logger.Info("Stream restore completed successfully")
}

// receiveDataInChunks 分块接收数据
func (ds *DatabaseServer) receiveDataInChunks(conn net.Conn, file *os.File, expectedSize int64) (int64, error) {
	const bufferSize = 64 * 1024 // 64KB缓冲区
	var totalReceived int64
	buffer := make([]byte, bufferSize)

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(15 * time.Minute))

	for totalReceived < expectedSize {
		remaining := expectedSize - totalReceived
		readSize := bufferSize
		if remaining < int64(bufferSize) {
			readSize = int(remaining)
		}

		// 设置每次读取的超时
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		bytesRead, err := conn.Read(buffer[:readSize])
		if err != nil {
			if err == io.EOF && totalReceived == expectedSize {
				break
			}
			return totalReceived, fmt.Errorf("failed to read chunk at offset %d: %v", totalReceived, err)
		}

		if bytesRead == 0 {
			if totalReceived == expectedSize {
				break
			}
			return totalReceived, fmt.Errorf("unexpected end of stream at offset %d", totalReceived)
		}

		// 写入文件
		bytesWritten, err := file.Write(buffer[:bytesRead])
		if err != nil {
			return totalReceived, fmt.Errorf("failed to write to temp file: %v", err)
		}

		if bytesWritten != bytesRead {
			return totalReceived, fmt.Errorf("incomplete write: wrote %d, read %d", bytesWritten, bytesRead)
		}

		totalReceived += int64(bytesRead)

		// 每接收一定数据后报告进度
		if totalReceived%1048576 == 0 || totalReceived == expectedSize { // 每1MB或完成时
			progress := float64(totalReceived) / float64(expectedSize) * 100
			logger.Info("Restore progress: %.1f%% (%d/%d bytes)", progress, totalReceived, expectedSize)
		}
	}

	logger.Info("Data reception completed: %d bytes received", totalReceived)
	return totalReceived, nil
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

// createBackupFile 创建当前数据库的备份
func (ds *DatabaseServer) createBackupFile(originalPath, backupPath string) error {
	return ds.db.View(func(tx *bolt.Tx) error {
		file, err := os.Create(backupPath)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = tx.WriteTo(file)
		return err
	})
}

// recoverFromBackup 从备份恢复数据库
func (ds *DatabaseServer) recoverFromBackup(originalPath, backupPath string) {
	logger.Info("Attempting to recover from backup")

	// 删除损坏的文件
	os.Remove(originalPath)

	// 恢复备份
	if err := os.Rename(backupPath, originalPath); err != nil {
		logger.Error("Failed to recover from backup: %v", err)
		return
	}

	// 重新打开原始数据库
	db, err := bolt.Open(originalPath, 0600, nil)
	if err != nil {
		logger.Error("Failed to reopen original database: %v", err)
		return
	}

	ds.db = db
	logger.Info("Successfully recovered from backup")
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

	// 关闭只读事务
	if err := session.boltTx.Rollback(); err != nil {
		logger.Error("Error rolling back transaction for client %s: %v", session.ID, err)
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
	if session.inTransaction {
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

	// 关闭当前数据库
	originalPath := ds.db.Path()
	if err := ds.db.Close(); err != nil {
		os.Remove(tempFile)
		return fmt.Sprintf("Error closing current database: %v", err)
	}

	// 替换数据库文件
	if err := os.Rename(tempFile, originalPath); err != nil {
		// 重新打开原数据库
		ds.db, _ = bolt.Open(originalPath, 0600, nil)
		os.Remove(tempFile)
		return fmt.Sprintf("Error replacing database file: %v", err)
	}

	// 重新打开数据库
	db, err := bolt.Open(originalPath, 0600, nil)
	if err != nil {
		return fmt.Sprintf("Error reopening database: %v", err)
	}

	ds.db = db

	// 确保bucket存在
	err = ds.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})

	if err != nil {
		return fmt.Sprintf("Error creating bucket: %v", err)
	}

	return "RESTORE -> database restored successfully"
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

	// 启动服务器
	if err := server.Start(*port); err != nil {
		logger.Error("Server error: %v", err)
		os.Exit(1)
	}
}
