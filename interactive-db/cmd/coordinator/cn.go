package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"interactive-db/pkg/logger"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort       = "9090" // CN节点监听端口
	defaultServerPort = "8080" // 注册server的端口
	minNodes          = 3      // 最少节点数
	heartbeatInterval = 5 * time.Second
	heartbeatTimeout  = 15 * time.Second

	// 同步控制参数
	lsnSyncThreshold  = 10               // LSN差异超过10才触发同步
	syncCooldown      = 10 * time.Second // 同步冷却时间10秒
	maxSyncRetries    = 3                // 最大重试次数
	syncCheckInterval = 15 * time.Second // 数据追赶检测间隔
)

// NodeStatus 节点状态
type NodeStatus int

const (
	NodeStatusActive NodeStatus = iota
	NodeStatusDown
	NodeStatusSyncing // 正在同步状态
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusActive:
		return "Active"
	case NodeStatusDown:
		return "Down"
	case NodeStatusSyncing:
		return "Syncing"
	default:
		return "Unknown"
	}
}

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

// ServerNode 服务器节点信息
type ServerNode struct {
	ID             string     `json:"id"`
	Address        string     `json:"address"`
	Port           string     `json:"port"`
	Role           NodeRole   `json:"role"`
	Status         NodeStatus `json:"status"`
	LSN            int64      `json:"lsn"` // Log Sequence Number
	LastSeen       time.Time  `json:"last_seen"`
	LastSyncTime   time.Time  `json:"last_sync_time"`   // 上次同步时间
	SyncRetryCount int        `json:"sync_retry_count"` // 当前重试次数
	conn           net.Conn   // 到该节点的连接
}

// ClientSession 客户端会话
type ClientSession struct {
	ID         string
	conn       net.Conn // 到客户端的连接
	serverConn net.Conn // 到服务器的连接
}

// RegisterRequest 节点注册请求
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

// CoordinatorNode CN节点
type CoordinatorNode struct {
	port        string
	serverNodes map[string]*ServerNode
	clients     map[string]*ClientSession
	primaryNode *ServerNode
	mu          sync.RWMutex
	started     bool
	startTime   time.Time
}

// NewCoordinatorNode 创建新的协调节点
func NewCoordinatorNode(port string) *CoordinatorNode {
	return &CoordinatorNode{
		port:        port,
		serverNodes: make(map[string]*ServerNode),
		clients:     make(map[string]*ClientSession),
		started:     false,
	}
}

// Start 启动协调节点
func (cn *CoordinatorNode) Start() error {
	// 启动客户端监听服务
	go cn.startClientListener()

	// 启动服务器注册监听服务
	go cn.startServerListener()

	// 启动心跳监听服务
	go cn.startHeartbeatListener()

	// 启动心跳检测
	go cn.startHeartbeatChecker()

	// 启动数据追赶监控器
	go cn.startDataCatchupMonitor()

	logger.Info("Coordinator Node started on port %s", cn.port)
	logger.Info("Waiting for at least %d server nodes to register...", minNodes)

	// 阻塞主线程
	select {}
}

// startClientListener 启动客户端监听
func (cn *CoordinatorNode) startClientListener() {
	listener, err := net.Listen("tcp", ":"+cn.port)
	if err != nil {
		logger.Error("Failed to start client listener: %v", err)
		return
	}
	defer listener.Close()

	logger.Info("Client listener started on port %s", cn.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept client connection: %v", err)
			continue
		}

		clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())
		session := &ClientSession{
			ID:   clientID,
			conn: conn,
		}

		cn.mu.Lock()
		cn.clients[clientID] = session
		cn.mu.Unlock()

		logger.Info("Client %s connected from %s", clientID, conn.RemoteAddr())
		go cn.handleClient(session)
	}
}

// startServerListener 启动服务器注册监听
func (cn *CoordinatorNode) startServerListener() {
	serverPort := "9091" // 服务器注册端口
	listener, err := net.Listen("tcp", ":"+serverPort)
	if err != nil {
		logger.Error("Failed to start server listener: %v", err)
		return
	}
	defer listener.Close()

	logger.Info("Server registration listener started on port %s", serverPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept server connection: %v", err)
			continue
		}

		go cn.handleServerRegistration(conn)
	}
}

// handleServerRegistration 处理服务器注册
func (cn *CoordinatorNode) handleServerRegistration(conn net.Conn) {
	defer conn.Close()

	logger.Info("New server connection from %s", conn.RemoteAddr())

	decoder := json.NewDecoder(conn)
	var req RegisterRequest

	if err := decoder.Decode(&req); err != nil {
		logger.Error("Failed to decode registration request: %v", err)
		return
	}

	logger.Info("Server registration request from %s:%s (ID: %s)", req.Address, req.Port, req.NodeID)

	// 创建服务器节点
	node := &ServerNode{
		ID:       req.NodeID,
		Address:  req.Address,
		Port:     req.Port,
		Role:     NodeRoleReplica, // 默认为从节点
		Status:   NodeStatusActive,
		LSN:      0,
		LastSeen: time.Now(),
	}

	cn.mu.Lock()
	cn.serverNodes[req.NodeID] = node
	nodeCount := len(cn.serverNodes)
	needsCatchup := cn.started && cn.primaryNode != nil && cn.primaryNode.LSN > 0
	cn.mu.Unlock()

	logger.Info("Server %s registered successfully. Total nodes: %d", req.NodeID, nodeCount)

	// 发送注册成功响应
	response := map[string]interface{}{
		"status": "success",
		"role":   node.Role.String(),
	}

	logger.Info("Sending registration response to %s: %+v", req.NodeID, response)
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to send registration response to %s: %v", req.NodeID, err)
	} else {
		logger.Info("Registration response sent successfully to %s", req.NodeID)
	}

	// 检查是否可以启动系统
	if !cn.started && nodeCount >= minNodes {
		cn.startSystem()
	} else {
		// 打印当前注册的节点
		logger.Info("Current registered nodes: %v", cn.serverNodes)
	}

	// 如果系统已启动且新节点需要追赶数据，启动数据同步
	if needsCatchup {
		go cn.performDataCatchup(node)
	}
}

// connectToServer 连接到服务器节点
func (cn *CoordinatorNode) connectToServer(node *ServerNode) (net.Conn, error) {
	address := fmt.Sprintf("%s:%s", node.Address, node.Port)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// startSystem 启动系统（当有足够节点时）
func (cn *CoordinatorNode) startSystem() {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if cn.started {
		return
	}

	logger.Info("Starting distributed system with %d nodes", len(cn.serverNodes))

	// 等待节点心跳更新LSN信息
	logger.Info("Waiting for nodes to send heartbeat and update LSN...")

	// 解锁以允许心跳更新
	cn.mu.Unlock()
	time.Sleep(5 * time.Second) // 等待心跳
	cn.mu.Lock()

	// 选举主节点
	cn.electPrimaryNode()

	if cn.primaryNode == nil {
		logger.Error("Failed to elect primary node")
		return
	}

	cn.started = true
	cn.startTime = time.Now()

	logger.Info("System started successfully. Primary node: %s", cn.primaryNode.ID)

	// 启动数据一致性检查和追赶（异步执行，不阻塞系统启动）
	go cn.performStartupDataCatchup()
}

// electPrimaryNode 选举主节点
func (cn *CoordinatorNode) electPrimaryNode() {
	if len(cn.serverNodes) == 0 {
		return
	}

	// 如果已经有主节点且状态正常，不需要重新选举
	if cn.primaryNode != nil && cn.primaryNode.Status == NodeStatusActive {
		return
	}

	logger.Info("Starting primary node election...")

	// 从活跃节点中选择LSN最高的作为主节点
	var candidates []*ServerNode
	maxLSN := int64(-1)

	// 先打印所有节点的当前状态
	logger.Info("Current node status:")
	for _, node := range cn.serverNodes {
		logger.Info("  Node %s: Status=%s, LSN=%d", node.ID, node.Status.String(), node.LSN)
	}

	for _, node := range cn.serverNodes {
		if node.Status == NodeStatusActive {
			if node.LSN > maxLSN {
				maxLSN = node.LSN
				candidates = []*ServerNode{node}
				logger.Info("New candidate with higher LSN: %s (LSN: %d)", node.ID, node.LSN)
			} else if node.LSN == maxLSN {
				candidates = append(candidates, node)
				logger.Info("Additional candidate with same LSN: %s (LSN: %d)", node.ID, node.LSN)
			}
		}
	}

	if len(candidates) == 0 {
		logger.Error("No active nodes available for primary election")
		return
	}

	logger.Info("Election candidates with max LSN %d: %d nodes", maxLSN, len(candidates))

	// 如果有多个候选者，随机选择一个
	rand.Seed(time.Now().UnixNano())
	selected := candidates[rand.Intn(len(candidates))]

	// 更新之前的主节点为从节点
	if cn.primaryNode != nil {
		cn.primaryNode.Role = NodeRoleReplica
		logger.Info("Demoting previous primary node %s to replica", cn.primaryNode.ID)
	}

	// 设置新的主节点
	selected.Role = NodeRolePrimary
	cn.primaryNode = selected

	logger.Info("Elected new primary node: %s (LSN: %d)", selected.ID, selected.LSN)
}

// handleClient 处理客户端连接
func (cn *CoordinatorNode) handleClient(session *ClientSession) {
	defer func() {
		session.conn.Close()
		// 关闭到服务器的连接
		if session.serverConn != nil {
			session.serverConn.Close()
		}
		cn.mu.Lock()
		delete(cn.clients, session.ID)
		cn.mu.Unlock()
		logger.Info("Client %s disconnected", session.ID)
	}()

	// 发送欢迎消息
	cn.sendToClient(session, "Connected to Distributed Database Coordinator")
	cn.sendToClient(session, "Type 'HELP' for available commands, 'EXIT' to quit")

	if !cn.started {
		cn.sendToClient(session, "System is starting up, please wait...")
		// 等待系统启动
		for !cn.started {
			time.Sleep(time.Second)
		}
		cn.sendToClient(session, "System is now ready!")
	}

	scanner := bufio.NewScanner(session.conn)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			continue
		}

		logger.Info("Client %s executed: %s", session.ID, command)

		// 检查退出命令
		if strings.ToUpper(strings.Fields(command)[0]) == "EXIT" ||
			strings.ToUpper(strings.Fields(command)[0]) == "QUIT" {
			cn.sendToClient(session, "Goodbye!")
			break
		}

		// 转发命令到主节点
		response := cn.forwardToMasterForClient(session, command)
		cn.sendToClient(session, response)
	}
}

// forwardToMasterForClient 为特定客户端转发命令到主节点
func (cn *CoordinatorNode) forwardToMasterForClient(session *ClientSession, command string) string {
	cn.mu.RLock()
	primary := cn.primaryNode
	cn.mu.RUnlock()

	if primary == nil {
		return "Error: No primary node available"
	}

	if primary.Status != NodeStatusActive {
		// 尝试重新选举主节点
		cn.mu.Lock()
		cn.electPrimaryNode()
		primary = cn.primaryNode
		cn.mu.Unlock()

		if primary == nil || primary.Status != NodeStatusActive {
			return "Error: No active primary node available"
		}
	}

	// 为客户端会话建立或重用到主节点的连接
	if session.serverConn == nil {
		conn, err := cn.connectToServer(primary)
		if err != nil {
			return fmt.Sprintf("Error: Failed to connect to primary node: %v", err)
		}
		session.serverConn = conn

		// 读取并丢弃服务器的欢迎消息
		scanner := bufio.NewScanner(session.serverConn)
		// 读取第一行: "Connected to Interactive Database Server"
		if scanner.Scan() {
			// 丢弃欢迎消息
		}
		// 读取第二行: "Type 'HELP' for available commands, 'EXIT' to quit"
		if scanner.Scan() {
			// 丢弃提示消息
		}
	}

	// 发送命令到主节点
	response, err := cn.sendCommandToClientServerConn(session, command)
	if err != nil {
		logger.Error("Failed to send command to primary node for client %s: %v", session.ID, err)

		// 连接可能断开，关闭并重置连接
		if session.serverConn != nil {
			session.serverConn.Close()
			session.serverConn = nil
		}

		// 如果主节点连接失败，标记为DOWN并重新选举
		cn.mu.Lock()
		primary.Status = NodeStatusDown
		cn.electPrimaryNode()
		cn.mu.Unlock()

		return "Error: Primary node is not responding"
	}

	// 如果是写操作，需要同步到从节点
	if cn.isWriteCommand(command) {
		go cn.syncToReplicas(command)
	}

	return response
}

// sendCommandToClientServerConn 通过客户端的服务器连接发送命令
func (cn *CoordinatorNode) sendCommandToClientServerConn(session *ClientSession, command string) (string, error) {
	// 发送命令
	_, err := session.serverConn.Write([]byte(command + "\n"))
	if err != nil {
		return "", err
	}

	// 读取响应
	scanner := bufio.NewScanner(session.serverConn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", fmt.Errorf("no response from server")
}

// isWriteCommand 判断是否为写命令
func (cn *CoordinatorNode) isWriteCommand(command string) bool {
	parts := strings.Fields(strings.ToUpper(command))
	if len(parts) == 0 {
		return false
	}

	writeCommands := []string{"PUT", "DEL", "BEGIN", "COMMIT", "ABORT"}
	for _, cmd := range writeCommands {
		if parts[0] == cmd {
			return true
		}
	}
	return false
}

// syncToReplicas 异步同步写操作到从节点
func (cn *CoordinatorNode) syncToReplicas(command string) {
	cn.mu.RLock()
	replicas := make([]*ServerNode, 0)
	for _, node := range cn.serverNodes {
		if node.Role == NodeRoleReplica && node.Status == NodeStatusActive {
			replicas = append(replicas, node)
		}
	}
	cn.mu.RUnlock()

	if len(replicas) == 0 {
		return
	}

	// 完全异步同步，不等待结果，不影响客户端响应
	go func() {
		var wg sync.WaitGroup
		for _, node := range replicas {
			wg.Add(1)
			go func(n *ServerNode) {
				defer wg.Done()

				// 添加重试机制
				maxRetries := 2
				for retry := 0; retry <= maxRetries; retry++ {
					_, err := cn.sendCommandToNode(n, command)
					if err == nil {
						// 成功，不需要记录日志避免日志泛滥
						return
					}

					if retry < maxRetries {
						// 重试前等待一下
						time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond)
					} else {
						// 最终失败，标记节点为DOWN
						logger.Error("Failed to sync to replica %s after %d retries: %v", n.ID, maxRetries+1, err)
						cn.mu.Lock()
						n.Status = NodeStatusDown
						cn.mu.Unlock()
					}
				}
			}(node)
		}

		// 设置更长的超时时间，适应大数据量
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// 同步完成，不记录日志避免日志泛滥
		case <-time.After(30 * time.Second): // 增加到30秒
			logger.Error("Replica sync timeout for command (truncated): %.50s...", command)
		}
	}()
}

// sendToClient 发送消息到客户端
func (cn *CoordinatorNode) sendToClient(session *ClientSession, message string) {
	_, err := session.conn.Write([]byte(message + "\n"))
	if err != nil {
		logger.Error("Failed to send message to client %s: %v", session.ID, err)
	}
}

// startHeartbeatChecker 启动心跳检测
func (cn *CoordinatorNode) startHeartbeatChecker() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		cn.checkNodeHealth()
	}
}

// checkNodeHealth 检查节点健康状态
func (cn *CoordinatorNode) checkNodeHealth() {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	now := time.Now()
	primaryDown := false

	for _, node := range cn.serverNodes {
		if now.Sub(node.LastSeen) > heartbeatTimeout {
			if node.Status == NodeStatusActive {
				logger.Info("Node %s marked as DOWN (last seen: %v)", node.ID, node.LastSeen)
				node.Status = NodeStatusDown

				if node.Role == NodeRolePrimary {
					primaryDown = true
				}
			}
		}
	}

	// 如果主节点下线，重新选举
	if primaryDown {
		logger.Info("Primary node is down, starting election...")
		cn.electPrimaryNode()
	}
}

// startHeartbeatListener 启动心跳监听
func (cn *CoordinatorNode) startHeartbeatListener() {
	heartbeatPort := "9092" // 心跳监听端口
	listener, err := net.Listen("tcp", ":"+heartbeatPort)
	if err != nil {
		logger.Error("Failed to start heartbeat listener: %v", err)
		return
	}
	defer listener.Close()

	logger.Info("Heartbeat listener started on port %s", heartbeatPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept heartbeat connection: %v", err)
			continue
		}

		go cn.handleHeartbeat(conn)
	}
}

// handleHeartbeat 处理心跳请求
func (cn *CoordinatorNode) handleHeartbeat(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var req HeartbeatRequest

	if err := decoder.Decode(&req); err != nil {
		logger.Error("Failed to decode heartbeat request: %v", err)
		return
	}

	// 更新节点的最后心跳时间和LSN
	cn.mu.Lock()
	if node, exists := cn.serverNodes[req.NodeID]; exists {
		node.LastSeen = time.Now()
		node.LSN = req.LSN
		// 如果节点之前是DOWN状态，重新标记为ACTIVE
		if node.Status == NodeStatusDown {
			logger.Info("Node %s is back online", req.NodeID)
			node.Status = NodeStatusActive
		}
	}
	cn.mu.Unlock()

	// 可选：发送心跳响应确认
	response := map[string]interface{}{
		"status": "ok",
		"time":   time.Now().Unix(),
	}
	encoder := json.NewEncoder(conn)
	encoder.Encode(response)
}

// sendCommandToNode 发送命令到指定节点（用于向从节点同步）
func (cn *CoordinatorNode) sendCommandToNode(node *ServerNode, command string) (string, error) {
	if node.conn == nil {
		// 尝试重新连接
		conn, err := cn.connectToServer(node)
		if err != nil {
			return "", err
		}
		node.conn = conn

		// 读取并丢弃服务器的欢迎消息
		scanner := bufio.NewScanner(node.conn)
		// 读取第一行: "Connected to Interactive Database Server"
		if scanner.Scan() {
			// 丢弃欢迎消息
		}
		// 读取第二行: "Type 'HELP' for available commands, 'EXIT' to quit"
		if scanner.Scan() {
			// 丢弃提示消息
		}
	}

	// 发送命令
	_, err := node.conn.Write([]byte(command + "\n"))
	if err != nil {
		// 连接可能断开，重置连接
		node.conn = nil
		return "", err
	}

	// 读取响应
	scanner := bufio.NewScanner(node.conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}

	if err := scanner.Err(); err != nil {
		node.conn = nil
		return "", err
	}

	return "", fmt.Errorf("no response from node")
}

// streamBackupFromNode 从节点流式备份数据
func (cn *CoordinatorNode) streamBackupFromNode(node *ServerNode) (io.ReadCloser, int64, error) {
	// 计算备份端口
	portNum, err := strconv.Atoi(node.Port)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid port number: %v", err)
	}
	backupPort := strconv.Itoa(portNum + 1000) // backupPortOffset = 1000

	// 连接到备份端口
	backupAddr := fmt.Sprintf("%s:%s", node.Address, backupPort)
	logger.Info("Connecting to backup port: %s", backupAddr)

	conn, err := net.DialTimeout("tcp", backupAddr, 10*time.Second)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to connect to backup port: %v", err)
	}

	// 设置TCP选项
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
	}

	// 设置连接超时
	conn.SetDeadline(time.Now().Add(10 * time.Minute))

	// 发送BACKUP请求
	_, err = conn.Write([]byte("BACKUP\n"))
	if err != nil {
		conn.Close()
		return nil, 0, fmt.Errorf("failed to send backup request: %v", err)
	}

	// 读取响应和数据大小
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		conn.Close()
		return nil, 0, fmt.Errorf("failed to read backup response")
	}

	response := strings.TrimSpace(scanner.Text())
	parts := strings.Fields(response)
	if len(parts) != 2 || parts[0] != "OK" {
		conn.Close()
		return nil, 0, fmt.Errorf("backup request failed: %s", response)
	}

	dataSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		conn.Close()
		return nil, 0, fmt.Errorf("invalid data size in response: %s", parts[1])
	}

	// 清除读取超时，准备接收数据流
	conn.SetReadDeadline(time.Time{})
	logger.Info("Backup stream ready from %s, expecting %d bytes", node.ID, dataSize)

	// 创建一个包装器来处理连接关闭和确认
	wrapper := &backupStreamWrapper{
		conn:     conn,
		dataSize: dataSize,
		node:     node,
	}

	// 返回包装器作为数据流和数据大小
	return wrapper, dataSize, nil
}

// backupStreamWrapper 包装备份数据流连接
type backupStreamWrapper struct {
	conn     net.Conn
	dataSize int64
	node     *ServerNode
	closed   bool
}

func (w *backupStreamWrapper) Read(p []byte) (n int, err error) {
	return w.conn.Read(p)
}

func (w *backupStreamWrapper) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// 发送确认消息
	_, err := w.conn.Write([]byte("ACK\n"))
	if err != nil {
		logger.Info("Failed to send acknowledgment to %s: %v", w.node.ID, err)
	} else {
		logger.Info("Sent acknowledgment to %s", w.node.ID)
	}

	// 等待一小段时间确保确认被发送
	time.Sleep(100 * time.Millisecond)

	return w.conn.Close()
}

// streamRestoreToNode 流式恢复数据到节点 - 增强版本，带重试机制
func (cn *CoordinatorNode) streamRestoreToNode(node *ServerNode, dataReader io.Reader, dataSize int64) error {
	// 将数据读取到缓冲区，以便可以重试
	dataBuffer := make([]byte, dataSize)
	bytesRead, err := io.ReadFull(dataReader, dataBuffer)
	if err != nil {
		return fmt.Errorf("failed to read data into buffer: %v", err)
	}
	if int64(bytesRead) != dataSize {
		return fmt.Errorf("read %d bytes, expected %d bytes", bytesRead, dataSize)
	}

	logger.Info("Buffered %d bytes for restore to %s", dataSize, node.ID)

	// 重试机制
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Info("Attempt %d/%d to restore data to %s", attempt, maxRetries, node.ID)

		err := cn.performStreamRestore(node, dataBuffer, dataSize)
		if err == nil {
			logger.Info("Successfully restored data to %s on attempt %d", node.ID, attempt)
			return nil
		}

		logger.Error("Attempt %d failed to restore to %s: %v", attempt, node.ID, err)

		if attempt < maxRetries {
			// 等待一段时间后重试
			waitTime := time.Duration(attempt) * 2 * time.Second
			logger.Info("Waiting %v before retry %d", waitTime, attempt+1)
			time.Sleep(waitTime)
		}
	}

	return fmt.Errorf("failed to restore after %d attempts", maxRetries)
}

// performStreamRestore 执行单次流式恢复尝试
func (cn *CoordinatorNode) performStreamRestore(node *ServerNode, dataBuffer []byte, dataSize int64) error {
	// 计算备份端口
	portNum, err := strconv.Atoi(node.Port)
	if err != nil {
		return fmt.Errorf("invalid port number: %v", err)
	}
	backupPort := strconv.Itoa(portNum + 1000)

	// 连接到备份端口
	backupAddr := fmt.Sprintf("%s:%s", node.Address, backupPort)
	logger.Info("Connecting to restore port: %s", backupAddr)

	conn, err := net.DialTimeout("tcp", backupAddr, 15*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to backup port: %v", err)
	}
	defer conn.Close()

	// 设置TCP选项和连接超时
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	conn.SetDeadline(time.Now().Add(15 * time.Minute))

	// 发送RESTORE请求
	logger.Info("Sending RESTORE request to %s", node.ID)
	_, err = conn.Write([]byte("RESTORE\n"))
	if err != nil {
		return fmt.Errorf("failed to send restore request: %v", err)
	}

	// 发送数据大小
	sizeMsg := fmt.Sprintf("%d\n", dataSize)
	logger.Info("Sending data size %d to %s", dataSize, node.ID)
	_, err = conn.Write([]byte(sizeMsg))
	if err != nil {
		return fmt.Errorf("failed to send data size: %v", err)
	}

	// 读取确认响应
	logger.Info("Waiting for OK response from %s", node.ID)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	responseBuffer := make([]byte, 1024)
	n, err := conn.Read(responseBuffer)
	if err != nil {
		return fmt.Errorf("failed to read restore response from %s: %v", node.ID, err)
	}

	response := strings.TrimSpace(string(responseBuffer[:n]))
	logger.Info("Received response from %s: '%s'", node.ID, response)

	if !strings.HasPrefix(response, "OK") {
		return fmt.Errorf("restore request failed, server response: %s", response)
	}

	// 清除读取超时，准备发送数据流
	conn.SetReadDeadline(time.Time{})
	logger.Info("Starting chunked data stream to %s, sending %d bytes", node.ID, dataSize)

	// 分块传输数据以提高稳定性
	return cn.sendDataInChunks(conn, dataBuffer, node.ID)
}

// sendDataInChunks 分块发送数据
func (cn *CoordinatorNode) sendDataInChunks(conn net.Conn, data []byte, nodeID string) error {
	const chunkSize = 64 * 1024 // 64KB块
	totalSize := len(data)
	var totalSent int

	for offset := 0; offset < totalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > totalSize {
			end = totalSize
		}

		chunk := data[offset:end]
		chunkLen := len(chunk)

		logger.Info("Sending chunk to %s: offset=%d, size=%d, total_progress=%.1f%%",
			nodeID, offset, chunkLen, float64(offset)/float64(totalSize)*100)

		// 设置写入超时
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

		bytesSent, err := conn.Write(chunk)
		if err != nil {
			return fmt.Errorf("failed to send chunk at offset %d: %v", offset, err)
		}

		if bytesSent != chunkLen {
			return fmt.Errorf("incomplete chunk write: sent %d, expected %d", bytesSent, chunkLen)
		}

		totalSent += bytesSent

		// 每发送一个块后稍微暂停，确保网络稳定
		if chunkLen == chunkSize {
			time.Sleep(10 * time.Millisecond)
		}
	}

	logger.Info("Completed chunked transfer to %s: sent %d bytes", nodeID, totalSent)

	if totalSent != totalSize {
		return fmt.Errorf("total bytes mismatch: sent %d, expected %d", totalSent, totalSize)
	}

	return nil
}

// performDataCatchup 执行数据追赶 - 改进版本（新节点注册时）
func (cn *CoordinatorNode) performDataCatchup(newNode *ServerNode) {
	// 等待新节点完全启动
	logger.Info("Waiting for node %s to fully start before data catchup", newNode.ID)
	time.Sleep(2 * time.Second)

	logger.Info("Starting data catchup for new node %s", newNode.ID)

	cn.mu.RLock()
	primary := cn.primaryNode
	cn.mu.RUnlock()

	if primary == nil || primary.Status != NodeStatusActive {
		logger.Error("No active primary node available for data catchup")
		return
	}

	// 新节点注册时强制同步，不检查阈值
	logger.Info("New node %s (LSN: %d) will sync from primary %s (LSN: %d)",
		newNode.ID, newNode.LSN, primary.ID, primary.LSN)

	// 使用重试机制进行同步
	go cn.performSyncWithRetry(newNode, primary)

	logger.Info("Data catchup initiated for new node %s", newNode.ID)
}

// performStartupDataCatchup 执行启动时的数据追赶 - 简化版本
func (cn *CoordinatorNode) performStartupDataCatchup() {
	// 等待一段时间，确保所有节点都完全启动
	logger.Info("Waiting for all nodes to fully start before data consistency check")
	time.Sleep(3 * time.Second)

	logger.Info("Starting startup data catchup process")

	cn.mu.RLock()
	primary := cn.primaryNode
	replicaCount := 0
	for _, node := range cn.serverNodes {
		if node.Role == NodeRoleReplica {
			replicaCount++
		}
	}
	cn.mu.RUnlock()

	if primary == nil {
		logger.Error("No primary node available for startup data catchup")
		return
	}

	logger.Info("Startup sync check: Primary %s LSN=%d, %d replicas detected",
		primary.ID, primary.LSN, replicaCount)

	// 启动后的数据一致性检查会由独立的监控器线程处理
	// 这里只记录状态，让定期检查来处理同步
	logger.Info("Startup data catchup process completed - periodic monitor will handle ongoing sync")
}

// getSyncSkipReason 获取跳过同步的原因（用于日志）
func (cn *CoordinatorNode) getSyncSkipReason(replica, primary *ServerNode) string {
	lsnDiff := primary.LSN - replica.LSN

	if lsnDiff < lsnSyncThreshold {
		return fmt.Sprintf("LSN diff %d below threshold %d", lsnDiff, lsnSyncThreshold)
	}

	if time.Since(replica.LastSyncTime) < syncCooldown {
		return fmt.Sprintf("in cooldown period (%v remaining)",
			syncCooldown-time.Since(replica.LastSyncTime))
	}

	if replica.Status == NodeStatusSyncing {
		return "already syncing"
	}

	return "unknown reason"
}

// shouldTriggerSync 判断是否应该触发同步 - 改进版本
func (cn *CoordinatorNode) shouldTriggerSync(replica, primary *ServerNode, forceSync bool) bool {
	// 强制同步（如新节点注册）
	if forceSync {
		return true
	}

	// 检查节点状态
	if replica.Status != NodeStatusActive || primary.Status != NodeStatusActive {
		return false
	}

	// 检查LSN差异是否超过阈值
	lsnDiff := primary.LSN - replica.LSN
	if lsnDiff < lsnSyncThreshold {
		return false
	}

	// 检查是否在冷却期（只有重试耗尽后才会进入冷却期）
	if replica.SyncRetryCount >= maxSyncRetries && time.Since(replica.LastSyncTime) < syncCooldown {
		logger.Info("Node %s is in sync cooldown period after %d failed retries, skipping (cooldown ends in %v)",
			replica.ID, replica.SyncRetryCount, syncCooldown-time.Since(replica.LastSyncTime))
		return false
	}

	// 如果重试次数耗尽且冷却期已过，重置重试计数
	if replica.SyncRetryCount >= maxSyncRetries && time.Since(replica.LastSyncTime) >= syncCooldown {
		logger.Info("Cooldown period ended for %s, resetting retry count", replica.ID)
		replica.SyncRetryCount = 0
	}

	logger.Info("Sync decision for %s: LSN diff=%d (threshold=%d), retry count=%d/%d",
		replica.ID, lsnDiff, lsnSyncThreshold, replica.SyncRetryCount, maxSyncRetries)

	return true
}

// performSyncWithRetry 执行带重试的同步
func (cn *CoordinatorNode) performSyncWithRetry(replica, primary *ServerNode) {
	cn.mu.Lock()
	if replica.Status == NodeStatusSyncing {
		cn.mu.Unlock()
		logger.Info("Node %s is already syncing, skipping", replica.ID)
		return
	}

	// 标记为同步状态
	replica.Status = NodeStatusSyncing
	cn.mu.Unlock()

	logger.Info("Starting sync for %s (attempt %d/%d)", replica.ID, replica.SyncRetryCount+1, maxSyncRetries)

	// 执行同步
	err := cn.performStreamDataSync(primary, replica)

	// 处理同步结果
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if err != nil {
		replica.SyncRetryCount++
		logger.Error("Sync failed for %s (attempt %d/%d): %v",
			replica.ID, replica.SyncRetryCount, maxSyncRetries, err)

		if replica.SyncRetryCount >= maxSyncRetries {
			logger.Error("Max retries reached for %s, entering cooldown period", replica.ID)
			replica.LastSyncTime = time.Now() // 进入冷却期
		}
		replica.Status = NodeStatusActive // 恢复活跃状态
	} else {
		logger.Info("Sync completed successfully for %s", replica.ID)
		replica.Status = NodeStatusActive
		replica.SyncRetryCount = 0 // 重置重试计数
		replica.LastSyncTime = time.Now()

		// 验证同步结果
		cn.verifySyncResult(replica, primary)
	}
}

// verifySyncResult 验证同步结果
func (cn *CoordinatorNode) verifySyncResult(replica, primary *ServerNode) {
	// 让新节点自己计算当前LSN状态
	logger.Info("Requesting current LSN from node %s after data sync", replica.ID)
	lsnResponse, err := cn.sendCommandToNode(replica, "GET __lsn__")
	if err != nil {
		logger.Error("Failed to get LSN from node %s: %v", replica.ID, err)
	} else {
		logger.Info("Node %s current LSN status: %s", replica.ID, lsnResponse)

		// 尝试解析LSN值进行验证
		if strings.Contains(lsnResponse, ":") {
			parts := strings.Split(lsnResponse, ":")
			if len(parts) >= 2 {
				if newLSN, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
					lsnDiff := primary.LSN - newLSN
					logger.Info("Sync result: %s LSN=%d, Primary LSN=%d, remaining diff=%d",
						replica.ID, newLSN, primary.LSN, lsnDiff)

					// 更新节点的LSN值
					replica.LSN = newLSN
				}
			}
		}
	}
}

// performStreamDataSync 执行流式数据同步
func (cn *CoordinatorNode) performStreamDataSync(source, target *ServerNode) error {
	logger.Info("Starting stream data sync from %s to %s", source.ID, target.ID)

	// 从源节点获取备份流
	dataStream, dataSize, err := cn.streamBackupFromNode(source)
	if err != nil {
		return fmt.Errorf("failed to get backup stream from %s: %v", source.ID, err)
	}
	defer dataStream.Close()

	logger.Info("Backup stream established from %s, data size: %d bytes", source.ID, dataSize)

	// 将数据流恢复到目标节点
	err = cn.streamRestoreToNode(target, dataStream, dataSize)
	if err != nil {
		return fmt.Errorf("failed to restore stream to %s: %v", target.ID, err)
	}

	logger.Info("Stream data sync completed from %s to %s", source.ID, target.ID)
	return nil
}

// startDataCatchupMonitor 启动数据追赶监控器
func (cn *CoordinatorNode) startDataCatchupMonitor() {
	ticker := time.NewTicker(syncCheckInterval)
	defer ticker.Stop()

	logger.Info("Data catchup monitor started, checking every %v", syncCheckInterval)

	for range ticker.C {
		cn.performPeriodicDataCatchup()
	}
}

// performPeriodicDataCatchup 定期检查数据追赶
func (cn *CoordinatorNode) performPeriodicDataCatchup() {
	cn.mu.RLock()
	if !cn.started || cn.primaryNode == nil {
		cn.mu.RUnlock()
		return
	}

	primary := cn.primaryNode
	replicas := make([]*ServerNode, 0)
	for _, node := range cn.serverNodes {
		if node.Role == NodeRoleReplica && (node.Status == NodeStatusActive || node.Status == NodeStatusSyncing) {
			replicas = append(replicas, node)
		}
	}
	cn.mu.RUnlock()

	if len(replicas) == 0 {
		return
	}

	logger.Info("Periodic sync check: Primary %s LSN=%d, checking %d replicas",
		primary.ID, primary.LSN, len(replicas))

	for _, replica := range replicas {
		if cn.shouldTriggerSync(replica, primary, false) {
			logger.Info("Triggering periodic sync for %s (LSN diff: %d)",
				replica.ID, primary.LSN-replica.LSN)
			go cn.performSyncWithRetry(replica, primary)
		}
	}
}

func main() {
	// 初始化日志器
	logger.Init()
	logger.SetComponent("CN")
	defer logger.Close()

	port := flag.String("port", defaultPort, "Port for client connections")
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Println("Distributed Database Coordinator Node")
		fmt.Println("Usage: coordinator [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	cn := NewCoordinatorNode(*port)
	if err := cn.Start(); err != nil {
		logger.Error("Failed to start coordinator: %v", err)
		os.Exit(1)
	}
}
