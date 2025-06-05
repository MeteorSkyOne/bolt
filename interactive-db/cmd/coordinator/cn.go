package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"interactive-db/pkg/logger"
	pb "interactive-db/proto/coordinator"
	serverpb "interactive-db/proto/server"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultPort       = "9090"           // CN节点监听端口
	defaultGRPCPort   = "9091"           // gRPC服务端口
	minNodes          = 3                // 最少节点数
	heartbeatInterval = 10 * time.Second // 调整为10秒，避免过于频繁
	heartbeatTimeout  = 20 * time.Second // 相应调整超时时间

	// 同步控制参数 - 改为定时同步策略
	syncInterval      = 10 * time.Second // 定时同步间隔（不再基于LSN阈值）
	syncCooldown      = 5 * time.Second  // 同步失败后的冷却时间
	maxSyncRetries    = 3                // 最大重试次数
	syncCheckInterval = 8 * time.Second  // 数据追赶检测间隔（更频繁）
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
	ID             string                         `json:"id"`
	Address        string                         `json:"address"`
	Port           string                         `json:"port"`
	GRPCPort       string                         // gRPC端口
	Role           NodeRole                       `json:"role"`
	Status         NodeStatus                     `json:"status"`
	LSN            int64                          `json:"lsn"` // Log Sequence Number
	LastSeen       time.Time                      `json:"last_seen"`
	LastSyncTime   time.Time                      `json:"last_sync_time"`   // 上次同步时间
	SyncRetryCount int                            `json:"sync_retry_count"` // 当前重试次数
	conn           net.Conn                       // 到该节点的连接（TCP，用于客户端请求转发）
	grpcConn       *grpc.ClientConn               // gRPC连接
	grpcClient     serverpb.DatabaseServiceClient // gRPC客户端
}

// ClientSession 客户端会话
type ClientSession struct {
	ID         string
	conn       net.Conn // 到客户端的连接
	serverConn net.Conn // 到服务器的连接
}

// CoordinatorNode CN节点
type CoordinatorNode struct {
	pb.UnimplementedCoordinatorServiceServer
	port        string
	grpcPort    string
	serverNodes map[string]*ServerNode
	clients     map[string]*ClientSession
	primaryNode *ServerNode
	mu          sync.RWMutex
	started     bool
	startTime   time.Time
	grpcServer  *grpc.Server
}

// NewCoordinatorNode 创建新的协调节点
func NewCoordinatorNode(port, grpcPort string) *CoordinatorNode {
	cn := &CoordinatorNode{
		port:        port,
		grpcPort:    grpcPort,
		serverNodes: make(map[string]*ServerNode),
		clients:     make(map[string]*ClientSession),
		started:     false,
		grpcServer: grpc.NewServer(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    30 * time.Second, // 增加服务器发送keepalive ping的时间间隔
				Timeout: 10 * time.Second, // 增加等待keepalive ping应答的超时时间
			}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             10 * time.Second, // 增加客户端keepalive ping的最小时间间隔
				PermitWithoutStream: true,             // 允许在没有活跃流时发送keepalive ping
			}),
		),
	}

	// 注册gRPC服务
	pb.RegisterCoordinatorServiceServer(cn.grpcServer, cn)

	return cn
}

// Start 启动协调节点
func (cn *CoordinatorNode) Start() error {
	// 启动gRPC服务器
	grpcListener, err := net.Listen("tcp", ":"+cn.grpcPort)
	if err != nil {
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}

	go func() {
		logger.Info("gRPC server started on port %s", cn.grpcPort)
		if err := cn.grpcServer.Serve(grpcListener); err != nil {
			logger.Error("gRPC server error: %v", err)
		}
	}()

	// 启动客户端监听服务（保留原有的客户端连接功能）
	go cn.startClientListener()

	// 启动心跳检测
	go cn.startHeartbeatChecker()

	// 启动数据追赶监控器
	go cn.startDataCatchupMonitor()

	logger.Info("Coordinator Node started")
	logger.Info("Client connections on port %s", cn.port)
	logger.Info("gRPC services on port %s", cn.grpcPort)
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

// connectToServer 连接到服务器节点（用于客户端请求转发的TCP连接）
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

	// 不要释放锁，直接等待心跳更新
	// 心跳会在另一个goroutine中更新LSN，使用独立的锁机制
	logger.Info("Waiting for initial heartbeat from nodes...")

	// 标记系统为已启动，这样心跳可以正常处理
	cn.started = true
	cn.startTime = time.Now()

	// 选举主节点
	cn.electPrimaryNode()

	if cn.primaryNode == nil {
		logger.Error("Failed to elect primary node")
		cn.started = false // 回滚启动状态
		return
	}

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

// sendCommandToNode 发送命令到指定节点（用于向从节点同步）
func (cn *CoordinatorNode) sendCommandToNode(node *ServerNode, command string) (string, error) {
	if node.grpcClient == nil {
		return "", fmt.Errorf("gRPC client not initialized for node %s", node.ID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // 增加超时时间
	defer cancel()

	resp, err := node.grpcClient.ExecuteCommand(ctx, &serverpb.CommandRequest{
		Command: command,
	})
	if err != nil {
		return "", err
	}

	return resp.Result, nil
}

// streamBackupFromNode 从节点流式备份数据
func (cn *CoordinatorNode) streamBackupFromNode(node *ServerNode) (io.ReadCloser, int64, error) {
	if node.grpcClient == nil {
		return nil, 0, fmt.Errorf("gRPC client not initialized for node %s", node.ID)
	}

	// 不要在这里使用带超时的context，因为stream可能需要长时间运行
	// 改为使用一个可以手动控制的context
	ctx := context.Background()

	stream, err := node.grpcClient.StreamBackup(ctx, &serverpb.BackupRequest{
		SourceNodeId: node.ID,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to start backup stream: %v", err)
	}

	// 创建管道用于流式读取
	pr, pw := io.Pipe()
	var totalSize int64

	// 启动goroutine从gRPC流读取数据并写入管道
	go func() {
		defer pw.Close()

		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("Failed to receive backup chunk from %s: %v", node.ID, err)
				pw.CloseWithError(err)
				return
			}

			// 第一个块包含总大小
			if totalSize == 0 {
				totalSize = chunk.TotalSize
			}

			// 写入数据到管道
			if _, err := pw.Write(chunk.Data); err != nil {
				logger.Error("Failed to write backup data: %v", err)
				pw.CloseWithError(err)
				return
			}

			if chunk.IsLast {
				break
			}
		}
	}()

	// 等待第一个块来获取总大小
	time.Sleep(100 * time.Millisecond)

	logger.Info("Backup stream ready from %s via gRPC", node.ID)
	return pr, totalSize, nil
}

// streamRestoreToNode 流式恢复数据到节点
func (cn *CoordinatorNode) streamRestoreToNode(node *ServerNode, dataReader io.Reader, dataSize int64) error {
	if node.grpcClient == nil {
		return fmt.Errorf("gRPC client not initialized for node %s", node.ID)
	}

	logger.Info("Starting gRPC stream restore to %s, size: %d bytes", node.ID, dataSize)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second) // 增加超时时间用于大数据传输
	defer cancel()

	stream, err := node.grpcClient.StreamRestore(ctx)
	if err != nil {
		return fmt.Errorf("failed to start restore stream: %v", err)
	}

	// 分块发送数据
	chunkSize := 64 * 1024 // 64KB chunks
	buffer := make([]byte, chunkSize)
	var totalSent int64

	for totalSent < dataSize {
		n, err := dataReader.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read data: %v", err)
		}

		if n == 0 {
			break
		}

		chunk := &serverpb.RestoreChunk{
			Data:      buffer[:n],
			TotalSize: dataSize,
			IsLast:    totalSent+int64(n) >= dataSize,
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send restore chunk: %v", err)
		}

		totalSent += int64(n)

		// 报告进度，减少日志频率
		if totalSent%(5*1024*1024) == 0 || totalSent >= dataSize {
			progress := float64(totalSent) / float64(dataSize) * 100
			logger.Info("Restore progress to %s: %.1f%% (%d/%d bytes)", node.ID, progress, totalSent, dataSize)
		}
	}

	// 关闭流并获取响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to complete restore stream: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("restore failed: %s", resp.Message)
	}

	logger.Info("gRPC stream restore to %s completed successfully", node.ID)
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

	// 检查新节点LSN是否小于主节点，只有小于才需要同步（使用读锁保护）
	cn.mu.RLock()
	newNodeLSN := newNode.LSN
	primaryLSN := primary.LSN
	cn.mu.RUnlock()

	if newNodeLSN >= primaryLSN {
		logger.Info("New node %s (LSN: %d) is already up-to-date with primary %s (LSN: %d), no sync needed",
			newNode.ID, newNodeLSN, primary.ID, primaryLSN)
		return
	}

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

	// 使用读锁保护对primary LSN的访问
	cn.mu.RLock()
	primaryLSN := primary.LSN
	cn.mu.RUnlock()

	logger.Info("Startup sync check: Primary %s LSN=%d, %d replicas detected",
		primary.ID, primaryLSN, replicaCount)

	// 启动后的数据一致性检查会由独立的监控器线程处理
	// 这里只记录状态，让定期检查来处理同步
	logger.Info("Startup data catchup process completed - periodic monitor will handle ongoing sync")
}

// getSyncSkipReason 获取跳过同步的原因（用于日志）（修复竞态条件）
func (cn *CoordinatorNode) getSyncSkipReason(replica, primary *ServerNode) string {
	// 使用读锁保护对节点状态的访问
	cn.mu.RLock()
	replicaLSN := replica.LSN
	primaryLSN := primary.LSN
	replicaLastSyncTime := replica.LastSyncTime
	replicaStatus := replica.Status
	cn.mu.RUnlock()

	lsnDiff := primaryLSN - replicaLSN
	timeSinceLastSync := time.Since(replicaLastSyncTime)

	if lsnDiff <= 0 {
		return fmt.Sprintf("no LSN diff (diff=%d)", lsnDiff)
	}

	if timeSinceLastSync < syncInterval {
		return fmt.Sprintf("within sync interval (%v remaining)", syncInterval-timeSinceLastSync)
	}

	if timeSinceLastSync < syncCooldown {
		return fmt.Sprintf("in cooldown period (%v remaining)", syncCooldown-timeSinceLastSync)
	}

	if replicaStatus == NodeStatusSyncing {
		return "already syncing"
	}

	return "unknown reason"
}

// shouldTriggerSync 判断是否应该触发同步 - 定时同步策略（修复竞态条件）
func (cn *CoordinatorNode) shouldTriggerSync(replica, primary *ServerNode, forceSync bool) bool {
	// 强制同步（如新节点注册）
	if forceSync {
		return true
	}

	// 使用读锁保护对节点状态的访问
	cn.mu.RLock()
	replicaStatus := replica.Status
	primaryStatus := primary.Status
	replicaLSN := replica.LSN
	primaryLSN := primary.LSN
	replicaSyncRetryCount := replica.SyncRetryCount
	replicaLastSyncTime := replica.LastSyncTime
	cn.mu.RUnlock()

	// 检查节点状态
	if replicaStatus != NodeStatusActive || primaryStatus != NodeStatusActive {
		return false
	}

	// 如果有LSN差异且距离上次同步已超过同步间隔，则触发同步
	lsnDiff := primaryLSN - replicaLSN
	timeSinceLastSync := time.Since(replicaLastSyncTime)

	// 如果没有LSN差异，不需要同步
	if lsnDiff <= 0 {
		return false
	}

	// 检查是否在冷却期（只有重试耗尽后才会进入冷却期）
	if replicaSyncRetryCount >= maxSyncRetries && timeSinceLastSync < syncCooldown {
		logger.Info("Node %s is in sync cooldown period after %d failed retries, skipping (cooldown ends in %v)",
			replica.ID, replicaSyncRetryCount, syncCooldown-timeSinceLastSync)
		return false
	}

	// 如果重试次数耗尽且冷却期已过，重置重试计数（需要写锁）
	if replicaSyncRetryCount >= maxSyncRetries && timeSinceLastSync >= syncCooldown {
		logger.Info("Cooldown period ended for %s, resetting retry count", replica.ID)
		cn.mu.Lock()
		replica.SyncRetryCount = 0
		cn.mu.Unlock()
	}

	// 基于时间间隔的同步策略：有LSN差异且距离上次同步超过syncInterval时间
	shouldSync := timeSinceLastSync >= syncInterval

	logger.Info("Sync decision for %s: LSN diff=%d, time since last sync=%v (interval=%v), retry count=%d/%d, should sync=%v",
		replica.ID, lsnDiff, timeSinceLastSync, syncInterval, replicaSyncRetryCount, maxSyncRetries, shouldSync)

	return shouldSync
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

	// 使用读锁获取当前的重试计数来记录日志
	cn.mu.RLock()
	currentRetryCount := replica.SyncRetryCount
	cn.mu.RUnlock()

	logger.Info("Starting sync for %s (attempt %d/%d)", replica.ID, currentRetryCount+1, maxSyncRetries)

	// 执行同步
	err := cn.performStreamDataSync(primary, replica)

	// 处理同步结果
	if err != nil {
		// 同步失败，更新状态（需要锁保护）
		cn.mu.Lock()
		replica.SyncRetryCount++
		logger.Error("Sync failed for %s (attempt %d/%d): %v",
			replica.ID, replica.SyncRetryCount, maxSyncRetries, err)

		if replica.SyncRetryCount >= maxSyncRetries {
			logger.Error("Max retries reached for %s, entering cooldown period", replica.ID)
			replica.LastSyncTime = time.Now() // 进入冷却期
		}
		replica.Status = NodeStatusActive // 恢复活跃状态
		cn.mu.Unlock()
	} else {
		// 同步成功，更新状态（需要锁保护）
		cn.mu.Lock()
		logger.Info("Sync completed successfully for %s", replica.ID)
		replica.Status = NodeStatusActive
		replica.SyncRetryCount = 0 // 重置重试计数
		replica.LastSyncTime = time.Now()
		cn.mu.Unlock()

		// 验证同步结果（在锁外执行网络操作）
		cn.verifySyncResult(replica, primary)
	}
}

// verifySyncResult 验证同步结果（修复竞态条件）
func (cn *CoordinatorNode) verifySyncResult(replica, primary *ServerNode) {
	// 使用gRPC获取节点的最新LSN
	if replica.grpcClient == nil {
		logger.Error("gRPC client not initialized for replica %s", replica.ID)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // 增加超时时间
	defer cancel()

	resp, err := replica.grpcClient.GetLSN(ctx, &serverpb.Empty{})
	if err != nil {
		logger.Error("Failed to get LSN from node %s: %v", replica.ID, err)
		return
	}

	newLSN := resp.Lsn
	logger.Info("Node %s current LSN after sync: %d", replica.ID, newLSN)

	// 使用读锁获取primary的LSN
	cn.mu.RLock()
	primaryLSN := primary.LSN
	cn.mu.RUnlock()

	lsnDiff := primaryLSN - newLSN
	logger.Info("Sync result: %s LSN=%d, Primary LSN=%d, remaining diff=%d",
		replica.ID, newLSN, primaryLSN, lsnDiff)

	// 使用写锁更新节点的LSN值
	cn.mu.Lock()
	replica.LSN = newLSN
	cn.mu.Unlock()
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
			// 使用读锁获取当前LSN值以记录日志
			cn.mu.RLock()
			lsnDiff := primary.LSN - replica.LSN
			cn.mu.RUnlock()

			logger.Info("Triggering periodic sync for %s (LSN diff: %d)",
				replica.ID, lsnDiff)
			go cn.performSyncWithRetry(replica, primary)
		}
	}
}

// RegisterServer 实现gRPC RegisterServer方法
func (cn *CoordinatorNode) RegisterServer(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	logger.Info("Received gRPC registration request from %s:%s (ID: %s, LSN: %d)", req.Address, req.Port, req.NodeId, req.Lsn)

	// 计算gRPC端口（假设server的gRPC端口是主端口+100）
	portNum, err := strconv.Atoi(req.Port)
	if err != nil {
		return &pb.RegisterResponse{
			Success: false,
			Message: "Invalid port number",
		}, nil
	}
	grpcPort := strconv.Itoa(portNum + 100)
	cn.mu.Lock()
	// 注意：不使用defer，因为我们需要在方法中间释放锁

	// 检查是否为重复注册
	existingNode, exists := cn.serverNodes[req.NodeId]
	if exists {
		logger.Info("Node %s re-registering, updating existing node info (LSN: %d -> %d)", req.NodeId, existingNode.LSN, req.Lsn)

		// 更新现有节点的信息
		existingNode.Address = req.Address
		existingNode.Port = req.Port
		existingNode.GRPCPort = grpcPort
		existingNode.LSN = req.Lsn
		existingNode.LastSeen = time.Now()
		existingNode.Status = NodeStatusActive

		// 检查是否需要重新建立gRPC连接
		serverAddr := fmt.Sprintf("%s:%s", req.Address, grpcPort)
		if existingNode.grpcConn == nil {
			logger.Info("Re-establishing gRPC connection for node %s", req.NodeId)
			grpcConn, err := grpc.Dial(serverAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: true,
				}),
			)
			if err != nil {
				logger.Error("Failed to re-establish gRPC connection for %s: %v", req.NodeId, err)
				cn.mu.Unlock() // 错误处理，释放锁
				return &pb.RegisterResponse{
					Success: false,
					Message: fmt.Sprintf("Failed to re-establish gRPC connection: %v", err),
				}, nil
			}
			existingNode.grpcConn = grpcConn
			existingNode.grpcClient = serverpb.NewDatabaseServiceClient(grpcConn)
		}

		cn.mu.Unlock() // 重复注册处理完成，释放锁
		return &pb.RegisterResponse{
			Success: true,
			Role:    existingNode.Role.String(),
			Message: "Re-registration successful",
		}, nil
	}

	// 新节点注册 - 创建到服务器的gRPC连接
	logger.Info("New node %s registering with address %s and port %s", req.NodeId, req.Address, grpcPort)
	serverAddr := fmt.Sprintf("%s:%s", req.Address, grpcPort)
	grpcConn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // 与服务器端保持一致
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		logger.Error("Failed to connect to server %s via gRPC: %v", req.NodeId, err)
		cn.mu.Unlock() // 错误处理，释放锁
		return &pb.RegisterResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to establish gRPC connection: %v", err),
		}, nil
	}

	// 创建服务器节点
	node := &ServerNode{
		ID:         req.NodeId,
		Address:    req.Address,
		Port:       req.Port,
		GRPCPort:   grpcPort,
		Role:       NodeRoleReplica, // 默认为从节点
		Status:     NodeStatusActive,
		LSN:        req.Lsn, // 使用节点注册时的LSN
		LastSeen:   time.Now(),
		grpcConn:   grpcConn,
		grpcClient: serverpb.NewDatabaseServiceClient(grpcConn),
	}

	cn.serverNodes[req.NodeId] = node
	nodeCount := len(cn.serverNodes)
	wasStarted := cn.started
	// 释放锁，避免死锁
	cn.mu.Unlock()

	logger.Info("Server %s registered successfully with LSN %d. Total nodes: %d", req.NodeId, req.Lsn, nodeCount)

	// 检查是否可以启动系统
	if !wasStarted && nodeCount >= minNodes {
		// 在锁外调用 startSystem()
		go func() {
			cn.startSystem()

			// 系统刚刚启动，检查所有副本节点是否需要同步
			cn.mu.RLock()
			primary := cn.primaryNode
			var replicasNeedingSync []*ServerNode

			if primary != nil {
				for _, serverNode := range cn.serverNodes {
					if serverNode.Role == NodeRoleReplica && serverNode.LSN < primary.LSN {
						replicasNeedingSync = append(replicasNeedingSync, serverNode)
					}
				}
			}
			cn.mu.RUnlock()

			// 对所有需要同步的副本节点启动数据同步
			for _, replica := range replicasNeedingSync {
				logger.Info("System startup: initiating sync for replica %s (LSN: %d) from primary %s (LSN: %d)",
					replica.ID, replica.LSN, primary.ID, primary.LSN)
				go cn.performDataCatchup(replica)
			}

			if len(replicasNeedingSync) > 0 {
				logger.Info("System startup: initiated sync for %d replica nodes", len(replicasNeedingSync))
			} else {
				logger.Info("System startup: all replica nodes are up-to-date")
			}
		}()
	} else {
		// 打印当前注册的节点
		logger.Info("Current registered nodes: %v", cn.serverNodes)

		// 系统已经运行，检查当前注册的节点是否需要同步
		cn.mu.RLock()
		needsCatchup := cn.started && cn.primaryNode != nil && node.LSN < cn.primaryNode.LSN
		cn.mu.RUnlock()

		// 如果系统已启动且新节点LSN小于主节点，启动数据同步
		if needsCatchup {
			go cn.performDataCatchup(node)
		}
	}

	// 由于我们手动释放了锁，这里不需要defer unlock
	return &pb.RegisterResponse{
		Success: true,
		Role:    node.Role.String(),
		Message: "Registration successful",
	}, nil
}

// Heartbeat 实现gRPC Heartbeat方法
func (cn *CoordinatorNode) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// 更新节点的最后心跳时间和LSN
	cn.mu.Lock()
	if node, exists := cn.serverNodes[req.NodeId]; exists {
		oldStatus := node.Status
		node.LastSeen = time.Now()
		node.LSN = req.Lsn
		// 如果节点之前是DOWN状态，重新标记为ACTIVE
		if node.Status == NodeStatusDown {
			logger.Info("Node %s is back online", req.NodeId)
			node.Status = NodeStatusActive
		}
		// 只在状态变化时记录日志
		if oldStatus != node.Status {
			logger.Info("Node %s status changed from %s to %s", req.NodeId, oldStatus.String(), node.Status.String())
		}
	}
	cn.mu.Unlock()

	return &pb.HeartbeatResponse{
		Success:   true,
		Timestamp: time.Now().Unix(),
	}, nil
}

// StreamBackup 实现gRPC StreamBackup方法（CN作为客户端从server拉取数据）
func (cn *CoordinatorNode) StreamBackup(req *pb.BackupRequest, stream pb.CoordinatorService_StreamBackupServer) error {
	// 这个方法在当前架构中不需要，因为CN是从server拉取数据，而不是server推送到CN
	return fmt.Errorf("not implemented: CN pulls data from servers, not the other way")
}

// StreamRestore 实现gRPC StreamRestore方法（CN作为客户端推送数据到server）
func (cn *CoordinatorNode) StreamRestore(stream pb.CoordinatorService_StreamRestoreServer) error {
	// 这个方法在当前架构中不需要，因为CN是主动推送数据到server，而不是server从CN拉取
	return fmt.Errorf("not implemented: CN pushes data to servers, not pulled by servers")
}

func main() {
	// 初始化日志器
	logger.Init()
	logger.SetComponent("CN")
	defer logger.Close()

	port := flag.String("port", defaultPort, "Port for client connections")
	grpcPort := flag.String("grpc-port", defaultGRPCPort, "Port for gRPC connections")
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Println("Distributed Database Coordinator Node")
		fmt.Println("Usage: coordinator [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	cn := NewCoordinatorNode(*port, *grpcPort)
	if err := cn.Start(); err != nil {
		logger.Error("Failed to start coordinator: %v", err)
		os.Exit(1)
	}
}
