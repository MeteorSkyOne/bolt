package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chzyer/readline"
)

const (
	defaultServerHost = "localhost"
	defaultServerPort = "8080"
)

// ExecuteParams 执行参数结构体
type ExecuteParams struct {
	InputFile string
	ExecNum   int
	ExecTime  int
}

// ExecuteResult 执行结果结构体
type ExecuteResult struct {
	ProcessID int
	Success   bool
	Output    string
	Error     string
}

// DatabaseClient 客户端结构体
type DatabaseClient struct {
	host      string
	port      string
	conn      net.Conn
	reader    *bufio.Reader
	connected bool
	history   []string // 命令历史记录
}

// NewDatabaseClient 创建新的数据库客户端
func NewDatabaseClient(host, port string) (*DatabaseClient, error) {
	dc := &DatabaseClient{
		host:    host,
		port:    port,
		history: make([]string, 0),
	}

	err := dc.Connect()
	if err != nil {
		return nil, err
	}

	return dc, nil
}

// Connect 连接到服务器
func (dc *DatabaseClient) Connect() error {
	if dc.connected {
		return fmt.Errorf("already connected")
	}

	serverAddress := dc.host + ":" + dc.port
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to server at %s: %v", serverAddress, err)
	}

	dc.conn = conn
	dc.reader = bufio.NewReader(conn)
	dc.connected = true
	return nil
}

// Close 关闭客户端连接
func (dc *DatabaseClient) Close() error {
	if !dc.connected || dc.conn == nil {
		return nil
	}

	err := dc.conn.Close()
	dc.conn = nil
	dc.reader = nil
	dc.connected = false
	return err
}

// IsConnected 检查连接状态
func (dc *DatabaseClient) IsConnected() bool {
	return dc.connected && dc.conn != nil
}

// AddToHistory 添加命令到历史记录
func (dc *DatabaseClient) AddToHistory(command string) {
	// 避免重复添加相同的命令
	if len(dc.history) > 0 && dc.history[len(dc.history)-1] == command {
		return
	}

	dc.history = append(dc.history, command)

	// 限制历史记录数量为100条
	if len(dc.history) > 100 {
		dc.history = dc.history[1:]
	}
}

// GetHistory 获取历史记录
func (dc *DatabaseClient) GetHistory() []string {
	return dc.history
}

// createCompleter 创建自动补全器
func (dc *DatabaseClient) createCompleter() readline.AutoCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("PUT"),
		readline.PcItem("GET"),
		readline.PcItem("DEL"),
		readline.PcItem("BEGIN"),
		readline.PcItem("COMMIT"),
		readline.PcItem("ABORT"),
		readline.PcItem("SHOW"),
		readline.PcItem("HELP"),
		readline.PcItem("EXECUTE"),
		readline.PcItem("OPEN"),
		readline.PcItem("CLOSE"),
		readline.PcItem("EXIT"),
		readline.PcItem("QUIT"),
		readline.PcItem("HISTORY"),
		readline.PcItem("CLEAR"),
	)
}

// ProcessHistoryCommand 处理历史相关命令
func (dc *DatabaseClient) ProcessHistoryCommand(command string) (string, bool) {
	cmd := strings.TrimSpace(command)

	// !! - 重复最后一个命令
	if cmd == "!!" {
		if len(dc.history) == 0 {
			return "", false
		}
		return dc.history[len(dc.history)-1], true
	}

	// !n - 重复第n个命令 (1-based)
	if strings.HasPrefix(cmd, "!") && len(cmd) > 1 {
		numStr := cmd[1:]
		if num, err := strconv.Atoi(numStr); err == nil {
			if num > 0 && num <= len(dc.history) {
				return dc.history[num-1], true
			}
		}
		return "", false
	}

	// history - 显示历史记录
	if strings.ToUpper(cmd) == "HISTORY" {
		if len(dc.history) == 0 {
			fmt.Println("No command history")
		} else {
			fmt.Println("Command history:")
			for i, histCmd := range dc.history {
				fmt.Printf("%3d  %s\n", i+1, histCmd)
			}
		}
		return "", false
	}

	return "", false
}

// SendCommand 发送命令到服务器
func (dc *DatabaseClient) SendCommand(command string) error {
	if !dc.IsConnected() {
		return fmt.Errorf("not connected to server")
	}
	_, err := dc.conn.Write([]byte(command + "\n"))
	return err
}

// ReadResponse 从服务器读取响应
func (dc *DatabaseClient) ReadResponse() (string, error) {
	if !dc.IsConnected() {
		return "", fmt.Errorf("not connected to server")
	}
	response, err := dc.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(response, "\n"), nil
}

// isLocalCommand 检查是否是本地命令
func (dc *DatabaseClient) isLocalCommand(command string) bool {
	cmd := strings.ToUpper(strings.Fields(command)[0])
	switch cmd {
	case "EXECUTE", "CLOSE", "OPEN", "HISTORY", "CLEAR":
		return true
	default:
		return false
	}
}

// processLocalCommand 处理本地命令，返回是否已处理和是否应该退出
func (dc *DatabaseClient) processLocalCommand(command string, isInteractive bool) (handled bool, shouldExit bool) {
	cmd := strings.ToUpper(strings.Fields(command)[0])

	switch cmd {
	case "EXECUTE":
		// 处理EXECUTE命令（本地执行）
		dc.executeCommand(command)
		return true, false

	case "CLOSE":
		if dc.IsConnected() {
			err := dc.Close()
			if err != nil {
				if isInteractive {
					fmt.Printf("Error closing connection: %v\n", err)
				}
			} else {
				if isInteractive {
					fmt.Println("Connection closed")
				}
			}
		} else {
			if isInteractive {
				fmt.Println("Not connected")
			}
		}
		return true, false

	case "OPEN":
		if dc.IsConnected() {
			if isInteractive {
				fmt.Println("Already connected")
			}
		} else {
			err := dc.Connect()
			if err != nil {
				if isInteractive {
					fmt.Printf("Error connecting: %v\n", err)
				}
			} else {
				if isInteractive {
					fmt.Println("Connected to server")
					// 读取欢迎消息
					for i := 0; i < 2; i++ {
						welcome, err := dc.ReadResponse()
						if err != nil {
							fmt.Printf("Error reading welcome message: %v\n", err)
							break
						}
						fmt.Println(welcome)
					}
				} else {
					// 静默处理欢迎消息
					for i := 0; i < 2; i++ {
						_, err := dc.ReadResponse()
						if err != nil {
							break
						}
					}
				}
			}
		}
		return true, false

	case "HISTORY":
		if isInteractive {
			dc.ProcessHistoryCommand(command)
		}
		return true, false

	case "CLEAR":
		if isInteractive {
			// 清屏命令
			fmt.Print("\033[H\033[2J")
		}
		return true, false

	case "EXIT", "QUIT":
		// 如果连接着，发送退出命令到服务器
		if dc.IsConnected() {
			dc.SendCommand(command)
			// 读取服务器响应
			if response, err := dc.ReadResponse(); err == nil {
				if isInteractive {
					dc.outputResult(command, response)
				}
			}
		}
		return true, true

	default:
		return false, false
	}
}

// RunInteractive 运行交互式客户端
func (dc *DatabaseClient) RunInteractive() {
	fmt.Println("Interactive Database Client")
	fmt.Println("Connecting to server...")

	// 读取欢迎消息
	for i := 0; i < 2; i++ {
		welcome, err := dc.ReadResponse()
		if err != nil {
			fmt.Printf("Error reading welcome message: %v\n", err)
			return
		}
		fmt.Println(welcome)
	}

	fmt.Println(strings.Repeat("-", 55))

	// 配置readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "db> ",
		HistoryFile:     filepath.Join(os.TempDir(), ".db_history"),
		AutoComplete:    dc.createCompleter(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Printf("Error setting up readline: %v\n", err)
		return
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if len(line) == 0 {
					break
				} else {
					continue
				}
			} else {
				break
			}
		}

		command := strings.TrimSpace(line)
		if command == "" {
			continue
		}

		// 处理历史命令
		if histCmd, isHist := dc.ProcessHistoryCommand(command); isHist {
			if histCmd == "" {
				fmt.Println("History command not found")
				continue
			}
			command = histCmd
			fmt.Printf("Executing: %s\n", command)
		}

		// 处理本地命令
		if handled, shouldExit := dc.processLocalCommand(command, true); handled {
			if shouldExit {
				return
			}
			continue
		}

		// 添加到历史记录（排除历史命令本身）
		if !strings.HasPrefix(command, "!") && strings.ToUpper(command) != "HISTORY" {
			dc.AddToHistory(command)
		}

		// 检查连接状态
		if !dc.IsConnected() {
			fmt.Println("Not connected to server. Use 'OPEN' to connect.")
			continue
		}

		// 发送命令到服务器
		err = dc.SendCommand(command)
		if err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			// 如果是连接错误，标记为断开
			if strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "connection reset") {
				dc.connected = false
				fmt.Println("Connection lost. Use 'OPEN' to reconnect.")
			}
			continue
		}

		// 读取并处理响应
		response, err := dc.ReadResponse()
		if err != nil {
			fmt.Printf("Error reading response: %v\n", err)
			// 如果是连接错误，标记为断开
			if strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "connection reset") {
				dc.connected = false
				fmt.Println("Connection lost. Use 'OPEN' to reconnect.")
			}
			continue
		}

		// 使用统一的结果输出函数
		dc.outputResult(command, response)
	}

	fmt.Println("\nDisconnected from server.")
}

// RunBatch 运行批处理模式
func (dc *DatabaseClient) RunBatch(testFile string) {
	file, err := os.Open(testFile)
	if err != nil {
		fmt.Printf("Test file %s not found: %v\n", testFile, err)
		return
	}
	defer file.Close()

	// 读取欢迎消息
	for i := 0; i < 2; i++ {
		_, err := dc.ReadResponse()
		if err != nil {
			fmt.Printf("Error reading welcome message: %v\n", err)
			return
		}
	}

	fmt.Printf("Running test case from %s\n", testFile)
	fmt.Println(strings.Repeat("-", 50))

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command != "" && !strings.HasPrefix(command, "#") {
			fmt.Printf("db> %s\n", command)

			// 处理本地命令
			if handled, shouldExit := dc.processLocalCommand(command, false); handled {
				if shouldExit {
					break
				}
				continue
			}

			// 检查连接状态
			if !dc.IsConnected() {
				fmt.Println("Not connected to server")
				break
			}

			// 发送命令到服务器
			err := dc.SendCommand(command)
			if err != nil {
				fmt.Printf("Error sending command: %v\n", err)
				break
			}

			// 读取并显示响应
			response, err := dc.ReadResponse()
			if err != nil {
				fmt.Printf("Error reading response: %v\n", err)
				break
			}

			fmt.Println(response)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading test file: %v\n", err)
	}

	fmt.Println("Test case completed.")
}

// RunSingleCommand 运行单个命令模式
func (dc *DatabaseClient) RunSingleCommand(command string) {
	// 处理本地命令
	if handled, _ := dc.processLocalCommand(command, false); handled {
		return
	}

	// 读取欢迎消息（静默处理）
	for i := 0; i < 2; i++ {
		_, err := dc.ReadResponse()
		if err != nil {
			fmt.Printf("Error reading welcome message: %v\n", err)
			return
		}
	}

	// 检查连接状态
	if !dc.IsConnected() {
		fmt.Printf("Not connected to server\n")
		return
	}

	// 发送命令到服务器
	err := dc.SendCommand(command)
	if err != nil {
		fmt.Printf("Error sending command: %v\n", err)
		return
	}

	// 读取并处理响应
	response, err := dc.ReadResponse()
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		return
	}

	// 解析响应，只输出结果值
	dc.outputResult(command, response)
}

// outputResult 根据命令类型输出相应的结果
func (dc *DatabaseClient) outputResult(command, response string) {
	cmd := strings.ToUpper(strings.Fields(command)[0])

	switch cmd {
	case "GET":
		// 对于GET命令，只输出值部分
		// 响应格式: "GET key -> value" 或 "GET key -> Key not found"
		if strings.Contains(response, " -> ") {
			parts := strings.Split(response, " -> ")
			if len(parts) == 2 {
				value := strings.TrimSpace(parts[1])
				if value == "Key not found" {
					// 对于不存在的键，输出Key not found
					fmt.Println("Key not found")
				} else {
					fmt.Println(value)
				}
				return
			}
		}
		fmt.Println(response)

	case "SHOW":
		// 对于SHOW命令，只输出数据部分，不包含"Current database contents:"头部
		lines := strings.Split(response, "\n")
		for i, line := range lines {
			line = strings.TrimSpace(line)
			if line == "Current database contents:" {
				continue // 跳过标题行
			}
			if line == "Database is empty" {
				fmt.Println(line)
				continue
			}
			if strings.HasPrefix(line, "Transaction status:") {
				fmt.Println(line)
				continue
			}
			if line != "" && i > 0 { // 跳过空行和标题行
				fmt.Println(line)
			}
		}

	case "PUT", "DEL":
		// 对于PUT和DEL命令，只输出操作结果状态
		if strings.Contains(response, " -> ") {
			parts := strings.Split(response, " -> ")
			if len(parts) == 2 {
				result := strings.TrimSpace(parts[1])
				fmt.Println(result)
				return
			}
		}
		fmt.Println(response)

	case "HELP":
		// 对于HELP命令，显示增强的帮助信息
		fmt.Println(response)
		fmt.Println("\nClient commands:")
		fmt.Println("  EXECUTE          - Run multiple processes concurrently")
		fmt.Println("                     Format: EXECUTE NumProcess OutputFile [InputFile ExecNum ExecTime]...")
		fmt.Println("  OPEN             - Connect to server")
		fmt.Println("  CLOSE            - Disconnect from server")
		fmt.Println("  HISTORY          - Show command history")
		fmt.Println("  CLEAR            - Clear screen")
		fmt.Println("  !!               - Repeat last command")
		fmt.Println("  !n               - Repeat nth command from history")
		fmt.Println("\nNavigation:")
		fmt.Println("  ↑/↓ arrows       - Browse command history")
		fmt.Println("  Tab              - Auto-complete commands")
		fmt.Println("  Ctrl+C           - Interrupt current input")
		fmt.Println("  Ctrl+D           - Exit client")

	default:
		// 对于其他命令（BEGIN, COMMIT, ABORT等），输出完整响应
		fmt.Println(response)
	}
}

// parseExecuteCommand 解析EXECUTE命令
func (dc *DatabaseClient) parseExecuteCommand(command string) (int, string, []ExecuteParams, error) {
	parts := strings.Fields(command)
	if len(parts) < 3 {
		return 0, "", nil, fmt.Errorf("EXECUTE command requires at least NumProcess and OutputFile")
	}

	// 解析NumProcess
	numProcess, err := strconv.Atoi(parts[1])
	if err != nil || numProcess <= 0 {
		return 0, "", nil, fmt.Errorf("invalid NumProcess: %s", parts[1])
	}

	// 解析OutputFile
	outputFile := parts[2]

	// 解析进程参数
	if len(parts) != 3+numProcess*3 {
		return 0, "", nil, fmt.Errorf("expected %d parameters but got %d", 3+numProcess*3, len(parts))
	}

	params := make([]ExecuteParams, numProcess)
	for i := 0; i < numProcess; i++ {
		baseIdx := 3 + i*3

		inputFile := parts[baseIdx]
		execNum, err := strconv.Atoi(parts[baseIdx+1])
		if err != nil {
			return 0, "", nil, fmt.Errorf("invalid ExecNum for process %d: %s", i+1, parts[baseIdx+1])
		}

		execTime, err := strconv.Atoi(parts[baseIdx+2])
		if err != nil {
			return 0, "", nil, fmt.Errorf("invalid ExecTime for process %d: %s", i+1, parts[baseIdx+2])
		}

		// 验证ExecNum和ExecTime只有一个为非-1
		if (execNum != -1 && execTime != -1) || (execNum == -1 && execTime == -1) {
			return 0, "", nil, fmt.Errorf("process %d: exactly one of ExecNum or ExecTime must be -1", i+1)
		}

		params[i] = ExecuteParams{
			InputFile: inputFile,
			ExecNum:   execNum,
			ExecTime:  execTime,
		}
	}

	return numProcess, outputFile, params, nil
}

// executeProcess 执行单个进程
func (dc *DatabaseClient) executeProcess(processID int, params ExecuteParams, outputFile string, results chan<- ExecuteResult) {
	defer func() {
		if r := recover(); r != nil {
			results <- ExecuteResult{
				ProcessID: processID,
				Success:   false,
				Error:     fmt.Sprintf("Process panicked: %v", r),
			}
		}
	}()

	var output strings.Builder
	var execCount int
	startTime := time.Now()

	// 检查输入文件是否存在
	if _, err := os.Stat(params.InputFile); os.IsNotExist(err) {
		results <- ExecuteResult{
			ProcessID: processID,
			Success:   false,
			Error:     fmt.Sprintf("Input file not found: %s", params.InputFile),
		}
		return
	}

	for {
		// 检查执行次数限制
		if params.ExecNum != -1 && execCount >= params.ExecNum {
			break
		}

		// 检查执行时间限制
		if params.ExecTime != -1 && time.Since(startTime).Seconds() >= float64(params.ExecTime) {
			break
		}

		execCount++

		// 创建新的客户端进程执行批处理，使用当前客户端的端口和host
		cmd := exec.Command("./bin/client", "--host", dc.host, "--port", dc.port, "--batch", params.InputFile)
		cmdOutput, err := cmd.CombinedOutput()

		if err != nil {
			output.WriteString(fmt.Sprintf("Process %d Execution %d Error: %v\n", processID, execCount, err))
		} else {
			// 只保留实际的命令输出，移除额外信息
			lines := strings.Split(string(cmdOutput), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				// 跳过标准的批处理输出行
				if line != "" &&
					!strings.HasPrefix(line, "Running test case from") &&
					!strings.HasPrefix(line, "Test case completed") &&
					!strings.Contains(line, "----") &&
					!strings.HasPrefix(line, "db>") {
					output.WriteString(line + "\n")
				}
			}
		}

		// 如果是按时间执行，添加小延迟避免过于频繁
		if params.ExecTime != -1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	results <- ExecuteResult{
		ProcessID: processID,
		Success:   true,
		Output:    output.String(),
	}
}

// executeCommand 执行EXECUTE命令
func (dc *DatabaseClient) executeCommand(command string) {
	numProcess, outputFile, params, err := dc.parseExecuteCommand(command)
	if err != nil {
		fmt.Printf("Error parsing EXECUTE command: %v\n", err)
		return
	}

	fmt.Printf("Starting %d processes...\n", numProcess)
	fmt.Printf("Output will be saved to: %s\n", outputFile)

	// 创建结果通道
	results := make(chan ExecuteResult, numProcess)

	// 启动所有进程
	var wg sync.WaitGroup
	for i := 0; i < numProcess; i++ {
		wg.Add(1)
		go func(processID int, p ExecuteParams) {
			defer wg.Done()
			dc.executeProcess(processID, p, outputFile, results)
		}(i+1, params[i])
	}

	// 等待所有进程完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	var allOutput strings.Builder
	successCount := 0

	for result := range results {
		if result.Success {
			successCount++
			allOutput.WriteString(result.Output)
		} else {
			allOutput.WriteString(fmt.Sprintf("Process %d FAILED: %s\n", result.ProcessID, result.Error))
		}
	}

	// 写入输出文件
	if err := os.WriteFile(outputFile, []byte(allOutput.String()), 0644); err != nil {
		fmt.Printf("Error writing to output file: %v\n", err)
	} else {
		fmt.Printf("Results saved to %s\n", outputFile)
	}

	// 显示摘要
	fmt.Printf("Execution completed: %d/%d processes successful\n", successCount, numProcess)
}

func main() {
	// 解析命令行参数
	port := flag.String("port", defaultServerPort, "Server port to connect to")
	host := flag.String("host", defaultServerHost, "Server host to connect to")
	batch := flag.String("batch", "", "Run in batch mode with specified test file")
	command := flag.String("command", "", "Run single command and exit")
	help := flag.Bool("help", false, "Show help message")
	flag.Parse()

	if *help {
		fmt.Println("Interactive Database Client")
		fmt.Println("Usage: client [options]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Println("  client                           # Interactive mode")
		fmt.Println("  client -port 9000                # Connect to custom port")
		fmt.Println("  client -batch test.txt           # Batch mode")
		fmt.Println("  client -command \"PUT A 1\"        # Single command")
		return
	}

	client, err := NewDatabaseClient(*host, *port)
	if err != nil {
		fmt.Printf("Failed to connect to database server: %v\n", err)
		fmt.Printf("Make sure the server is running on %s:%s\n", *host, *port)
		os.Exit(1)
	}
	defer client.Close()

	// 处理不同的运行模式
	if *batch != "" {
		client.RunBatch(*batch)
	} else if *command != "" {
		client.RunSingleCommand(*command)
	} else {
		// 检查旧式命令行参数兼容性
		args := flag.Args()
		if len(args) > 0 {
			// 兼容模式：直接指定测试文件
			client.RunBatch(args[0])
		} else {
			// 运行交互式模式
			client.RunInteractive()
		}
	}
}
