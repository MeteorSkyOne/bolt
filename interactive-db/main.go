package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meteorsky/bolt"
)

const (
	dbFile     = "interactive.db"
	bucketName = "kv_store"
)

// TransactionOperation 事务操作类型
type TransactionOperation struct {
	Type  string // "PUT", "GET", "DEL"
	Key   string
	Value string
}

// ProcessConfig 进程配置
type ProcessConfig struct {
	InputFile string
	ExecNum   int // -1表示不限制次数
	ExecTime  int // -1表示不限制时间（秒）
}

// InteractiveDB 交互式数据库结构体
type InteractiveDB struct {
	db               *bolt.DB
	inTransaction    bool
	transactionOps   []TransactionOperation
	transactionCache map[string]string // 事务内的临时缓存
	systemOpened     bool              // 系统是否已打开
}

// NewInteractiveDB 创建新的数据库实例
func NewInteractiveDB() (*InteractiveDB, error) {
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

	return &InteractiveDB{
		db:               db,
		inTransaction:    false,
		transactionOps:   make([]TransactionOperation, 0),
		transactionCache: make(map[string]string),
		systemOpened:     false,
	}, nil
}

// Close 关闭数据库
func (idb *InteractiveDB) Close() error {
	if idb.db != nil {
		return idb.db.Close()
	}
	return nil
}

// Begin 开始事务
func (idb *InteractiveDB) Begin() {
	if idb.inTransaction {
		fmt.Println("Error: Already in transaction")
		return
	}
	idb.inTransaction = true
	idb.transactionOps = make([]TransactionOperation, 0)
	idb.transactionCache = make(map[string]string)
	fmt.Println("BEGIN -> transaction started")
}

// Commit 提交事务
func (idb *InteractiveDB) Commit() {
	if !idb.inTransaction {
		fmt.Println("Error: No active transaction")
		return
	}

	// 执行所有缓存的操作
	err := idb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))

		for _, op := range idb.transactionOps {
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
		fmt.Printf("Error committing transaction: %v\n", err)
	} else {
		fmt.Println("COMMIT -> transaction committed")
	}

	// 清理事务状态
	idb.inTransaction = false
	idb.transactionOps = make([]TransactionOperation, 0)
	idb.transactionCache = make(map[string]string)
}

// Abort 中止事务
func (idb *InteractiveDB) Abort() {
	if !idb.inTransaction {
		fmt.Println("Error: No active transaction")
		return
	}

	// 清理事务状态，不执行任何操作
	idb.inTransaction = false
	idb.transactionOps = make([]TransactionOperation, 0)
	idb.transactionCache = make(map[string]string)
	fmt.Println("ABORT -> transaction aborted")
}

// getValueFromCacheOrDB 从缓存或数据库获取值
func (idb *InteractiveDB) getValueFromCacheOrDB(key string) (string, bool) {
	// 如果在事务中，先检查缓存
	if idb.inTransaction {
		if value, exists := idb.transactionCache[key]; exists {
			return value, true
		}
	}

	// 从数据库获取
	var value string
	var found bool
	err := idb.db.View(func(tx *bolt.Tx) error {
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

// Put 存储或更新键值对
func (idb *InteractiveDB) Put(key, value string) {
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
			fmt.Printf("Error: Invalid constant value: %s\n", constStr)
			return
		}

		// 获取变量当前值
		currentValStr, found := idb.getValueFromCacheOrDB(varName)
		if !found {
			fmt.Printf("Error: Variable %s does not exist\n", varName)
			return
		}

		currentVal, err := strconv.Atoi(currentValStr)
		if err != nil {
			fmt.Printf("Error: Variable %s is not a number\n", varName)
			return
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
			fmt.Printf("Error: Invalid value format: %s\n", value)
			return
		}
		finalValue = value
	}

	if idb.inTransaction {
		// 在事务中，添加到操作列表和缓存
		idb.transactionOps = append(idb.transactionOps, TransactionOperation{
			Type:  "PUT",
			Key:   key,
			Value: finalValue,
		})
		idb.transactionCache[key] = finalValue
		fmt.Printf("PUT %s %s -> staged in transaction\n", key, value)
	} else {
		// 立即执行
		err := idb.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			return b.Put([]byte(key), []byte(finalValue))
		})

		if err != nil {
			fmt.Printf("Error storing value: %v\n", err)
			return
		}
		fmt.Printf("PUT %s %s -> stored\n", key, value)
	}
}

// Get 查询键对应的值
func (idb *InteractiveDB) Get(key string) {
	value, found := idb.getValueFromCacheOrDB(key)

	if found {
		fmt.Printf("GET %s -> %s\n", key, value)
	} else {
		fmt.Printf("GET %s -> Key not found\n", key)
	}
}

// Delete 删除键值对
func (idb *InteractiveDB) Delete(key string) {
	if idb.inTransaction {
		// 在事务中，添加到操作列表
		idb.transactionOps = append(idb.transactionOps, TransactionOperation{
			Type: "DEL",
			Key:  key,
		})
		// 从缓存中删除（标记为已删除）
		delete(idb.transactionCache, key)
		fmt.Printf("DEL %s -> staged in transaction\n", key)
	} else {
		// 立即执行
		var found bool

		// 首先检查key是否存在
		err := idb.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			data := b.Get([]byte(key))
			if data != nil {
				found = true
			}
			return nil
		})

		if err != nil {
			fmt.Printf("Error checking key: %v\n", err)
			return
		}

		if found {
			// 删除键值对
			err = idb.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucketName))
				return b.Delete([]byte(key))
			})

			if err != nil {
				fmt.Printf("Error deleting key: %v\n", err)
				return
			}
			fmt.Printf("DEL %s -> deleted\n", key)
		} else {
			fmt.Printf("DEL %s -> Key not found\n", key)
		}
	}
}

// ExecuteCommand 执行命令
func (idb *InteractiveDB) ExecuteCommand(command string) {
	parts := strings.Fields(strings.TrimSpace(command))
	if len(parts) == 0 {
		return
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "BEGIN":
		idb.Begin()

	case "COMMIT":
		idb.Commit()

	case "ABORT":
		idb.Abort()

	case "PUT":
		if len(parts) < 3 {
			fmt.Println("Error: PUT requires key and value")
			return
		}
		key := parts[1]
		// value可能包含空格（如表达式），所以需要重新组合
		value := strings.Join(parts[2:], " ")
		idb.Put(key, value)

	case "GET":
		if len(parts) != 2 {
			fmt.Println("Error: GET requires exactly one key")
			return
		}
		key := parts[1]
		idb.Get(key)

	case "DEL":
		if len(parts) != 2 {
			fmt.Println("Error: DEL requires exactly one key")
			return
		}
		key := parts[1]
		idb.Delete(key)

	case "HELP":
		idb.ShowHelp()

	case "EXIT", "QUIT":
		fmt.Println("Goodbye!")
		idb.Close()
		os.Exit(0)

	case "SHOW":
		idb.ShowAll()

	case "EXECUTE":
		if len(parts) < 3 {
			fmt.Println("Error: EXECUTE command format: EXECUTE NumProcess OutputFile [InputFile ExecNum ExecTime]...")
			return
		}
		idb.ExecuteMultiProcess(parts[1:])

	case "OPEN":
		idb.OpenSystem()

	case "CLOSE":
		idb.CloseSystem()

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		fmt.Println("Type 'HELP' for available commands")
	}
}

// ShowHelp 显示帮助信息
func (idb *InteractiveDB) ShowHelp() {
	fmt.Println(`
Available commands:
  BEGIN                 - Start a new transaction
  COMMIT                - Commit the current transaction
  ABORT                 - Abort the current transaction
  PUT <key> <value>     - Store or update a key-value pair
                         Value can be integer or expression like (key+1)
  GET <key>            - Retrieve value for a key
  DEL <key>            - Delete a key-value pair
  SHOW                 - Show all stored data
  EXECUTE NumProcess OutputFile [InputFile ExecNum ExecTime]...
                       - Execute multiple processes for testing
  OPEN                 - Load existing database file and start system
  CLOSE                - Save database file to disk and close system
  HELP                 - Show this help message
  EXIT/QUIT            - Exit the program

Command Line Options:
  ./interactive-db                    - Run in interactive mode
  ./interactive-db testfile           - Run test file (legacy mode)
  ./interactive-db --batch testfile   - Run test file in batch mode
  ./interactive-db -b testfile        - Run test file in batch mode (short)
  ./interactive-db --command "cmd"    - Execute single command and exit
  ./interactive-db -c "cmd"           - Execute single command and exit (short)

Transaction Example:
  BEGIN
  PUT A 1
  PUT B 2
  COMMIT

Expression Example:
  PUT A 3
  PUT B (A+1)

EXECUTE Command Parameters:
  NumProcess           - Number of processes to start
  OutputFile           - File to store combined output
  InputFile            - Input file for each process
  ExecNum              - Number of executions (-1 for unlimited)
  ExecTime             - Execution time in seconds (-1 for unlimited)

Note: For each process, exactly one of ExecNum or ExecTime must be -1.

EXECUTE Example:
  EXECUTE 2 output.txt test1.txt 5 -1 test2.txt -1 10

Command Line Examples:
  ./interactive-db --batch test_case1.txt
  ./interactive-db -c "PUT A 1"
  ./interactive-db -c "GET A"
`)
}

// ShowAll 显示所有存储的数据
func (idb *InteractiveDB) ShowAll() {
	var count int
	err := idb.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		fmt.Println("Current database contents:")
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("  %s: %s\n", string(k), string(v))
			count++
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error reading database: %v\n", err)
		return
	}

	if count == 0 {
		fmt.Println("Database is empty")
	}

	// 如果在事务中，显示事务状态
	if idb.inTransaction {
		fmt.Printf("Transaction status: %d operations pending\n", len(idb.transactionOps))
	}
}

// RunInteractive 运行交互式终端
func (idb *InteractiveDB) RunInteractive() {
	fmt.Println("Interactive Database Terminal (BoltDB) with Transactions")
	fmt.Println("Type 'HELP' for available commands, 'EXIT' to quit")
	fmt.Println(strings.Repeat("-", 55))

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("db> ")
		if !scanner.Scan() {
			break
		}
		command := scanner.Text()
		if strings.TrimSpace(command) != "" {
			idb.ExecuteCommand(command)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
	fmt.Println("\nGoodbye!")
}

// RunTestCase 运行测试用例文件
func (idb *InteractiveDB) RunTestCase(testFile string) {
	file, err := os.Open(testFile)
	if err != nil {
		fmt.Printf("Test file %s not found: %v\n", testFile, err)
		return
	}
	defer file.Close()

	fmt.Printf("Running test case from %s\n", testFile)
	fmt.Println(strings.Repeat("-", 50))

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command != "" && !strings.HasPrefix(command, "#") {
			fmt.Printf("db> %s\n", command)
			idb.ExecuteCommand(command)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading test file: %v\n", err)
	}

	// 确保在处理完测试文件后程序退出
	fmt.Println("Test case completed.")
}

// OpenSystem 打开数据库系统
func (idb *InteractiveDB) OpenSystem() {
	if idb.systemOpened {
		fmt.Println("OPEN -> Database already opened")
		return
	}

	// 检查数据库文件是否存在
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		fmt.Println("OPEN -> No existing database file found, will create new one")
	} else {
		fmt.Println("OPEN -> Loading existing database file")
	}

	idb.systemOpened = true
	fmt.Println("OPEN -> Database system started")
}

// CloseSystem 关闭数据库系统
func (idb *InteractiveDB) CloseSystem() {
	if !idb.systemOpened {
		fmt.Println("CLOSE -> Database not opened")
		return
	}

	// 检查数据库文件是否存在
	if _, err := os.Stat(dbFile); err == nil {
		fmt.Println("CLOSE -> Database file saved to disk")
	} else {
		fmt.Println("CLOSE -> No database file to save")
	}

	idb.systemOpened = false
	fmt.Println("CLOSE -> Database system closed")
}

// runProcessWithLimit 运行进程（有次数限制）
func (idb *InteractiveDB) runProcessWithLimit(inputFile string, execNum int, outputFile string, processID int, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < execNum; i++ {
		cmd := exec.Command("./interactive-db", "--batch", inputFile)
		output, err := cmd.CombinedOutput()

		if err != nil {
			fmt.Printf("Process %d execution %d failed: %v\n", processID, i+1, err)
			continue
		}

		// 写入输出文件
		idb.writeOutput(outputFile, fmt.Sprintf("=== Process %d Execution %d ===\n%s\n", processID, i+1, string(output)))
	}

	fmt.Printf("Process %d completed %d executions\n", processID, execNum)
}

// runProcessWithTime 运行进程（有时间限制）
func (idb *InteractiveDB) runProcessWithTime(inputFile string, execTime int, outputFile string, processID int, wg *sync.WaitGroup) {
	defer wg.Done()

	startTime := time.Now()
	endTime := startTime.Add(time.Duration(execTime) * time.Second)
	execCount := 0

	for time.Now().Before(endTime) {
		cmd := exec.Command("./interactive-db", "--batch", inputFile)
		output, err := cmd.CombinedOutput()

		if err != nil {
			fmt.Printf("Process %d execution %d failed: %v\n", processID, execCount+1, err)
			continue
		}

		execCount++
		// 写入输出文件
		idb.writeOutput(outputFile, fmt.Sprintf("=== Process %d Execution %d ===\n%s\n", processID, execCount, string(output)))

		// 检查是否超时
		if time.Now().After(endTime) {
			break
		}
	}

	fmt.Printf("Process %d completed %d executions in %d seconds\n", processID, execCount, execTime)
}

// writeOutput 线程安全地写入输出文件
func (idb *InteractiveDB) writeOutput(outputFile, content string) {
	file, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening output file %s: %v\n", outputFile, err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		fmt.Printf("Error writing to output file %s: %v\n", outputFile, err)
	}
}

// ExecuteMultiProcess 执行多进程测试
func (idb *InteractiveDB) ExecuteMultiProcess(args []string) {
	if len(args) < 2 {
		fmt.Println("Error: EXECUTE requires at least NumProcess and OutputFile")
		return
	}

	// 解析参数
	numProcess, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Printf("Error: Invalid NumProcess: %s\n", args[0])
		return
	}

	outputFile := args[1]

	// 检查参数组数
	remainingArgs := args[2:]
	if len(remainingArgs)%3 != 0 {
		fmt.Println("Error: Each process must have InputFile, ExecNum, and ExecTime")
		return
	}

	expectedGroups := len(remainingArgs) / 3
	if expectedGroups != numProcess {
		fmt.Printf("Error: Expected %d process groups, but got %d\n", numProcess, expectedGroups)
		return
	}

	// 清空输出文件
	os.Remove(outputFile)

	// 解析进程配置
	var configs []ProcessConfig
	for i := 0; i < numProcess; i++ {
		baseIndex := i * 3
		inputFile := remainingArgs[baseIndex]
		execNum, err1 := strconv.Atoi(remainingArgs[baseIndex+1])
		execTime, err2 := strconv.Atoi(remainingArgs[baseIndex+2])

		if err1 != nil || err2 != nil {
			fmt.Printf("Error: Invalid ExecNum or ExecTime for process %d\n", i+1)
			return
		}

		// 验证ExecNum和ExecTime只有一个不为-1
		if (execNum == -1 && execTime == -1) || (execNum != -1 && execTime != -1) {
			fmt.Printf("Error: Exactly one of ExecNum or ExecTime must be -1 for process %d\n", i+1)
			return
		}

		configs = append(configs, ProcessConfig{
			InputFile: inputFile,
			ExecNum:   execNum,
			ExecTime:  execTime,
		})
	}

	fmt.Printf("EXECUTE -> Starting %d processes\n", numProcess)
	fmt.Printf("Output will be saved to: %s\n", outputFile)

	// 启动所有进程
	var wg sync.WaitGroup
	startTime := time.Now()

	for i, config := range configs {
		wg.Add(1)
		processID := i + 1

		if config.ExecNum != -1 {
			// 按次数执行
			fmt.Printf("Starting process %d: %s (executions: %d)\n", processID, config.InputFile, config.ExecNum)
			go idb.runProcessWithLimit(config.InputFile, config.ExecNum, outputFile, processID, &wg)
		} else {
			// 按时间执行
			fmt.Printf("Starting process %d: %s (time: %d seconds)\n", processID, config.InputFile, config.ExecTime)
			go idb.runProcessWithTime(config.InputFile, config.ExecTime, outputFile, processID, &wg)
		}
	}

	// 等待所有进程完成
	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("EXECUTE -> All %d processes completed in %v\n", numProcess, elapsed)
	fmt.Printf("Results saved to: %s\n", outputFile)
}

func main() {
	idb, err := NewInteractiveDB()
	if err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		os.Exit(1)
	}
	defer idb.Close()

	// 检查命令行参数
	if len(os.Args) > 1 {
		// 检查是否是批处理模式
		if len(os.Args) >= 3 && (os.Args[1] == "--batch" || os.Args[1] == "-b") {
			// 批处理模式：--batch testfile
			testFile := os.Args[2]
			idb.RunTestCase(testFile)
			return
		} else if len(os.Args) >= 3 && (os.Args[1] == "--command" || os.Args[1] == "-c") {
			// 单命令模式：--command "PUT A 1"
			command := os.Args[2]
			fmt.Printf("db> %s\n", command)
			idb.ExecuteCommand(command)
			return
		} else {
			// 兼容模式：直接指定测试文件
			testFile := os.Args[1]
			idb.RunTestCase(testFile)
			return
		}
	} else {
		// 运行交互式模式
		idb.RunInteractive()
	}
}
