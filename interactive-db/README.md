# Interactive Database Terminal (BoltDB) with Transactions

一个用Go语言编写的交互式数据库终端程序，使用BoltDB作为底层存储，支持增删改查操作、数据持久化、手动事务控制和标准化测试。

## 功能特性

- **PUT**: 存储或更新键值对，支持整数值和表达式
- **GET**: 查询键对应的值
- **DEL**: 删除键值对
- **BEGIN**: 开始一个新事务
- **COMMIT**: 提交当前事务
- **ABORT**: 中止当前事务
- **EXECUTE**: 执行多进程标准化测试
- **OPEN**: 加载现有数据文件并启动系统
- **CLOSE**: 在磁盘上保存数据文件并关闭系统
- **HELP**: 显示帮助信息
- **SHOW**: 显示所有存储的数据
- **EXIT/QUIT**: 退出程序
- **数据持久化**: 所有数据都存储在BoltDB数据库文件中，程序重启后数据不会丢失
- **并发支持**: 支持多个进程同时访问数据库，BoltDB提供ACID事务保证
- **手动事务**: 支持BEGIN/COMMIT/ABORT手动控制事务边界
- **标准化测试**: 支持EXECUTE命令进行多进程并发测试

## 底层存储

本程序使用BoltDB作为底层存储引擎：
- 数据文件：`interactive.db`
- 使用bucket：`kv_store`
- 支持ACID事务
- 数据自动持久化到磁盘

## 并发特性

BoltDB提供了优秀的并发支持：
- **多读者**: 支持多个读事务并发执行
- **单写者**: 写事务是互斥的，但可以与读事务并发
- **文件锁**: 使用文件锁确保数据库完整性
- **事务隔离**: 每个事务都有独立的数据视图

## 事务支持

### 手动事务控制

程序支持手动事务控制，允许用户精确控制事务边界：

- **BEGIN**: 开始一个新事务
- **COMMIT**: 提交事务，将所有操作写入数据库
- **ABORT**: 中止事务，丢弃所有未提交的操作

### 事务特性

- **事务缓存**: 在事务中的操作会被缓存，直到COMMIT才执行
- **读一致性**: 在事务中可以读取到同一事务内的修改
- **原子性**: 事务内的所有操作要么全部成功，要么全部回滚
- **隔离性**: 每个事务看到一致的数据视图

### 事务示例

```
db> BEGIN
BEGIN -> transaction started
db> PUT A 1
PUT A 1 -> staged in transaction
db> PUT B (A+1)
PUT B (A+1) -> staged in transaction
db> GET A
GET A -> 1
db> GET B
GET B -> 2
db> COMMIT
COMMIT -> transaction committed
```

## 表达式支持

PUT命令支持两种值类型：
1. 整数值：`PUT A 3`
2. 表达式：`PUT A (B+1)` 或 `PUT A (B-2)`

表达式格式：`(variable +/- constant)`

## 编译和运行

### 编译程序
```bash
cd interactive-db
go build -o interactive-db main.go
```

### 运行模式

#### 1. 交互式模式
```bash
./interactive-db
```

#### 2. 批处理模式
```bash
# 使用批处理模式执行测试文件
./interactive-db --batch test_case1.txt
./interactive-db -b test_case1.txt

# 兼容模式（保持向后兼容）
./interactive-db test_case1.txt
```

#### 3. 单命令模式
```bash
# 执行单个命令后退出
./interactive-db --command "PUT A 1"
./interactive-db -c "GET A"
./interactive-db -c "EXECUTE 2 output.txt test1.txt 1 -1 test2.txt 1 -1"
```

### 命令行选项

| 选项 | 说明 | 示例 |
|-----|------|------|
| 无参数 | 交互式模式 | `./interactive-db` |
| `testfile` | 兼容模式，执行测试文件 | `./interactive-db test.txt` |
| `--batch testfile` | 批处理模式，执行后退出 | `./interactive-db --batch test.txt` |
| `-b testfile` | 批处理模式（短选项） | `./interactive-db -b test.txt` |
| `--command "cmd"` | 单命令模式 | `./interactive-db --command "PUT A 1"` |
| `-c "cmd"` | 单命令模式（短选项） | `./interactive-db -c "GET A"` |

### 并发测试
```bash
# 运行基本并发测试
./test_concurrent.sh

# 运行并发能力演示
./demo_concurrent.sh

# 运行事务测试
./test_case4.sh

# 运行读写混合并发测试
./test_case5.sh

# 运行新功能测试
./test_fix.sh

# 运行新功能演示
./demo_new_features.sh
```

## 使用示例

```
Interactive Database Terminal (BoltDB) with Transactions
Type 'HELP' for available commands, 'EXIT' to quit
-------------------------------------------------------
db> PUT A 3
PUT A 3 -> stored
db> PUT B 4
PUT B 4 -> stored
db> BEGIN
BEGIN -> transaction started
db> PUT A (A+1)
PUT A (A+1) -> staged in transaction
db> GET A
GET A -> 4
db> COMMIT
COMMIT -> transaction committed
db> GET A
GET A -> 4
db> BEGIN
BEGIN -> transaction started
db> PUT A (A-1)
PUT A (A-1) -> staged in transaction
db> ABORT
ABORT -> transaction aborted
db> GET A
GET A -> 4
```

## 持久化验证

数据会自动保存到`interactive.db`文件中，可以通过以下方式验证：

```bash
# 第一次运行，存储一些数据
echo "PUT X 100" | ./interactive-db

# 第二次运行，验证数据仍然存在
echo "GET X" | ./interactive-db
# 输出: GET X -> 100
```

## 测试用例

### 测试用例1：基础功能测试
```
PUT A 3
PUT B 4
PUT A (A+1)
PUT B (B+1)
GET A
GET B
DEL A
DEL B
PUT A 5
GET A
GET B
PUT B 5
GET B
```

### 测试用例4：手动事务测试

**初始化** (`test_case4-1.txt`)：
```
PUT A 0
PUT B 0
```

**事务操作** (`test_case4-2.txt`)：
```
BEGIN
PUT A (A+1)
PUT B (B+1)
PUT A (A+1)
PUT B (B+1)
COMMIT
BEGIN
GET A
GET B
COMMIT
BEGIN
GET A
GET B
COMMIT
BEGIN
PUT A (A-1)
PUT B (B-1)
ABORT
```

**查询结果** (`test_case4-3.txt`)：
```
GET A
GET B
```

### 测试用例5：读写混合并发测试

启动10个进程（2个写进程 + 8个读进程）进行混合并发测试：

**写进程** (`test_case5-2.txt`)：
```
BEGIN
PUT A (A+1)
PUT B (B+1)
PUT A (B+1)
PUT B (A+1)
COMMIT
```

**读进程** (`test_case5-3.txt`)：
```
BEGIN
GET A
GET B
GET A
GET B
COMMIT
```

## 并发测试用例

### 测试用例3：多进程并发测试

同时启动3个进程，每个进程循环执行操作10次：

**进程1** (`test_case3-1.txt`)：
```
PUT A 1
PUT B 1
PUT A (A+1)
PUT B (B+1)
GET A
DEL A
GET B
DEL B
# ... 重复10次
```

**进程2** (`test_case3-2.txt`)：
```
PUT C 1
PUT D 1
PUT C (C+1)
PUT D (D+1)
GET C
DEL C
GET D
DEL D
# ... 重复10次
```

**进程3** (`test_case3-3.txt`)：
```
PUT E 1
PUT F 1
PUT E (E+1)
PUT F (F+1)
GET E
DEL E
GET F
DEL F
# ... 重复10次
```

### 运行并发测试

```bash
# 运行完整的并发测试
./test_concurrent.sh

# 预期结果：所有进程正常完成，无错误
# 数据库最终状态为空（所有键都被删除）
```

## 依赖关系

- 使用本地BoltDB模块 (`github.com/meteorsky/bolt`)
- Go 1.19+

## 文件结构

```
interactive-db/
├── main.go                      # 主程序文件
├── go.mod                       # Go模块文件
├── README.md                    # 说明文档
├── test_case1.txt               # 基础测试用例
├── test_case3-1.txt             # 并发测试用例1
├── test_case3-2.txt             # 并发测试用例2
├── test_case3-3.txt             # 并发测试用例3
├── test_case4-1.txt             # 事务测试初始化
├── test_case4-2.txt             # 事务测试操作
├── test_case4-3.txt             # 事务测试查询
├── test_case5-1.txt             # 混合并发测试初始化
├── test_case5-2.txt             # 混合并发写操作
├── test_case5-3.txt             # 混合并发读操作
├── test_case5-4.txt             # 混合并发查询结果
├── test_interactive.txt         # 交互测试用例
├── test_persistence.txt         # 持久化测试用例
├── test_transaction_basic.txt   # 基础事务测试
├── test_concurrent.sh           # 并发测试脚本
├── demo_concurrent.sh           # 并发演示脚本
├── test_conflict_concurrent.sh  # 冲突测试脚本
├── stress_test.sh               # 压力测试脚本
├── test_case4.sh                # 事务测试脚本
├── test_case5.sh                # 读写混合测试脚本
└── interactive.db               # BoltDB数据库文件（运行时生成）
```

## 并发性能说明

BoltDB的并发模型确保了：
1. **数据一致性**: 所有事务都是ACID兼容的
2. **无死锁**: 简单的锁策略避免了复杂的死锁情况
3. **高读性能**: 多个读操作可以并发执行
4. **写操作序列化**: 写操作按顺序执行，确保数据完整性

### 事务性能特点

- **批量操作**: 事务内的操作批量提交，提高性能
- **读写分离**: 读事务不会阻塞写事务（短时间内）
- **原子性保证**: 事务要么全部成功，要么全部失败
- **一致性视图**: 事务内部看到一致的数据快照

这使得我们的交互式数据库终端能够安全、高效地处理多个客户端的并发访问和复杂的事务操作。

## 标准化测试命令

### EXECUTE 命令
用于执行多进程标准化测试的命令。

**格式**：
```
EXECUTE NumProcess OutputFile [InputFile ExecNum ExecTime]...
```

**参数说明**：
- `NumProcess`: 进程数
- `OutputFile`: 存储输出的文件
- 后接 NumProcess 组 `InputFile ExecNum ExecTime`，其中第i组参数表示第i个进程的输入参数
- `InputFile`: 读取的输入文件
- `ExecNum`: 执行次数。默认为-1，表示不限制执行次数，在ExecTime的时间范围内，不断执行进程。与ExecTime只有一个会取非-1的值。
- `ExecTime`: 执行时间（单位为秒）。默认为-1，表示不限制执行时间，只执行ExecNum次进程。

**示例**：
```
EXECUTE 2 output.txt test1.txt 5 -1 test2.txt -1 10
```
这个命令会启动2个进程：
- 进程1：执行 test1.txt 文件 5 次
- 进程2：执行 test2.txt 文件 10 秒钟
- 所有输出保存到 output.txt 文件

### OPEN 命令
加载现有（可持久化在磁盘上）的数据文件，并启动系统。

**格式**：
```
OPEN
```

### CLOSE 命令
在磁盘上保存数据文件，并关闭系统。

**格式**：
```
CLOSE
```

## 标准化测试示例

### 基础测试命令序列
```
db> OPEN
OPEN -> Loading existing database file
OPEN -> Database system started

db> EXECUTE 1 simple_test.txt test_case1.txt 1 -1
EXECUTE -> Starting 1 processes
Output will be saved to: simple_test.txt
Starting process 1: test_case1.txt (executions: 1)
Process 1 completed 1 executions
EXECUTE -> All 1 processes completed in 123ms
Results saved to: simple_test.txt

db> CLOSE
CLOSE -> Database file saved to disk
CLOSE -> Database system closed
```

### 多进程并发测试
```
db> EXECUTE 3 concurrent_test.txt test_case3-1.txt 10 -1 test_case3-2.txt 10 -1 test_case3-3.txt 10 -1
EXECUTE -> Starting 3 processes
Output will be saved to: concurrent_test.txt
Starting process 1: test_case3-1.txt (executions: 10)
Starting process 2: test_case3-2.txt (executions: 10)
Starting process 3: test_case3-3.txt (executions: 10)
Process 1 completed 10 executions
Process 2 completed 10 executions
Process 3 completed 10 executions
EXECUTE -> All 3 processes completed in 2.5s
Results saved to: concurrent_test.txt
```

### 时间限制测试
```
db> EXECUTE 2 time_test.txt test_case5-2.txt -1 5 test_case5-3.txt -1 5
EXECUTE -> Starting 2 processes
Output will be saved to: time_test.txt
Starting process 1: test_case5-2.txt (time: 5 seconds)
Starting process 2: test_case5-3.txt (time: 5 seconds)
Process 1 completed 12 executions in 5 seconds
Process 2 completed 15 executions in 5 seconds
EXECUTE -> All 2 processes completed in 5.1s
Results saved to: time_test.txt
```

### 运行标准化测试脚本

创建测试脚本文件：
```bash
# test_standardized.sh
#!/bin/bash
echo "OPEN" | ./interactive-db
echo "EXECUTE 2 output.txt test1.txt 3 -1 test2.txt -1 5" | ./interactive-db
echo "CLOSE" | ./interactive-db
```

运行测试：
```bash
chmod +x test_standardized.sh
./test_standardized.sh
``` 