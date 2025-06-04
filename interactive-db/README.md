# Interactive Database - Client-Server Architecture

这是一个基于BoltDB的交互式数据库系统，现在支持客户端-服务器架构，允许多个客户端同时连接和访问数据库。

## 功能特性

- **客户端-服务器架构**：支持多个客户端同时连接
- **事务支持**：每个客户端独立的事务状态
- **表达式计算**：支持基于现有值的计算操作
- **并发安全**：线程安全的数据库操作
- **多种模式**：交互式、批处理、单命令模式

## 快速开始

### 1. 启动服务器

```bash
cd interactive-db
go run ./cmd/server
```

服务器将在端口8080上启动，等待客户端连接。

### 2. 启动客户端

#### 交互式模式
```bash
go run ./cmd/client
```

#### 批处理模式
```bash
go run ./cmd/client --batch test_file.txt
# 或者简写
go run ./cmd/client -b test_file.txt
```

#### 单命令模式
```bash
go run ./cmd/client --command "PUT A 1"
# 或者简写
go run ./cmd/client -c "GET A"
```

#### 兼容模式（直接指定测试文件）
```bash
go run ./cmd/client test_file.txt
```

## 编译可执行文件

```bash
# 编译服务器
go build -o server ./cmd/server

# 编译客户端  
go build -o client ./cmd/client

# 使用编译后的程序
./server &
./client
```

## 支持的命令

### 基本操作
- `PUT <key> <value>` - 存储或更新键值对
- `GET <key>` - 获取键对应的值
- `DEL <key>` - 删除键值对
- `SHOW` - 显示所有数据

### 事务操作
- `BEGIN` - 开始事务
- `COMMIT` - 提交事务
- `ABORT` - 中止事务

### 其他命令
- `HELP` - 显示帮助信息
- `EXIT/QUIT` - 退出客户端

## 使用示例

### 基本操作示例
```
db> PUT A 5
PUT A 5 -> stored
db> PUT B 10
PUT B 10 -> stored
db> GET A
GET A -> 5
db> SHOW
Current database contents:
  A: 5
  B: 10
```

### 事务示例
```
db> BEGIN
BEGIN -> transaction started
db> PUT A 1
PUT A 1 -> staged in transaction
db> PUT B 2
PUT B 2 -> staged in transaction
db> COMMIT
COMMIT -> transaction committed
```

### 表达式示例
```
db> PUT A 5
PUT A 5 -> stored
db> PUT B (A+3)
PUT B (A+3) -> stored
db> GET B
GET B -> 8
```

## 架构说明

### 服务器端
- 监听TCP端口8080
- 管理共享的BoltDB数据库
- 为每个客户端维护独立的会话状态
- 处理并发访问和事务隔离

### 客户端
- 连接到服务器
- 提供用户交互界面
- 支持多种运行模式
- 自动处理网络通信

### 并发特性
- 多个客户端可以同时连接
- 每个客户端有独立的事务状态
- 服务器端线程安全
- 数据库级别的并发控制

## 测试

### 单客户端测试
```bash
# 使用Make命令
make test

# 或直接运行
go run ./cmd/client --batch test_sample.txt
```

### 多实例并发测试
```bash
# 使用Make命令（推荐）
make multi-test

# 或直接运行脚本
./multi_instance_test.sh
```

多实例测试将会：
1. 自动启动服务器
2. 同时启动4个客户端实例
3. 执行不同的并发操作
4. 测试事务隔离和数据一致性
5. 生成详细的测试报告

### 手动多客户端测试
1. 启动服务器：`go run ./cmd/server`
2. 在不同终端启动多个客户端：`go run ./cmd/client`
3. 在不同客户端中执行操作，观察数据同步

### 事务隔离测试
1. 客户端1：`BEGIN` -> `PUT A 1`
2. 客户端2：`GET A` (应该看不到未提交的值)
3. 客户端1：`COMMIT`
4. 客户端2：`GET A` (现在可以看到提交的值)

## Make命令

```bash
# 编译项目
make build

# 启动服务器
make server

# 启动客户端
make client

# 单客户端测试
make test

# 多实例并发测试
make multi-test

# 完整自动化测试
make full-test

# 清理文件
make clean

# 显示帮助
make help
```

## 技术实现

- **网络协议**：TCP
- **序列化**：文本协议（简单易调试）
- **并发控制**：Go的goroutine和sync包
- **数据库**：BoltDB（嵌入式键值存储）
- **事务管理**：每客户端独立的事务缓存

## 测试文件说明

- `test_sample.txt` - 基础功能测试
- `test_multi_client1.txt` - 客户端1测试（基础操作）
- `test_multi_client2.txt` - 客户端2测试（并发写操作）
- `test_multi_client3.txt` - 客户端3测试（事务冲突）
- `test_multi_client4.txt` - 客户端4测试（删除和检查）
- `multi_instance_test.sh` - 多实例测试脚本

## 注意事项

1. 服务器必须先启动才能运行客户端
2. 服务器关闭会断开所有客户端连接
3. 客户端异常断开不会影响其他客户端
4. 数据库文件`interactive.db`会在服务器启动目录创建
5. 事务只在单个客户端内有效，不支持分布式事务
6. 多实例测试结果保存在`test_output/`目录中