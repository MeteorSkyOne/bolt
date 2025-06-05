# 分布式交互式数据库

基于BoltDB的分布式键值数据库，支持事务、主从复制和故障转移。

## 架构设计

```
Client ─┐
Client ─┼─> CN (Coordinator Node) ─┐─> Primary Server (主节点)
Client ─┘                          ├─> Replica Server (从节点1)  
                                   └─> Replica Server (从节点2)
```

## 组件说明

### 1. Coordinator Node (CN)
- 协调节点，负责：
  - 接收服务器节点注册
  - 管理节点状态和角色
  - 接收客户端请求并转发到主节点
  - 主节点选举和故障转移
  - 同步写操作到从节点

### 2. Server Node
- 数据存储节点，负责：
  - 注册到协调节点
  - 处理数据读写操作
  - 维护LSN (Log Sequence Number)
  - 支持事务操作

### 3. Client
- 客户端程序，连接到协调节点进行数据操作

## 快速开始

### 1. 编译所有组件
```bash
make all
```

### 2. 启动3节点测试集群
```bash
make test-cluster
```

### 3. 连接客户端
```bash
make run-client
```

## 手动启动

### 1. 启动协调节点
```bash
make run-cn
# 或者
./bin/coordinator -port=9090
```

### 2. 启动服务器节点 (至少3个)
```bash
# 终端1
./bin/server -port=8081 -id=server_1

# 终端2  
./bin/server -port=8082 -id=server_2

# 终端3
./bin/server -port=8083 -id=server_3
```

### 3. 启动客户端
```bash
./bin/client -addr=localhost:9090
```

## 支持的命令

### 基本操作
- `PUT <key> <value>` - 存储键值对
- `GET <key>` - 获取键对应的值
- `DEL <key>` - 删除键值对
- `SHOW` - 显示所有数据

### 事务操作
- `BEGIN` - 开始事务
- `COMMIT` - 提交事务
- `ABORT` - 回滚事务

### 表达式支持
- `PUT A 5` - 存储A=5
- `PUT B (A+2)` - 存储B=A+2=7
- `PUT C (B-1)` - 存储C=B-1=6

### 其他命令
- `HELP` - 显示帮助信息
- `EXIT/QUIT` - 退出

## 示例操作

```
> PUT A 10
PUT A 10 -> stored
> PUT B (A+5)  
PUT B (A+5) -> stored
> GET B
GET B -> 15
> BEGIN
BEGIN -> transaction started
> PUT A 20
PUT A 20 -> staged in transaction
> PUT C (A+B)
PUT C (A+B) -> staged in transaction
> COMMIT
COMMIT -> transaction committed
> SHOW
Current database contents:
  A: 20
  B: 15
  C: 35
```

## 特性说明

### 1. 分布式特性
- **主从复制**: 写操作在主节点执行后同步到所有从节点
- **故障转移**: 主节点故障时自动选举新的主节点
- **一致性**: 使用LSN确保操作顺序一致

### 2. 事务支持
- **隔离性**: 使用BoltDB原生事务提供ACID特性
- **表达式**: 支持基于现有值的计算表达式

### 3. 高可用性
- **心跳检测**: 定期检测节点健康状态
- **自动重连**: 连接断开时自动重试
- **优雅降级**: 协调节点不可用时服务器可独立运行

## 配置参数

### Coordinator Node
- `-port`: 客户端连接端口 (默认: 9090)

### Server Node  
- `-port`: 服务端口 (默认: 8080)
- `-db`: 数据库文件路径 (默认: interactive.db)
- `-id`: 节点ID (默认: 自动生成)
- `-cn`: 协调节点地址 (默认: localhost:9091)
- `-standalone`: 独立模式，不注册到协调节点

### Client
- `-addr`: 协调节点地址 (默认: localhost:9090)

## 故障处理

### 主节点故障
1. 协调节点检测到主节点心跳超时
2. 从活跃的从节点中选择LSN最高的作为新主节点
3. 通知所有节点角色变更

### 协调节点故障
- 服务器节点可以继续在独立模式下运行
- 客户端需要直接连接到服务器节点

### 从节点故障
- 不影响系统整体运行
- 节点恢复后可重新注册并同步数据

## 清理

```bash
# 停止所有进程
make stop

# 清理编译文件和数据库
make clean
```

## 开发说明

- 基于Go语言开发
- 使用BoltDB作为存储引擎
- 采用TCP协议进行节点间通信
- 支持JSON格式的配置和通信协议 