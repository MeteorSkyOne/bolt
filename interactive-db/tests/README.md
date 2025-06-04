# 测试套件说明

这个目录包含了Interactive Database的完整测试套件，按功能分类组织。

## 目录结构

```
tests/
├── basic/                          # 基础功能测试
│   ├── basic_operations.txt        # 基本CRUD操作
│   └── test_sample.txt             # 原始示例测试
├── concurrent/                     # 并发测试
│   ├── multi_instance_test.sh      # 多实例测试脚本
│   ├── multi_process_transaction_test.sh  # 多进程事务正确性测试
│   ├── test_multi_client1.txt      # 多客户端测试1
│   ├── test_multi_client2.txt      # 多客户端测试2
│   ├── test_multi_client3.txt      # 多客户端测试3
│   └── test_multi_client4.txt      # 多客户端测试4
├── transaction/                    # 事务测试
│   └── single_transaction.txt      # 单客户端事务测试
└── execute/                        # 执行脚本
    ├── test_execute.sh             # 基础执行测试
    ├── test_execute_functional.sh  # 功能执行测试
    └── quick_execute_test.sh       # 快速执行测试
```

## 标准化命名规范

### 测试文件命名
- 测试脚本：`test_<功能>_<类型>.sh`
  - 例如：`test_execute_functional.sh`, `test_multi_instance.sh`
- 测试数据：`<功能>_<描述>.txt`
  - 例如：`basic_operations.txt`, `single_transaction.txt`
- 多客户端测试：`test_multi_client<数字>.txt`
  - 例如：`test_multi_client1.txt`, `test_multi_client2.txt`

### 输出目录结构
```
interactive-db/
├── outputs/                        # 统一输出目录
│   ├── logs/                       # 日志文件
│   │   ├── client<数字>.log        # 客户端日志
│   │   ├── server.log              # 服务器日志
│   │   └── test_<类型>.log         # 测试日志
│   ├── results/                    # 测试结果
│   │   ├── <测试名>_result.log     # 测试结果文件
│   │   └── summary.log             # 汇总结果
│   └── temp/                       # 临时文件
│       ├── client<数字>.txt        # 临时客户端文件
│       └── <测试名>_temp.txt       # 临时测试文件
├── test_output/                    # 保留兼容性(已弃用)
└── test_results/                   # 保留兼容性(已弃用)
```

### 文件命名规则
1. **日志文件**: `<组件>_<编号>.log`
   - `client_001.log`, `client_002.log`
   - `server_main.log`, `server_error.log`

2. **结果文件**: `<测试类型>_<场景>_result.log`
   - `concurrent_transfer_result.log`
   - `transaction_isolation_result.log`

3. **临时文件**: `<测试名>_<用途>_temp.txt`
   - `counter_client_temp.txt`
   - `transfer_data_temp.txt`

4. **配置文件**: `<功能>_config.txt`
   - `concurrent_config.txt`
   - `transaction_config.txt`

## 测试分类

### 基础测试 (basic/)
测试数据库的基本功能：
- PUT、GET、DEL操作
- 表达式计算
- SHOW命令
- 错误处理

### 并发测试 (concurrent/)
测试多客户端并发访问：
- 多客户端同时连接
- 并发读写操作
- 数据一致性验证
- 网络连接管理

### 事务测试 (transaction/)
测试事务功能：
- BEGIN、COMMIT、ABORT
- 事务隔离性
- 原子性保证
- 一致性验证

## 运行测试

### 快速测试
```bash
# 基础功能测试
make test  # 运行 tests/basic/test_sample.txt

# 多实例并发测试
make multi-test  # 运行 tests/concurrent/multi_instance_test.sh
```

### 专项测试

#### 1. 基础操作测试
```bash
go run ./cmd/client --batch tests/basic/basic_operations.txt
```

#### 2. 单客户端事务测试
```bash
go run ./cmd/client --batch tests/transaction/single_transaction.txt
```

#### 3. 多进程事务正确性测试
```bash
./tests/concurrent/multi_process_transaction_test.sh
```

#### 4. 多实例并发测试
```bash
./tests/concurrent/multi_instance_test.sh
```

## 测试预期结果

### 基础操作测试
- 所有PUT操作应该成功
- GET操作应该返回正确的值
- 表达式计算应该正确
- DEL操作后GET应该返回"Key not found"

### 事务测试
- 提交的事务应该持久化所有修改
- 回滚的事务应该不改变数据库状态
- 事务内的GET应该能看到未提交的修改

### 并发测试
- 多个客户端应该能同时连接
- 数据一致性应该得到保证
- 总金额在转账操作后应该保持不变
- 计数器并发更新应该得到正确结果

## 测试数据说明

### 银行转账测试
- 初始：账户A=1000, 账户B=1000, 总计=2000
- 操作：A→B(100), B→A(50), A→B(30)
- 预期：总计仍为2000，交易次数=3

### 计数器测试
- 初始：counter=0
- 操作：5个客户端同时+1
- 预期：counter=5

### 隔离性测试
- 事务外的读取不应该看到事务内未提交的修改
- 事务内的读取应该看到本事务的修改

## 故障排除

如果测试失败：
1. 检查服务器是否正常启动
2. 确认端口8080没有被占用
3. 查看详细的测试日志
4. 验证数据库文件权限

测试日志位置：
- 多进程测试：`test_results/` 目录
- 多实例测试：`test_output/` 目录 