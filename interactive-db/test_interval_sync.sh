#!/bin/bash

echo "===== 定时同步策略测试 ====="

# 清理旧进程
echo "清理旧进程..."
pkill -f "bin/cn" || true
pkill -f "bin/server" || true
sleep 2

# 清理旧数据
echo "清理旧数据..."
rm -f interactive*.db
rm -f test_output/*.log
mkdir -p test_output

# 编译
echo "编译程序..."
go build -race -o bin/server cmd/server/server.go
go build -race -o bin/cn cmd/coordinator/cn.go

if [ $? -ne 0 ]; then
    echo "编译失败"
    exit 1
fi

# 启动CN
echo "启动CN..."
./bin/cn > test_output/cn.log 2>&1 &
CN_PID=$!
sleep 2

# 启动第一个server（将成为主节点）
echo "启动server 1..."
./bin/server -id 1 -port 8081 -db interactive1.db > test_output/server1.log 2>&1 &
SERVER1_PID=$!
sleep 2

# 启动第二个server
echo "启动server 2..."
./bin/server -id 2 -port 8082 -db interactive2.db > test_output/server2.log 2>&1 &
SERVER2_PID=$!
sleep 2

# 启动第三个server
echo "启动server 3..."
./bin/server -id 3 -port 8083 -db interactive3.db > test_output/server3.log 2>&1 &
SERVER3_PID=$!
sleep 3

echo "所有组件启动完成，等待系统初始化..."
sleep 5

# 向主节点插入数据来创建LSN差异
echo "向主节点插入测试数据..."
echo "PUT test_key1 test_value1" | nc localhost 8081
sleep 1
echo "PUT test_key2 test_value2" | nc localhost 8081
sleep 1
echo "PUT test_key3 test_value3" | nc localhost 8081
sleep 1

echo "等待定时同步触发（10秒间隔）..."
sleep 12

echo "再次插入数据..."
echo "PUT test_key4 test_value4" | nc localhost 8081
sleep 1

echo "等待下一轮定时同步..."
sleep 12

echo "验证数据同步结果..."
echo "检查server2的数据:"
echo "GET test_key1" | nc localhost 8082
echo "GET test_key4" | nc localhost 8082

echo "检查server3的数据:"
echo "GET test_key1" | nc localhost 8083
echo "GET test_key4" | nc localhost 8083

# 查看日志中的同步信息
echo ""
echo "===== CN日志中的同步信息 ====="
grep -E "(Sync decision|Triggering periodic sync|Sync completed|LSN diff)" test_output/cn.log | tail -20

echo ""
echo "===== 程序运行状态 ====="
echo "CN PID: $CN_PID"
echo "Server1 PID: $SERVER1_PID"  
echo "Server2 PID: $SERVER2_PID"
echo "Server3 PID: $SERVER3_PID"

echo ""
echo "测试完成。日志文件位于 test_output/ 目录"
echo "按任意键清理进程..."
read -n 1

# 清理进程
kill $CN_PID $SERVER1_PID $SERVER2_PID $SERVER3_PID 2>/dev/null
wait 2>/dev/null

echo "进程已清理" 