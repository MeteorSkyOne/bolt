#!/bin/bash

# 清理旧进程
echo "Cleaning up old processes..."
pkill -f "bin/cn" || true
pkill -f "bin/server" || true
sleep 2

# 清理旧数据
echo "Cleaning up old data..."
rm -f interactive*.db
rm -f test_output/*.log
mkdir -p test_output

# 编译
echo "Building binaries..."
go build -o bin/server cmd/server/server.go
go build -o bin/cn cmd/coordinator/cn.go

# 启动CN
echo "Starting CN..."
./bin/cn > test_output/cn.log 2>&1 &
CN_PID=$!
sleep 2

# 启动第一个server（将成为主节点）
echo "Starting server 1..."
./bin/server -id 1 -port 8081 -db interactive1.db > test_output/server1.log 2>&1 &
SERVER1_PID=$!
sleep 2

# 启动第二个server
echo "Starting server 2..."
./bin/server -id 2 -port 8082 -db interactive2.db > test_output/server2.log 2>&1 &
SERVER2_PID=$!
sleep 2

# 启动第三个server（这个应该触发同步）
echo "Starting server 3..."
./bin/server -id 3 -port 8083 -db interactive3.db > test_output/server3.log 2>&1 &
SERVER3_PID=$!

# 等待系统启动
echo "Waiting for system to start..."
sleep 10

# 显示日志
echo "=== CN Log ==="
tail -n 50 test_output/cn.log | grep -E "(ERROR|Starting|Sending|Received|Failed|backup|restore|sync)"

echo ""
echo "=== Server 3 Log ==="
tail -n 30 test_output/server3.log | grep -E "(ERROR|backup|restore|Starting|Waiting|Received|Failed)"

# 清理
echo ""
echo "Press Enter to stop all processes..."
read

kill $CN_PID $SERVER1_PID $SERVER2_PID $SERVER3_PID 2>/dev/null
echo "All processes stopped." 