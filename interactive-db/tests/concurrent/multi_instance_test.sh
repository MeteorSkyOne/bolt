#!/bin/bash

# 多实例测试脚本
echo "========================================="
echo "Interactive Database Multi-Instance Test"
echo "========================================="

# 查找可用端口的函数
find_available_port() {
    local port=8080
    while true; do
        if ! lsof -i :$port >/dev/null 2>&1; then
            echo $port
            return
        fi
        port=$((port + 1))
        if [ $port -gt 9000 ]; then
            echo "ERROR: No available port found in range 8080-9000" >&2
            exit 1
        fi
    done
}

# 查找可用端口
AVAILABLE_PORT=$(find_available_port)
echo "Using port: $AVAILABLE_PORT"

# 清理之前的数据库文件
rm -f interactive.db

# 检查服务器是否已经运行
if pgrep -f "cmd/server" > /dev/null; then
    echo "Server is already running. Stopping it first..."
    pkill -f "cmd/server"
    sleep 1
fi

# 启动服务器
echo "Starting database server on port $AVAILABLE_PORT..."
go run ./cmd/server --port $AVAILABLE_PORT &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# 等待服务器启动
echo "Waiting for server to start..."
sleep 3

# 检查服务器是否成功启动
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start!"
    exit 1
fi

echo "Server started successfully!"
echo ""

# 创建输出目录
mkdir -p test_output

# 同时启动多个客户端实例
echo "Starting multiple client instances..."
echo "Client 1: Basic operations"
echo "Client 2: Concurrent write operations"
echo "Client 3: Read operations and transaction conflicts"
echo "Client 4: Delete operations and final check"
echo ""

# 启动客户端1（后台运行）
echo "=== Client 1 Output ===" > test_output/client1.log
go run ./cmd/client --port $AVAILABLE_PORT --batch tests/concurrent/test_multi_client1.txt >> test_output/client1.log 2>&1 &
CLIENT1_PID=$!

# 启动客户端2（后台运行）
echo "=== Client 2 Output ===" > test_output/client2.log
go run ./cmd/client --port $AVAILABLE_PORT --batch tests/concurrent/test_multi_client2.txt >> test_output/client2.log 2>&1 &
CLIENT2_PID=$!

# 启动客户端3（后台运行）  
echo "=== Client 3 Output ===" > test_output/client3.log
go run ./cmd/client --port $AVAILABLE_PORT --batch tests/concurrent/test_multi_client3.txt >> test_output/client3.log 2>&1 &
CLIENT3_PID=$!

# 稍微延迟启动客户端4，让前面的操作有时间完成
sleep 2

# 启动客户端4（后台运行）
echo "=== Client 4 Output ===" > test_output/client4.log
go run ./cmd/client --port $AVAILABLE_PORT --batch tests/concurrent/test_multi_client4.txt >> test_output/client4.log 2>&1 &
CLIENT4_PID=$!

echo "All clients started. Waiting for completion..."

# 等待所有客户端完成
wait $CLIENT1_PID
wait $CLIENT2_PID
wait $CLIENT3_PID
wait $CLIENT4_PID

echo ""
echo "All clients completed!"

# 显示测试结果
echo ""
echo "========================================="
echo "TEST RESULTS"
echo "========================================="

for i in {1..4}; do
    echo ""
    echo "--- Client $i Results ---"
    if [ -f "test_output/client$i.log" ]; then
        cat "test_output/client$i.log"
    else
        echo "No output file found for client $i"
    fi
done

# 最终状态检查
echo ""
echo "--- Final Database State ---"
go run ./cmd/client --port $AVAILABLE_PORT --command "SHOW" 2>/dev/null

# 关闭服务器
echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo ""
echo "========================================="
echo "Multi-instance test completed!"
echo "Check test_output/ directory for detailed logs"
echo "=========================================" 