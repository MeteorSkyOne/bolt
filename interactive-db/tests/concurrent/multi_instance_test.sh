#!/bin/bash

# 多实例测试脚本 - 分布式模式
echo "========================================="
echo "Interactive Database Multi-Instance Test"
echo "Distributed Mode with CN + Server Nodes"
echo "========================================="

# 查找可用端口的函数
find_available_port() {
    local start_port=$1
    local port=$start_port
    while true; do
        if ! lsof -i :$port >/dev/null 2>&1; then
            echo $port
            return
        fi
        port=$((port + 1))
        if [ $port -gt $((start_port + 1000)) ]; then
            echo "ERROR: No available port found in range $start_port-$((start_port + 1000))" >&2
            exit 1
        fi
    done
}

# 查找CN和服务器端口
CN_PORT=$(find_available_port 9090)
SERVER_PORT1=$(find_available_port 8080)
SERVER_PORT2=$(find_available_port 8081)
SERVER_PORT3=$(find_available_port 8082)

echo "Using CN port: $CN_PORT"
echo "Using server ports: $SERVER_PORT1, $SERVER_PORT2, $SERVER_PORT3"

# 清理之前的数据库文件
rm -f interactive.db node1.db node2.db node3.db

# 检查是否有进程已经运行
if pgrep -f "cmd/coordinator" > /dev/null || pgrep -f "cmd/server" > /dev/null; then
    echo "Stopping existing processes..."
    pkill -f "cmd/coordinator"
    pkill -f "cmd/server"
    sleep 2
fi

# 构建可执行文件
echo "Building distributed components..."
make build-cn build-server build-client

# 启动CN节点
echo "Starting CN (Coordinator Node) on port $CN_PORT..."
./bin/cn -port $CN_PORT &
CN_PID=$!
echo "CN PID: $CN_PID"

# 等待CN启动
echo "Waiting for CN to start..."
sleep 3

# 检查CN是否成功启动
if ! kill -0 $CN_PID 2>/dev/null; then
    echo "ERROR: CN failed to start!"
    exit 1
fi

# 启动3个服务器节点
echo "Starting server node 1 on port $SERVER_PORT1..."
./bin/server -port $SERVER_PORT1 -id node1 -cn "127.0.0.1:9091" -db node1.db &
SERVER1_PID=$!

echo "Starting server node 2 on port $SERVER_PORT2..."
./bin/server -port $SERVER_PORT2 -id node2 -cn "127.0.0.1:9091" -db node2.db &
SERVER2_PID=$!

echo "Starting server node 3 on port $SERVER_PORT3..."
./bin/server -port $SERVER_PORT3 -id node3 -cn "127.0.0.1:9091" -db node3.db &
SERVER3_PID=$!

# 等待服务器节点启动并注册
echo "Waiting for server nodes to start and register with CN..."
sleep 5

# 检查所有节点是否成功启动
failed=false
if ! kill -0 $SERVER1_PID 2>/dev/null; then
    echo "ERROR: Server node 1 failed to start!"
    failed=true
fi
if ! kill -0 $SERVER2_PID 2>/dev/null; then
    echo "ERROR: Server node 2 failed to start!"
    failed=true
fi
if ! kill -0 $SERVER3_PID 2>/dev/null; then
    echo "ERROR: Server node 3 failed to start!"
    failed=true
fi

if [ "$failed" = true ]; then
    echo "Stopping all processes..."
    kill $CN_PID $SERVER1_PID $SERVER2_PID $SERVER3_PID 2>/dev/null
    exit 1
fi

echo "All nodes started successfully!"
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

# 启动客户端1（后台运行）- 连接到CN
echo "=== Client 1 Output ===" > test_output/client1.log
./bin/client -host localhost -port $CN_PORT -batch tests/concurrent/test_multi_client1.txt >> test_output/client1.log 2>&1 &
CLIENT1_PID=$!

# 启动客户端2（后台运行）- 连接到CN
echo "=== Client 2 Output ===" > test_output/client2.log
./bin/client -host localhost -port $CN_PORT -batch tests/concurrent/test_multi_client2.txt >> test_output/client2.log 2>&1 &
CLIENT2_PID=$!

# 启动客户端3（后台运行）- 连接到CN
echo "=== Client 3 Output ===" > test_output/client3.log
./bin/client -host localhost -port $CN_PORT -batch tests/concurrent/test_multi_client3.txt >> test_output/client3.log 2>&1 &
CLIENT3_PID=$!

# 稍微延迟启动客户端4，让前面的操作有时间完成
sleep 2

# 启动客户端4（后台运行）- 连接到CN
echo "=== Client 4 Output ===" > test_output/client4.log
./bin/client -host localhost -port $CN_PORT -batch tests/concurrent/test_multi_client4.txt >> test_output/client4.log 2>&1 &
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
./bin/client -host localhost -port $CN_PORT -command "SHOW" 2>/dev/null

# 关闭所有节点
echo ""
echo "Stopping all nodes..."
kill $CN_PID $SERVER1_PID $SERVER2_PID $SERVER3_PID 2>/dev/null
wait $CN_PID $SERVER1_PID $SERVER2_PID $SERVER3_PID 2>/dev/null

echo ""
echo "========================================="
echo "Distributed multi-instance test completed!"
echo "Check test_output/ directory for detailed logs"
echo "=========================================" 