#!/bin/bash

# kvdb 分布式系统启动脚本
echo "=========================================="
echo "启动 kvdb 分布式系统"
echo "=========================================="

# 设置默认配置
DEFAULT_CN_PORT=9090
DEFAULT_CN_GRPC_PORT=9091
DEFAULT_SERVER1_PORT=18080
DEFAULT_SERVER2_PORT=28080
DEFAULT_SERVER3_PORT=38080

# 创建必要目录
mkdir -p logs

# 确保二进制文件存在
if [ ! -f "/app/bin/cn" ]; then
    echo "错误: 协调节点二进制文件不存在 /app/bin/cn"
    exit 1
fi

if [ ! -f "/app/bin/server" ]; then
    echo "错误: 服务器二进制文件不存在 /app/bin/server"
    exit 1
fi

# 创建停止函数
cleanup() {
    echo ""
    echo "正在停止所有服务..."
    # 杀死所有子进程
    jobs -p | xargs -r kill 2>/dev/null || true
    # 等待进程结束
    sleep 2
    echo "所有服务已停止"
    exit 0
}

# 捕获信号
trap cleanup SIGINT SIGTERM

echo "启动协调节点..."
/app/bin/cn -port $DEFAULT_CN_PORT -grpc-port $DEFAULT_CN_GRPC_PORT &
CN_PID=$!
echo "协调节点 PID: $CN_PID"

# 等待协调节点启动
sleep 5

echo "启动服务器节点..."
/app/bin/server -port $DEFAULT_SERVER1_PORT -id 1 -cn localhost:$DEFAULT_CN_GRPC_PORT -db server1.db &
SERVER1_PID=$!
sleep 1
/app/bin/server -port $DEFAULT_SERVER2_PORT -id 2 -cn localhost:$DEFAULT_CN_GRPC_PORT -db server2.db &
SERVER2_PID=$!
sleep 1
/app/bin/server -port $DEFAULT_SERVER3_PORT -id 3 -cn localhost:$DEFAULT_CN_GRPC_PORT -db server3.db &
SERVER3_PID=$!

echo "服务器节点1 PID: $SERVER1_PID"
echo "服务器节点2 PID: $SERVER2_PID"
echo "服务器节点3 PID: $SERVER3_PID"

# 等待服务器启动
sleep 5

echo ""
echo "============================================"
echo "kvdb 分布式系统已启动！"
echo "============================================"
echo "协调节点: localhost:$DEFAULT_CN_PORT"
echo "服务节点: localhost:$DEFAULT_SERVER1_PORT (server_1)"
echo "服务节点: localhost:$DEFAULT_SERVER2_PORT (server_2)"
echo "服务节点: localhost:$DEFAULT_SERVER3_PORT (server_3)"
echo ""
echo "日志位置:"
echo "  协调节点日志: logs/coordinator.log"
echo "  服务节点1日志: logs/server1.log"
echo "  服务节点2日志: logs/server2.log"
echo "  服务节点3日志: logs/server3.log"
echo ""
echo "使用客户端连接:"
echo "  /app/bin/client"
echo ""
echo "按 Ctrl+C 停止所有服务"
echo "============================================"

# 监控所有进程，如果任何一个退出就停止所有服务
while true; do
    # 检查协调节点是否还在运行
    if ! kill -0 $CN_PID 2>/dev/null; then
        echo "协调节点已停止，正在停止所有服务..."
        cleanup
    fi
    
    # 检查服务器节点是否还在运行
    if ! kill -0 $SERVER1_PID 2>/dev/null; then
        echo "服务器节点1已停止，正在停止所有服务..."
        cleanup
    fi
    
    if ! kill -0 $SERVER2_PID 2>/dev/null; then
        echo "服务器节点2已停止，正在停止所有服务..."
        cleanup
    fi
    
    if ! kill -0 $SERVER3_PID 2>/dev/null; then
        echo "服务器节点3已停止，正在停止所有服务..."
        cleanup
    fi
    
    sleep 5
done