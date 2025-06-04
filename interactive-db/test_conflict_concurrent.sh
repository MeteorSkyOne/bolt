#!/bin/bash

# 测试多进程对同一键的并发访问
echo "测试并发冲突 - 多个进程操作同一个键"
echo "================================================"

# 清理数据库
rm -f interactive.db

# 初始化共享键
echo "PUT SHARED 0" | ./interactive-db

echo "同时启动3个进程操作同一个键..."

# 同时启动3个进程
./interactive-db test_conflict.txt > conflict1.log 2>&1 &
PID1=$!

./interactive-db test_conflict.txt > conflict2.log 2>&1 &
PID2=$!

./interactive-db test_conflict.txt > conflict3.log 2>&1 &
PID3=$!

# 等待所有进程完成
wait $PID1 $PID2 $PID3

echo "所有进程完成"

echo ""
echo "最终结果："
echo "GET SHARED" | ./interactive-db

echo ""
echo "各进程的最后结果："
echo "进程1: $(tail -n 1 conflict1.log | grep 'GET SHARED')"
echo "进程2: $(tail -n 1 conflict2.log | grep 'GET SHARED')"
echo "进程3: $(tail -n 1 conflict3.log | grep 'GET SHARED')" 