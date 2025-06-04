#!/bin/bash

# 并发测试脚本
echo "开始并发测试 - 启动3个进程"
echo "================================================"

# 清理数据库文件以确保测试从干净状态开始
rm -f interactive.db

# 并发启动3个进程
echo "启动进程1..."
./interactive-db test_case3-1.txt > output1.log 2>&1 &
PID1=$!

echo "启动进程2..."
./interactive-db test_case3-2.txt > output2.log 2>&1 &
PID2=$!

echo "启动进程3..."
./interactive-db test_case3-3.txt > output3.log 2>&1 &
PID3=$!

echo "等待所有进程完成..."

# 等待所有进程完成
wait $PID1
EXIT1=$?
echo "进程1完成，退出码: $EXIT1"

wait $PID2
EXIT2=$?
echo "进程2完成，退出码: $EXIT2"

wait $PID3
EXIT3=$?
echo "进程3完成，退出码: $EXIT3"

echo "================================================"
echo "所有进程已完成"

# 显示结果
echo ""
echo "进程1输出 (最后10行):"
echo "------------------------"
tail -n 10 output1.log

echo ""
echo "进程2输出 (最后10行):"
echo "------------------------"
tail -n 10 output2.log

echo ""
echo "进程3输出 (最后10行):"
echo "------------------------"
tail -n 10 output3.log

echo ""
echo "最终数据库状态:"
echo "------------------------"
./interactive-db test_persistence.txt

echo ""
echo "测试完成！查看完整日志："
echo "  cat output1.log  # 进程1完整日志"
echo "  cat output2.log  # 进程2完整日志"  
echo "  cat output3.log  # 进程3完整日志" 