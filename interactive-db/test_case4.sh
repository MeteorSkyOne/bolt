#!/bin/bash

# 测试用例4：事务测试
echo "测试用例4：手动事务测试"
echo "================================================"

# 清理数据库
rm -f interactive.db

# 步骤1：初始化A和B
echo "步骤1：初始化A和B的值"
./interactive-db test_case4-1.txt

echo ""
echo "步骤2：启动3个进程并发执行事务操作..."

# 并发启动3个进程执行事务操作
./interactive-db test_case4-2.txt > tx_output1.log 2>&1 &
PID1=$!

./interactive-db test_case4-2.txt > tx_output2.log 2>&1 &
PID2=$!

./interactive-db test_case4-2.txt > tx_output3.log 2>&1 &
PID3=$!

echo "等待3个进程完成事务操作..."

# 等待所有进程完成
wait $PID1 $PID2 $PID3

echo "所有进程已完成"

echo ""
echo "步骤3：查看最终结果"
./interactive-db test_case4-3.txt

echo ""
echo "各进程的执行日志："
echo ""
echo "进程1最后几行:"
tail -n 5 tx_output1.log

echo ""
echo "进程2最后几行:"
tail -n 5 tx_output2.log

echo ""
echo "进程3最后几行:"
tail -n 5 tx_output3.log

echo ""
echo "测试完成！"
echo "查看完整日志："
echo "  cat tx_output1.log"
echo "  cat tx_output2.log"
echo "  cat tx_output3.log"

# 清理日志文件
rm -f tx_output*.log 