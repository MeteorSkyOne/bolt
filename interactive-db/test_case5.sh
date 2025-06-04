#!/bin/bash

# 测试用例5：读写混合并发测试
echo "测试用例5：读写混合并发测试"
echo "================================================"

# 清理数据库
rm -f interactive.db

# 步骤1：初始化A和B
echo "步骤1：初始化A和B的值"
./interactive-db test_case5-1.txt

echo ""
echo "步骤2：启动10个进程（2个写进程 + 8个读进程）..."

# 启动2个写进程
echo "启动2个写进程..."
./interactive-db test_case5-2.txt > write_output1.log 2>&1 &
WRITE_PID1=$!

./interactive-db test_case5-2.txt > write_output2.log 2>&1 &
WRITE_PID2=$!

# 启动8个读进程
echo "启动8个读进程..."
for i in {1..8}; do
    ./interactive-db test_case5-3.txt > read_output$i.log 2>&1 &
    READ_PIDS[$i]=$!
done

echo "等待所有进程完成..."

# 等待写进程完成
wait $WRITE_PID1 $WRITE_PID2
echo "写进程已完成"

# 等待读进程完成
for i in {1..8}; do
    wait ${READ_PIDS[$i]}
done
echo "读进程已完成"

echo ""
echo "步骤3：查看最终结果"
./interactive-db test_case5-4.txt

echo ""
echo "执行摘要："
echo ""
echo "写进程1最后结果:"
tail -n 3 write_output1.log

echo ""
echo "写进程2最后结果:"
tail -n 3 write_output2.log

echo ""
echo "读进程1最后结果:"
tail -n 5 read_output1.log

echo ""
echo "读进程2最后结果:"
tail -n 5 read_output2.log

echo ""
echo "测试完成！"
echo ""
echo "查看详细日志："
echo "  写进程日志: cat write_output1.log write_output2.log"
echo "  读进程日志: cat read_output*.log"

# 显示统计信息
echo ""
echo "统计信息："
echo "写操作总数: $(grep -c "PUT.*stored" write_output*.log)"
echo "读操作总数: $(grep -c "GET.*->" read_output*.log)"
echo "事务提交总数: $(grep -c "COMMIT.*committed" write_output*.log read_output*.log)"

# 清理日志文件
rm -f write_output*.log read_output*.log 