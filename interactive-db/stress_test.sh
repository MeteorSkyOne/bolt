#!/bin/bash

# 压力测试 - 高并发访问
echo "压力测试 - 启动10个并发进程"
echo "================================================"

# 清理数据库
rm -f interactive.db

# 创建临时测试文件
cat > stress_ops.txt << 'EOF'
PUT COUNTER 0
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
PUT COUNTER (COUNTER+1)
GET COUNTER
EOF

echo "启动10个并发进程，每个进程执行10次递增操作..."

# 启动10个并发进程
for i in {1..10}; do
    ./interactive-db stress_ops.txt > stress_$i.log 2>&1 &
    PIDS[$i]=$!
done

# 等待所有进程完成
echo "等待所有进程完成..."
for i in {1..10}; do
    wait ${PIDS[$i]}
    echo "进程 $i 完成"
done

echo ""
echo "最终结果："
echo "GET COUNTER" | ./interactive-db

echo ""
echo "理论值应该是: 100 (10个进程 × 10次递增)"

# 检查是否有错误
echo ""
echo "检查错误日志："
grep -i "error" stress_*.log || echo "没有发现错误"

# 清理临时文件
rm -f stress_ops.txt stress_*.log 