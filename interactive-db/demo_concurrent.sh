#!/bin/bash

# 并发能力演示
echo "BoltDB并发能力演示"
echo "================================================"

# 清理数据库
rm -f interactive.db

# 创建三个不同的操作文件
cat > demo1.txt << 'EOF'
PUT USER1_COUNTER 0
PUT USER1_COUNTER (USER1_COUNTER+1)
PUT USER1_COUNTER (USER1_COUNTER+1)
PUT USER1_COUNTER (USER1_COUNTER+1)
GET USER1_COUNTER
PUT USER1_DATA 100
GET USER1_DATA
EOF

cat > demo2.txt << 'EOF'
PUT USER2_COUNTER 0
PUT USER2_COUNTER (USER2_COUNTER+1)
PUT USER2_COUNTER (USER2_COUNTER+1)
PUT USER2_COUNTER (USER2_COUNTER+1)
PUT USER2_COUNTER (USER2_COUNTER+1)
PUT USER2_COUNTER (USER2_COUNTER+1)
GET USER2_COUNTER
PUT USER2_DATA 200
GET USER2_DATA
EOF

cat > demo3.txt << 'EOF'
PUT USER3_COUNTER 0
PUT USER3_COUNTER (USER3_COUNTER+1)
PUT USER3_COUNTER (USER3_COUNTER+1)
GET USER3_COUNTER
PUT USER3_DATA 300
GET USER3_DATA
PUT SHARED_RESOURCE 999
GET SHARED_RESOURCE
EOF

echo "启动3个进程，每个进程处理不同用户的数据..."

# 并发启动3个进程
./interactive-db demo1.txt > demo1.log 2>&1 &
PID1=$!

./interactive-db demo2.txt > demo2.log 2>&1 &
PID2=$!

./interactive-db demo3.txt > demo3.log 2>&1 &
PID3=$!

# 等待所有进程完成
wait $PID1 $PID2 $PID3

echo "所有进程完成"

echo ""
echo "最终数据库状态："
echo "SHOW" | ./interactive-db

echo ""
echo "各进程的执行结果："
echo ""
echo "用户1操作结果:"
tail -n 4 demo1.log

echo ""
echo "用户2操作结果:"
tail -n 4 demo2.log

echo ""
echo "用户3操作结果:"
tail -n 6 demo3.log

# 清理临时文件
rm -f demo1.txt demo2.txt demo3.txt demo1.log demo2.log demo3.log

echo ""
echo "演示完成！BoltDB成功处理了多进程并发访问。" 