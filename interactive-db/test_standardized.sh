#!/bin/bash

# 标准化测试脚本
echo "标准化测试脚本 - 测试EXECUTE、OPEN、CLOSE命令"
echo "================================================"

# 清理数据库
rm -f interactive.db test_output.txt

echo "1. 测试OPEN命令"
echo "OPEN" | ./interactive-db

echo ""
echo "2. 测试简单的EXECUTE命令（单进程）"
echo "EXECUTE 1 simple_output.txt test_case1.txt 1 -1" | ./interactive-db

echo ""
echo "3. 检查输出文件"
if [ -f "simple_output.txt" ]; then
    echo "输出文件创建成功，内容："
    head -n 20 simple_output.txt
else
    echo "输出文件未创建"
fi

echo ""
echo "4. 测试多进程EXECUTE命令"
echo "EXECUTE 2 multi_output.txt test_case1.txt 1 -1 test_case4-1.txt 1 -1" | ./interactive-db

echo ""
echo "5. 检查多进程输出"
if [ -f "multi_output.txt" ]; then
    echo "多进程输出文件创建成功，内容："
    cat multi_output.txt
else
    echo "多进程输出文件未创建"
fi

echo ""
echo "6. 测试CLOSE命令"
echo "CLOSE" | ./interactive-db

echo ""
echo "测试完成！" 