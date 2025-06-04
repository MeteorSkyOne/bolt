#!/bin/bash

echo "=== 新功能演示 ==="
echo "演示新增的命令行选项和批处理功能"
echo ""

# 重新编译
echo "1. 编译程序..."
go build -o interactive-db main.go
echo "✓ 编译完成"
echo ""

# 演示批处理模式
echo "2. 演示批处理模式（--batch）"
echo "命令：./interactive-db --batch test_simple.txt"
echo "输出："
./interactive-db --batch test_simple.txt
echo ""

# 演示单命令模式
echo "3. 演示单命令模式（--command）"
echo "命令：./interactive-db --command \"PUT DEMO 999\""
echo "输出："
./interactive-db --command "PUT DEMO 999"
echo ""

echo "命令：./interactive-db -c \"GET DEMO\""
echo "输出："
./interactive-db -c "GET DEMO"
echo ""

# 演示EXECUTE命令
echo "4. 演示EXECUTE命令（单进程）"
echo "命令：./interactive-db -c \"EXECUTE 1 demo_output.txt test_simple.txt 1 -1\""
echo "输出："
./interactive-db -c "EXECUTE 1 demo_output.txt test_simple.txt 1 -1"
echo ""

if [ -f "demo_output.txt" ]; then
    echo "生成的输出文件内容："
    cat demo_output.txt
    echo ""
fi

# 演示多进程EXECUTE
echo "5. 演示多进程EXECUTE命令"
echo "命令：./interactive-db -c \"EXECUTE 2 multi_demo.txt test_simple.txt 1 -1 test_case1.txt 1 -1\""
echo "输出："
./interactive-db -c "EXECUTE 2 multi_demo.txt test_simple.txt 1 -1 test_case1.txt 1 -1"
echo ""

if [ -f "multi_demo.txt" ]; then
    echo "多进程输出文件内容："
    cat multi_demo.txt
    echo ""
fi

# 演示帮助信息
echo "6. 查看更新的帮助信息"
echo "命令：./interactive-db -c \"HELP\""
echo "输出："
./interactive-db -c "HELP" | head -n 30
echo "..."
echo ""

# 清理
echo "7. 清理演示文件"
rm -f demo_output.txt multi_demo.txt
echo "✓ 清理完成"
echo ""

echo "=== 演示结束 ==="
echo "新功能说明："
echo "• --batch/-b: 批处理模式，执行测试文件后自动退出"
echo "• --command/-c: 单命令模式，执行单个命令后自动退出"
echo "• EXECUTE命令现在使用批处理模式调用子进程，避免卡住问题"
echo "• 兼容原有的直接指定文件方式" 