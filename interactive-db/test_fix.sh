#!/bin/bash

echo "测试修复后的EXECUTE命令"
echo "========================"

# 重新编译
echo "重新编译程序..."
go build -o interactive-db main.go

if [ $? -ne 0 ]; then
    echo "编译失败"
    exit 1
fi

echo "编译成功"

# 测试新的批处理模式
echo ""
echo "测试批处理模式..."
timeout 10s ./interactive-db --batch test_simple.txt

if [ $? -eq 124 ]; then
    echo "批处理模式超时！"
else
    echo "批处理模式正常完成"
fi

# 测试单命令模式
echo ""
echo "测试单命令模式..."
timeout 5s ./interactive-db --command "PUT X 100"
timeout 5s ./interactive-db -c "GET X"

# 测试兼容模式（原有方式）
echo ""
echo "测试兼容模式..."
timeout 10s ./interactive-db test_simple.txt

if [ $? -eq 124 ]; then
    echo "兼容模式超时！"
else
    echo "兼容模式正常完成"
fi

# 测试EXECUTE命令
echo ""
echo "测试EXECUTE命令..."
timeout 30s ./interactive-db --batch debug_execute.txt

if [ $? -eq 124 ]; then
    echo "EXECUTE命令超时！"
else
    echo "EXECUTE命令完成"
    if [ -f "debug_output.txt" ]; then
        echo "输出文件内容："
        cat debug_output.txt
    else
        echo "输出文件未生成"
    fi
fi

# 测试直接EXECUTE命令
echo ""
echo "测试直接EXECUTE命令..."
timeout 30s ./interactive-db -c "EXECUTE 1 direct_output.txt test_simple.txt 1 -1"

if [ $? -eq 124 ]; then
    echo "直接EXECUTE命令超时！"
else
    echo "直接EXECUTE命令完成"
    if [ -f "direct_output.txt" ]; then
        echo "直接输出文件内容："
        cat direct_output.txt
    else
        echo "直接输出文件未生成"
    fi
fi

echo ""
echo "测试完成！" 