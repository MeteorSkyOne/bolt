#!/bin/bash

# 生成gRPC代码的脚本

# 检查protoc是否安装
if ! command -v protoc &> /dev/null; then
    echo "protoc 未安装，请先安装 Protocol Buffer Compiler"
    echo "Ubuntu/Debian: sudo apt install protobuf-compiler"
    echo "MacOS: brew install protobuf" 
    echo "或者从 https://github.com/protocolbuffers/protobuf/releases 下载"
    exit 1
fi

# 检查Go的protoc插件是否安装
if ! command -v protoc-gen-go &> /dev/null; then
    echo "protoc-gen-go 未安装，正在安装..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "protoc-gen-go-grpc 未安装，正在安装..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# 确保输出目录存在
mkdir -p cmd/proto/coordinator
mkdir -p cmd/proto/server

echo "正在生成coordinator proto代码..."
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    cmd/proto/coordinator/coordinator.proto

echo "正在生成server proto代码..."
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    cmd/proto/server/server.proto

echo "gRPC代码生成完成！"
echo "生成的文件："
echo "  cmd/proto/coordinator/coordinator.pb.go"
echo "  cmd/proto/coordinator/coordinator_grpc.pb.go" 
echo "  cmd/proto/server/server.pb.go"
echo "  cmd/proto/server/server_grpc.pb.go" 