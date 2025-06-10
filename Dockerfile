FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 创建应用目录
WORKDIR /app

# 复制编译好的二进制文件
COPY bin/ /app/bin/

# 复制启动脚本
COPY entrypoint.sh /app/entrypoint.sh

# 给脚本和二进制文件执行权限
RUN chmod +x /app/entrypoint.sh && \
    chmod +x /app/bin/* && \
    ls -la /app/bin/

# 暴露端口 (协调节点9090，服务节点18080,28080,38080)
EXPOSE 9090 18080 28080 38080

# 使用run.sh启动服务
ENTRYPOINT ["./entrypoint.sh"]