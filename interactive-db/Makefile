# Interactive Database - Client-Server Makefile

.PHONY: all build clean server client help test demo multi-test transaction-test

# 默认配置
DEFAULT_PORT=8080
DEFAULT_DB=interactive.db

# 默认目标
all: build

# 编译所有程序
build:
	@echo "Building all components (coordinator + server + client)..."
	go build -o bin/cn ./cmd/coordinator
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client
	@echo "Build complete. Binaries are in bin/ directory."

# 编译协调节点
build-cn:
	@echo "Building coordinator..."
	go build -o bin/cn ./cmd/coordinator

# 编译服务器
build-server:
	@echo "Building server..."
	go build -o bin/server ./cmd/server

# 编译客户端
build-client:
	@echo "Building client..."
	go build -o bin/client ./cmd/client

# 兼容性别名
server-build: build-server
client-build: build-client

# ===========================================
# 兼容性运行目标（独立模式）
# ===========================================

# 运行服务器（默认配置）
server: build-server
	@echo "Starting database server on port $(DEFAULT_PORT) with database $(DEFAULT_DB)..."
	./bin/server -port $(DEFAULT_PORT) -db $(DEFAULT_DB) -standalone

# 运行服务器（自定义端口）
server-port: build-server
	@if [ -z "$(PORT)" ]; then \
		echo "Usage: make server-port PORT=8081"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(PORT) with database $(DEFAULT_DB)..."
	./bin/server -port $(PORT) -db $(DEFAULT_DB) -standalone

# 运行服务器（自定义数据库文件）
server-db: build-server
	@if [ -z "$(DB)" ]; then \
		echo "Usage: make server-db DB=mydata.db"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(DEFAULT_PORT) with database $(DB)..."
	./bin/server -port $(DEFAULT_PORT) -db $(DB) -standalone

# 运行服务器（自定义端口和数据库文件）
server-custom: build-server
	@if [ -z "$(PORT)" ] || [ -z "$(DB)" ]; then \
		echo "Usage: make server-custom PORT=8081 DB=mydata.db"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(PORT) with database $(DB)..."
	./bin/server -port $(PORT) -db $(DB) -standalone

# 运行客户端（交互式，默认端口）
client: build-client
	@echo "Starting interactive client..."
	./bin/client -addr localhost:$(DEFAULT_PORT)

# 运行客户端（自定义端口）
client-port: build-client
	@if [ -z "$(PORT)" ]; then \
		echo "Usage: make client-port PORT=8081"; \
		exit 1; \
	fi
	@echo "Starting interactive client connecting to port $(PORT)..."
	./bin/client -addr localhost:$(PORT)

# ===========================================
# 分布式架构运行目标
# ===========================================

# 运行协调节点 (端口9090)
run-cn: build-cn
	@echo "Starting coordinator on port 9090..."
	./bin/coordinator

# 运行服务器节点 (默认端口8080)
run-server: build-server
	@echo "Starting server on port 8080..."
	./bin/server

# 运行服务器节点指定端口
run-server-port: build-server
	@if [ -z "$(PORT)" ]; then \
		echo "Usage: make run-server-port PORT=8081"; \
		exit 1; \
	fi
	@echo "Starting server on port $(PORT)..."
	./bin/server -port $(PORT) -id server_$(PORT)

# 运行客户端连接协调节点
run-client: build-client
	@echo "Starting client..."
	./bin/client

# 测试3节点分布式集群
test-cluster: build
	@echo "Starting 3-node distributed cluster for testing..."
	@echo "Starting coordinator..."
	./bin/coordinator &
	@sleep 2
	@echo "Starting server nodes..."
	./bin/server -port 8081 -id server_1 &
	./bin/server -port 8082 -id server_2 &
	./bin/server -port 8083 -id server_3 &
	@echo ""
	@echo "============================================"
	@echo "分布式集群已启动！"
	@echo "============================================"
	@echo "协调节点: localhost:9090"
	@echo "服务节点1: localhost:8081 (server_1)"
	@echo "服务节点2: localhost:8082 (server_2)"
	@echo "服务节点3: localhost:8083 (server_3)"
	@echo ""
	@echo "使用以下命令连接客户端:"
	@echo "  make run-client"
	@echo ""
	@echo "按 Ctrl+C 停止所有进程"
	@echo "============================================"
	@wait

# 停止所有进程
stop:
	@echo "Stopping all processes..."
	pkill -f "bin/coordinator" || true
	pkill -f "bin/server" || true
	@echo "All processes stopped."

# ===========================================
# 测试目标（保持原有测试功能）
# ===========================================

# 运行基础测试
test: build-client
	@echo "Running basic functionality test..."
	./bin/client --batch tests/basic/test_sample.txt

# 运行多实例测试
multi-test:
	@echo "Running multi-instance test..."
	chmod +x tests/concurrent/multi_instance_test.sh
	./tests/concurrent/multi_instance_test.sh

# 运行多进程事务正确性测试
transaction-test:
	@echo "Running multi-process transaction correctness test..."
	chmod +x tests/concurrent/multi_process_transaction_test.sh
	./tests/concurrent/multi_process_transaction_test.sh

# 运行所有测试
test-all: test multi-test transaction-test
	@echo "All tests completed!"

# 演示多客户端功能（需要在不同终端中运行）
demo:
	@echo "演示选项:"
	@echo ""
	@echo "独立模式 (原有功能):"
	@echo "1. Terminal 1: make server"
	@echo "2. Terminal 2: make client"
	@echo "3. Terminal 3: make client"
	@echo "4. Terminal 4: make test"
	@echo ""
	@echo "分布式模式 (新功能):"
	@echo "1. Terminal 1: make test-cluster    # 启动分布式集群"
	@echo "2. Terminal 2: make run-client      # 连接客户端"
	@echo "3. Terminal 3: make run-client      # 多个客户端"
	@echo ""
	@echo "快速启动分布式集群: make test-cluster"

# 清理编译文件和数据库
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f interactive.db
	rm -f node*.db
	rm -f test_full*.db
	rm -rf test_output/
	rm -rf test_results/
	rm -f .cn_pid .server*_pid
	@echo "Clean complete."

# 创建目录
bin:
	mkdir -p bin

# 完整测试流程（运行所有测试套件）
full-test: build
	@echo "=========================================="
	@echo "Interactive Database - 完整测试套件"
	@echo "=========================================="
	@echo ""
	@echo "开始运行所有测试..."
	@mkdir -p test_results
	@echo ""
	@echo "测试1: 基础功能测试 (分布式模式)"
	@echo "================================="
	# @if pkill -f "bin/cn" >/dev/null 2>&1; then echo "停止现有CN..."; fi
	# @if pkill -f "bin/server" >/dev/null 2>&1; then echo "停止现有服务器..."; fi
	@sleep 2
	@echo "启动CN节点..."
	@./bin/cn -port 9090 & echo $$! > .cn_pid
	@sleep 2
	@echo "启动服务器节点..."
	@./bin/server -port 8080 -id test_node1 -cn "127.0.0.1:9091" -db test_full1.db & echo $$! > .server1_pid
	@./bin/server -port 8081 -id test_node2 -cn "127.0.0.1:9091" -db test_full2.db & echo $$! > .server2_pid
	@./bin/server -port 8082 -id test_node3 -cn "127.0.0.1:9091" -db test_full3.db & echo $$! > .server3_pid
	@sleep 5
	@if ./bin/client -host localhost -port 9090 -batch tests/basic/test_sample.txt > test_results/test_basic_result.log 2>&1; then \
		echo "✅ 基础功能测试 - PASS"; \
		TEST1_RESULT="PASS"; \
	else \
		echo "❌ 基础功能测试 - FAIL"; \
		TEST1_RESULT="FAIL"; \
	fi
	@if [ -f .cn_pid ]; then kill `cat .cn_pid` 2>/dev/null || true; rm -f .cn_pid; fi
	@if [ -f .server1_pid ]; then kill `cat .server1_pid` 2>/dev/null || true; rm -f .server1_pid; fi
	@if [ -f .server2_pid ]; then kill `cat .server2_pid` 2>/dev/null || true; rm -f .server2_pid; fi
	@if [ -f .server3_pid ]; then kill `cat .server3_pid` 2>/dev/null || true; rm -f .server3_pid; fi
	@sleep 2
	@echo ""
	@echo "测试2: 多实例并发测试"
	@echo "==================="
	@if ./tests/concurrent/multi_instance_test.sh > test_results/test_multi_result.log 2>&1; then \
		echo "✅ 多实例并发测试 - PASS"; \
		TEST2_RESULT="PASS"; \
	else \
		echo "❌ 多实例并发测试 - FAIL"; \
		TEST2_RESULT="FAIL"; \
	fi
	@echo ""
	@echo "测试3: 事务正确性测试"
	@echo "==================="
	@if ./tests/concurrent/multi_process_transaction_test.sh > test_results/test_transaction_result.log 2>&1; then \
		echo "✅ 事务正确性测试 - PASS"; \
		TEST3_RESULT="PASS"; \
	else \
		echo "❌ 事务正确性测试 - FAIL"; \
		TEST3_RESULT="FAIL"; \
	fi
	@echo ""
	@echo "=========================================="
	@echo "测试结果总结"
	@echo "=========================================="
	@if [ -f test_results/test_basic_result.log ]; then echo "基础功能测试: $$(if grep -q "Test case completed" test_results/test_basic_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@if [ -f test_results/test_multi_result.log ]; then echo "多实例并发测试: $$(if grep -q "completed!" test_results/test_multi_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@if [ -f test_results/test_transaction_result.log ]; then echo "事务正确性测试: $$(if grep -q "所有测试通过" test_results/test_transaction_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@echo ""
	@PASSED=0; TOTAL=3; \
	if [ -f test_results/test_basic_result.log ] && grep -q "Test case completed" test_results/test_basic_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ -f test_results/test_multi_result.log ] && grep -q "completed!" test_results/test_multi_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ -f test_results/test_transaction_result.log ] && grep -q "所有测试通过" test_results/test_transaction_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ $$PASSED -eq $$TOTAL ]; then \
		echo "🎉 所有测试通过！($$PASSED/$$TOTAL)"; \
		echo "完整测试套件执行成功！"; \
	else \
		echo "❌ $$((TOTAL-PASSED)) 个测试失败 ($$PASSED/$$TOTAL 通过)"; \
		echo "请检查详细日志: test_results/test_*_result.log"; \
		exit 1; \
	fi
	@echo ""
	@echo "清理测试文件..."
	@rm -f test_full.db test_full1.db test_full2.db test_full3.db .cn_pid .server1_pid .server2_pid .server3_pid
	@echo "完整测试完成！"

# 显示帮助信息
help:
	@echo "分布式交互式数据库 Makefile"
	@echo ""
	@echo "编译目标:"
	@echo "  all             - 编译所有组件 (coordinator + server + client)"
	@echo "  build           - 同 all"
	@echo "  build-cn        - 只编译协调节点"
	@echo "  build-server    - 只编译服务器节点"  
	@echo "  build-client    - 只编译客户端"
	@echo "  server-build    - 同 build-server (兼容性)"
	@echo "  client-build    - 同 build-client (兼容性)"
	@echo ""
	@echo "独立模式 (原有功能，向后兼容):"
	@echo "  server          - 运行服务器 (port=8080, db=interactive.db)"
	@echo "  server-port     - 自定义端口: make server-port PORT=8081"
	@echo "  server-db       - 自定义数据库: make server-db DB=mydata.db"
	@echo "  server-custom   - 自定义端口和数据库:"
	@echo "                    make server-custom PORT=8081 DB=mydata.db"
	@echo "  client          - 运行客户端 (连接到独立服务器)"
	@echo "  client-port     - 连接自定义端口: make client-port PORT=8081"
	@echo ""
	@echo "分布式模式 (新功能):"
	@echo "  run-cn          - 启动协调节点 (端口9090)"
	@echo "  run-server      - 启动服务器节点 (端口8080)"
	@echo "  run-server-port - 启动指定端口的服务器: make run-server-port PORT=8081"
	@echo "  run-client      - 启动客户端连接协调节点"
	@echo "  test-cluster    - 启动3节点测试集群"
	@echo "  stop            - 停止所有进程"
	@echo ""
	@echo "测试目标:"
	@echo "  test            - 运行基础功能测试"
	@echo "  multi-test      - 运行多实例并发测试"
	@echo "  transaction-test - 运行多进程事务正确性测试"
	@echo "  test-all        - 运行所有测试"
	@echo "  demo            - 显示演示说明"
	@echo "  full-test       - 完整自动化测试"
	@echo ""
	@echo "工具目标:"
	@echo "  clean           - 清理编译文件和数据库"
	@echo "  help            - 显示此帮助信息"
	@echo ""
	@echo "快速开始:"
	@echo ""
	@echo "独立模式 (向后兼容):"
	@echo "  make server     # Terminal 1"
	@echo "  make client     # Terminal 2"
	@echo ""
	@echo "分布式模式 (推荐):"
	@echo "  make test-cluster          # 启动分布式集群"
	@echo "  在新终端: make run-client  # 连接客户端"
	@echo ""
	@echo "自定义配置示例:"
	@echo "  make server-port PORT=9000              # 自定义端口"
	@echo "  make server-db DB=production.db         # 自定义数据库"
	@echo "  make server-custom PORT=9000 DB=test.db # 都自定义"
	@echo "  make client-port PORT=9000              # 连接到自定义端口" 