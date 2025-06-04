# Interactive Database - Client-Server Makefile

.PHONY: all build clean server client help test demo multi-test transaction-test

# 默认配置
DEFAULT_PORT=8080
DEFAULT_DB=interactive.db

# 默认目标
all: build

# 编译所有程序
build:
	@echo "Building server and client..."
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client
	@echo "Build complete. Binaries are in bin/ directory."

# 编译服务器
server-build:
	@echo "Building server..."
	go build -o bin/server ./cmd/server

# 编译客户端
client-build:
	@echo "Building client..."
	go build -o bin/client ./cmd/client

# 运行服务器（默认配置）
server:
	@echo "Starting database server on port $(DEFAULT_PORT) with database $(DEFAULT_DB)..."
	go run ./cmd/server --port $(DEFAULT_PORT) --db $(DEFAULT_DB)

# 运行服务器（自定义端口）
server-port:
	@if [ -z "$(PORT)" ]; then \
		echo "Usage: make server-port PORT=8081"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(PORT) with database $(DEFAULT_DB)..."
	go run ./cmd/server --port $(PORT) --db $(DEFAULT_DB)

# 运行服务器（自定义数据库文件）
server-db:
	@if [ -z "$(DB)" ]; then \
		echo "Usage: make server-db DB=mydata.db"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(DEFAULT_PORT) with database $(DB)..."
	go run ./cmd/server --port $(DEFAULT_PORT) --db $(DB)

# 运行服务器（自定义端口和数据库文件）
server-custom:
	@if [ -z "$(PORT)" ] || [ -z "$(DB)" ]; then \
		echo "Usage: make server-custom PORT=8081 DB=mydata.db"; \
		exit 1; \
	fi
	@echo "Starting database server on port $(PORT) with database $(DB)..."
	go run ./cmd/server --port $(PORT) --db $(DB)

# 运行客户端（交互式，默认端口）
client:
	@echo "Starting interactive client..."
	go run ./cmd/client

# 运行客户端（自定义端口）
client-port:
	@if [ -z "$(PORT)" ]; then \
		echo "Usage: make client-port PORT=8081"; \
		exit 1; \
	fi
	@echo "Starting interactive client connecting to port $(PORT)..."
	go run ./cmd/client --port $(PORT)

# 运行基础测试
test:
	@echo "Running basic functionality test..."
	go run ./cmd/client --batch tests/basic/test_sample.txt

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
	@echo "To demo multi-client functionality:"
	@echo "1. Terminal 1: make server"
	@echo "2. Terminal 2: make client"
	@echo "3. Terminal 3: make client"
	@echo "4. Terminal 4: make test"

# 清理编译文件和数据库
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f interactive.db
	rm -rf test_output/
	rm -rf test_results/
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
	@echo ""
	@echo "测试1: 基础功能测试"
	@echo "==================="
	@if pkill -f "bin/server" >/dev/null 2>&1; then echo "停止现有服务器..."; fi
	@sleep 1
	@./bin/server --port 8080 --db test_full.db & echo $$! > .server_pid
	@sleep 3
	@if ./bin/client --batch tests/basic/test_sample.txt > test_basic_result.log 2>&1; then \
		echo "✅ 基础功能测试 - PASS"; \
		TEST1_RESULT="PASS"; \
	else \
		echo "❌ 基础功能测试 - FAIL"; \
		TEST1_RESULT="FAIL"; \
	fi
	@if [ -f .server_pid ]; then kill `cat .server_pid` 2>/dev/null || true; rm -f .server_pid; fi
	@sleep 2
	@echo ""
	@echo "测试2: 多实例并发测试"
	@echo "==================="
	@if ./tests/concurrent/multi_instance_test.sh > test_multi_result.log 2>&1; then \
		echo "✅ 多实例并发测试 - PASS"; \
		TEST2_RESULT="PASS"; \
	else \
		echo "❌ 多实例并发测试 - FAIL"; \
		TEST2_RESULT="FAIL"; \
	fi
	@echo ""
	@echo "测试3: 事务正确性测试"
	@echo "==================="
	@if ./tests/concurrent/multi_process_transaction_test.sh > test_transaction_result.log 2>&1; then \
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
	@if [ -f test_basic_result.log ]; then echo "基础功能测试: $$(if grep -q "Test case completed" test_basic_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@if [ -f test_multi_result.log ]; then echo "多实例并发测试: $$(if grep -q "completed!" test_multi_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@if [ -f test_transaction_result.log ]; then echo "事务正确性测试: $$(if grep -q "所有测试通过" test_transaction_result.log; then echo "PASS"; else echo "FAIL"; fi)"; fi
	@echo ""
	@PASSED=0; TOTAL=3; \
	if [ -f test_basic_result.log ] && grep -q "Test case completed" test_basic_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ -f test_multi_result.log ] && grep -q "completed!" test_multi_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ -f test_transaction_result.log ] && grep -q "所有测试通过" test_transaction_result.log; then PASSED=$$((PASSED+1)); fi; \
	if [ $$PASSED -eq $$TOTAL ]; then \
		echo "🎉 所有测试通过！($$PASSED/$$TOTAL)"; \
		echo "完整测试套件执行成功！"; \
	else \
		echo "❌ $$((TOTAL-PASSED)) 个测试失败 ($$PASSED/$$TOTAL 通过)"; \
		echo "请检查详细日志: test_*_result.log"; \
		exit 1; \
	fi
	@echo ""
	@echo "清理测试文件..."
	@rm -f test_*.log test_full.db .server_pid
	@echo "完整测试完成！"

# 显示帮助信息
help:
	@echo "Interactive Database - Client-Server Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  build           - Build both server and client"
	@echo "  server-build    - Build only server"
	@echo "  client-build    - Build only client"
	@echo ""
	@echo "Server targets:"
	@echo "  server          - Run server (port=8080, db=interactive.db)"
	@echo "  server-port     - Run server with custom port: make server-port PORT=8081"
	@echo "  server-db       - Run server with custom database: make server-db DB=mydata.db"
	@echo "  server-custom   - Run server with custom port and database:"
	@echo "                    make server-custom PORT=8081 DB=mydata.db"
	@echo ""
	@echo "Client targets:"
	@echo "  client          - Run client (connects to port 8080)"
	@echo "  client-port     - Run client with custom port: make client-port PORT=8081"
	@echo ""
	@echo "Testing targets:"
	@echo "  test            - Run basic functionality test"
	@echo "  multi-test      - Run multi-instance concurrent test"
	@echo "  transaction-test - Run multi-process transaction correctness test"
	@echo "  test-all        - Run all tests"
	@echo "  demo            - Show demo instructions"
	@echo "  full-test       - Complete automated test"
	@echo "  clean           - Remove compiled files and database"
	@echo "  help            - Show this help message"
	@echo ""
	@echo "Quick start:"
	@echo "  make server     # Terminal 1"
	@echo "  make client     # Terminal 2"
	@echo ""
	@echo "Custom configuration examples:"
	@echo "  make server-port PORT=9000              # Custom port"
	@echo "  make server-db DB=production.db         # Custom database"
	@echo "  make server-custom PORT=9000 DB=test.db # Both custom"
	@echo "  make client-port PORT=9000              # Connect to custom port" 