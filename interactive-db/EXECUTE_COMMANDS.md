# EXECUTE 命令使用说明

## 问题修复 ✅

**问题**：EXECUTE 命令执行时子进程卡住，无法正常完成。

**根本原因**：子进程在执行完测试文件后没有自动退出，而是等待更多输入，导致父进程一直等待。

**解决方案**：
1. **新增批处理模式**：添加 `--batch/-b` 选项，确保程序执行完文件后立即退出
2. **新增单命令模式**：添加 `--command/-c` 选项，执行单个命令后立即退出
3. **修改子进程调用**：EXECUTE命令现在使用 `--batch` 选项调用子进程
4. **保持向后兼容**：原有的直接指定文件方式仍然支持

## 新增命令行选项

### 批处理模式
```bash
# 长选项
./interactive-db --batch testfile.txt

# 短选项
./interactive-db -b testfile.txt
```

### 单命令模式
```bash
# 长选项
./interactive-db --command "PUT A 1"

# 短选项
./interactive-db -c "GET A"
./interactive-db -c "EXECUTE 2 output.txt test1.txt 1 -1 test2.txt 1 -1"
```

## 命令格式

```
EXECUTE NumProcess OutputFile [InputFile ExecNum ExecTime]...
```

**重要改进**：现在子进程调用使用批处理模式，确保正常退出：
```bash
# 内部调用变更（用户无需关心）
# 原来：./interactive-db inputFile
# 现在：./interactive-db --batch inputFile
```

### 参数说明

- `NumProcess`: 要启动的进程数量
- `OutputFile`: 保存所有进程输出的文件名
- 每个进程需要三个参数：
  - `InputFile`: 进程要执行的测试文件
  - `ExecNum`: 执行次数（-1表示不限制次数）
  - `ExecTime`: 执行时间（秒，-1表示不限制时间）

### 重要约束

- `ExecNum` 和 `ExecTime` 必须有且仅有一个为 -1
- 如果 `ExecNum` 不为 -1，进程会执行指定次数后结束
- 如果 `ExecTime` 不为 -1，进程会在指定时间后结束

## 使用示例

### 1. 基础使用（交互式）
```bash
# 启动交互式终端
./interactive-db

# 执行命令
db> OPEN
db> EXECUTE 1 output.txt test_case1.txt 1 -1
db> CLOSE
```

### 2. 单命令执行
```bash
# 直接执行EXECUTE命令
./interactive-db -c "EXECUTE 1 output.txt test_case1.txt 1 -1"

# 执行其他命令
./interactive-db -c "PUT A 1"
./interactive-db -c "GET A"
```

### 3. 批处理文件使用
```bash
# 创建批处理文件 batch_test.txt
echo "OPEN" > batch_test.txt
echo "EXECUTE 2 results.txt test1.txt 3 -1 test2.txt -1 5" >> batch_test.txt
echo "CLOSE" >> batch_test.txt

# 使用批处理模式运行
./interactive-db --batch batch_test.txt
```

### 4. 多进程并发测试
```bash
# 直接执行多进程测试
./interactive-db -c "EXECUTE 3 concurrent_output.txt test_case3-1.txt 5 -1 test_case3-2.txt 5 -1 test_case3-3.txt 5 -1"
```

### 5. 时间限制测试
```bash
# 时间限制测试
./interactive-db -c "EXECUTE 2 time_output.txt test_case1.txt -1 10 test_case4-1.txt -1 10"
```

## 输出格式

所有进程的输出会按以下格式保存到指定文件：

```
=== Process 1 Execution 1 ===
Running test case from test_case1.txt
--------------------------------------------------
db> PUT A 1
PUT A 1 -> stored
db> GET A
GET A -> 1
Test case completed.

=== Process 2 Execution 1 ===
Running test case from test_case4-1.txt
--------------------------------------------------
db> PUT A 0
PUT A 0 -> stored
db> PUT B 0
PUT B 0 -> stored
Test case completed.
```

## 测试验证

### 新功能验证脚本
```bash
# 运行完整的功能测试
chmod +x test_fix.sh
./test_fix.sh

# 运行新功能演示
chmod +x demo_new_features.sh
./demo_new_features.sh
```

### 手动验证步骤
1. **批处理模式测试**：
   ```bash
   timeout 10s ./interactive-db --batch test_simple.txt
   # 应该在10秒内正常完成，不应该超时
   ```

2. **单命令模式测试**：
   ```bash
   ./interactive-db -c "PUT TEST 123"
   ./interactive-db -c "GET TEST"
   # 应该立即执行并退出
   ```

3. **EXECUTE命令测试**：
   ```bash
   ./interactive-db -c "EXECUTE 1 test_output.txt test_simple.txt 1 -1"
   # 应该正常完成并生成输出文件
   ```

## 性能优势

- ✅ **无卡住问题**：子进程确保正常退出
- ✅ **快速执行**：批处理模式减少启动开销
- ✅ **灵活使用**：支持交互式、批处理、单命令三种模式
- ✅ **向后兼容**：原有脚本无需修改
- ✅ **并发安全**：多进程执行稳定可靠

## 最佳实践

1. **推荐使用批处理模式**：
   ```bash
   # 推荐
   ./interactive-db --batch testfile.txt
   
   # 而不是
   ./interactive-db testfile.txt
   ```

2. **单命令适合简单操作**：
   ```bash
   ./interactive-db -c "PUT A 1"
   ./interactive-db -c "GET A"
   ```

3. **复杂测试使用EXECUTE命令**：
   ```bash
   ./interactive-db -c "EXECUTE 3 results.txt test1.txt 5 -1 test2.txt 5 -1 test3.txt 5 -1"
   ```

4. **调试时使用超时保护**：
   ```bash
   timeout 30s ./interactive-db --batch your_test.txt
   ```

## 错误处理

### 常见错误和解决方案

1. **参数数量错误**：
   ```
   Error: EXECUTE requires at least NumProcess and OutputFile
   ```
   解决：检查命令格式，确保提供了所有必需参数

2. **进程配置错误**：
   ```
   Error: Each process must have InputFile, ExecNum, and ExecTime
   ```
   解决：确保每个进程都有三个参数

3. **执行约束错误**：
   ```
   Error: Exactly one of ExecNum or ExecTime must be -1
   ```
   解决：确保每个进程的 ExecNum 和 ExecTime 中有且仅有一个为 -1

4. **输入文件不存在**：
   ```
   Process 1 execution 1 failed: exit status 1
   ```
   解决：检查输入文件路径是否正确

## 性能特点

- **并发执行**：多个进程同时运行，提高测试效率
- **输出聚合**：所有进程输出统一保存，便于分析
- **超时控制**：支持时间限制，避免无限等待
- **错误隔离**：单个进程失败不影响其他进程

## 最佳实践

1. **测试文件设计**：
   - 保持测试文件简洁明确
   - 避免在测试文件中使用 EXIT 命令
   - 确保测试操作能够正常完成

2. **并发测试**：
   - 不同进程使用不同的键名空间（如进程1用A,B，进程2用C,D）
   - 合理设置执行次数和时间限制

3. **输出管理**：
   - 使用描述性的输出文件名
   - 定期清理输出文件

4. **调试技巧**：
   - 先用单进程测试验证测试文件正确性
   - 使用 timeout 命令避免无限等待
   - 检查输出文件内容确认执行结果 