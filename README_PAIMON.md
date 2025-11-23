# DuckDB Paimon Extension

这是一个基于 [paimon-rust](https://github.com/apache/paimon-rust) 的 DuckDB extension，允许在 DuckDB 中直接读取 Apache Paimon 表。

## 项目结构

```
duckdb-extension-paimon/
├── rust-ffi/              # Rust FFI 包装层
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs         # C ABI 接口
├── src/
│   ├── include/
│   │   ├── paimon_extension.hpp
│   │   └── paimon_ffi.h   # C++ FFI 头文件
│   └── paimon_extension.cpp  # DuckDB extension 实现
├── CMakeLists.txt
└── extension_config.cmake
```

## 构建要求

1. **Rust 工具链**: 需要安装 Rust 和 Cargo
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **CMake 3.10+**

3. **DuckDB 源代码**: 需要 DuckDB 的头文件
   - 如果项目中有 `duckdb/` 子目录，会自动检测
   - 否则需要设置 `DUCKDB_DIR` CMake 变量

4. **paimon-rust**: 需要 `paimon-rust` 项目在 `../../rust-projects/paimon-rust`

## 构建步骤

### 在 CLion 中构建

1. 打开项目目录
2. CLion 会自动检测 CMake 项目
3. 确保 Rust 工具链已安装（Cargo 在 PATH 中）
4. 构建项目

### 命令行构建

```bash
mkdir build && cd build
cmake ..
make
```

## 使用方法

加载 extension 后，可以使用 `paimon_read` table function 来读取 Paimon 表：

```sql
-- 加载 extension
LOAD 'paimon';

-- 读取 Paimon 表
SELECT * FROM paimon_read('/path/to/warehouse', 'database_name', 'table_name');
```

## 当前状态

✅ 已完成：
- Rust FFI 包装层
- CMake 构建配置
- DuckDB Table Function 框架
- 基本的类型映射

⚠️ 待完成：
- Arrow RecordBatch 到 DuckDB DataChunk 的完整转换
- 错误处理优化
- 性能优化
- 测试

## 注意事项

1. **路径依赖**: Rust FFI 代码中硬编码了 `paimon-rust` 的路径为 `../../rust-projects/paimon-rust`，如果路径不同需要修改 `rust-ffi/Cargo.toml`

2. **异步运行时**: Rust FFI 使用 Tokio runtime，每个 catalog 实例都有自己的 runtime

3. **内存管理**: 注意正确释放 Rust FFI 返回的资源

4. **类型转换**: 当前只实现了基本的 Arrow 类型到 DuckDB 类型的映射，可能需要扩展

## 开发计划

- [ ] 实现完整的 Arrow 到 DuckDB 数据转换
- [ ] 支持更多 Arrow 数据类型
- [ ] 添加错误处理和日志
- [ ] 性能优化（批量处理、并行扫描等）
- [ ] 添加单元测试和集成测试
- [ ] 支持更多 Paimon 功能（分区、快照选择等）

