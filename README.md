# DuckDB Paimon Extension

这是一个基于 [paimon-rust](https://github.com/apache/paimon-rust) 的 DuckDB extension，允许在 DuckDB 中直接读取 Apache Paimon 表。

## ⚠️ 实验性状态（Experimental Status）

**本扩展目前处于实验性阶段**，正在积极开发中。请注意：

- 🔬 **功能限制**：当前仅支持日志表和开启了 Deletion Vector 的主键表
- 🐛 **可能不稳定**：可能存在 bug 或性能问题
- 📝 **API 可能变化**：接口和功能可能会在后续版本中发生变化
- ⚡ **不建议用于生产环境**：请谨慎使用，建议先在测试环境中验证

如果你遇到问题或有改进建议，欢迎提交 Issue 或 Pull Request。

## 获取项目

### 克隆项目（推荐方式）

项目使用了 Git Submodules，需要同时克隆主项目和子模块：

```bash
# 方式 1: 克隆时同时初始化 submodules（推荐）
git clone --recurse-submodules <repository-url>
cd duckdb-extension-paimon

# 方式 2: 如果已经克隆了项目，需要初始化 submodules
git clone <repository-url>
cd duckdb-extension-paimon
git submodule update --init --recursive
```

### 更新 Submodules

如果项目更新了，可能需要更新 submodules：

```bash
git submodule update --remote --recursive
```

**重要提示**：项目包含以下 submodules：
- `duckdb/` - DuckDB 源代码（必需，用于构建扩展）
- `extension-ci-tools/` - DuckDB 扩展构建工具（必需，用于构建和打包）

如果没有正确初始化 submodules，构建会失败。

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
   - 项目通过 Git Submodule 包含 `duckdb/` 目录
   - 克隆项目时务必使用 `--recurse-submodules` 或运行 `git submodule update --init --recursive`
   - 如果 `duckdb/` 子目录存在，构建系统会自动检测
   - 否则需要设置 `DUCKDB_DIR` CMake 变量指向 DuckDB 源码目录

4. **paimon-rust**: 依赖会自动从 GitHub 仓库下载
   - 使用 `https://github.com/luoyuxia/paimon-rust` 的 `poc` 分支
   - 首次构建时会自动下载依赖

## 支持的平台

✅ **macOS** (Apple Silicon 和 Intel)  
✅ **Linux** (x86_64 和 ARM64)

构建系统会自动检测平台并配置相应的链接选项。

## 构建步骤

### 使用构建脚本（推荐）

最简单的方式是使用提供的构建脚本，它会在 macOS 和 Linux 上自动工作：

```bash
./build.sh
```

**更新依赖**：如果 `paimon-rust` 的 `poc` 分支更新了，需要更新依赖：

```bash
./build.sh --update-deps
# 或者简写
./build.sh -u
```

这会运行 `cargo update` 来获取最新的依赖代码。

### 手动命令行构建

#### macOS 和 Linux

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

**注意**：如果遇到版本不匹配的错误，需要指定正确的 DuckDB 版本：

```bash
# 首先检查你的 DuckDB 版本
python3 -c "import duckdb; conn = duckdb.connect(); print(conn.execute('SELECT library_version FROM pragma_version()').fetchone()[0])"

# 然后使用该版本构建扩展
cmake .. -DCMAKE_BUILD_TYPE=Release -DDUCKDB_VERSION=v1.4.2
```

对于 CPP ABI，版本必须与使用的 DuckDB 版本完全匹配。构建系统会尝试自动检测，但如果检测失败，你需要手动指定。

构建完成后，你会在 `build/` 目录下找到：
- `paimon.duckdb_extension` - 可加载的扩展文件
- `libpaimon_extension.a` - 静态库文件

## 使用方法

加载 extension 后，可以使用 `paimon_read` table function 来读取 Paimon 表：

```sql
-- 加载 extension
LOAD 'paimon';

-- 读取 Paimon 表
-- 支持日志表和开启了 Deletion Vector 的主键表
SELECT * FROM paimon_read('/path/to/warehouse', 'database_name', 'table_name');
```

**注意**：当前仅支持日志表和开启了 Deletion Vector 的主键表。如果尝试读取不支持的表类型，可能会遇到错误。

## 当前状态

✅ 已完成：
- Rust FFI 包装层
- CMake 构建配置
- DuckDB Table Function 框架
- 基本的类型映射

### 支持的表类型

当前扩展支持以下 Paimon 表类型：

- ✅ **日志表（Log Table）**
- ✅ **主键表（Primary Key Table）**：仅支持开启了 Deletion Vector 的主键表

⚠️ **不支持**：
- 未开启 Deletion Vector 的主键表

## 注意事项

1. **Git 依赖**: Rust FFI 依赖从 GitHub 仓库获取，使用 `poc` 分支。如果需要修改，请编辑 `rust-ffi/Cargo.toml`

2. **依赖更新**: Cargo 会锁定依赖版本到 `Cargo.lock` 文件中。如果 `paimon-rust` 的 `poc` 分支更新了：
   - 使用 `./build.sh --update-deps` 自动更新
   - 或手动运行 `cd rust-ffi && cargo update`
   - 或删除 `rust-ffi/Cargo.lock` 让 Cargo 重新解析依赖


