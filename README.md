# DuckDB Fluss Extension

A DuckDB extension based on [paimon-rust](https://github.com/apache/paimon-rust) that allows reading Apache Paimon tables directly in DuckDB.

## ⚠️ Experimental Status

**This extension is currently in experimental stage** and under active development. Please note:

- 🔬 **Feature Limitations**: Currently only supports log tables and primary key tables with Deletion Vector enabled
- 🐛 **May be unstable**: May contain bugs or performance issues
- 📝 **API may change**: Interfaces and features may change in future versions
- ⚡ **Not recommended for production**: Use with caution, please test in a test environment first

If you encounter issues or have suggestions for improvement, please feel free to submit an Issue or Pull Request.

## Getting Started

### Clone the Project (Recommended)

The project uses Git Submodules, so you need to clone both the main project and submodules:

```bash
# Method 1: Clone with submodules initialized (recommended)
git clone --recurse-submodules <repository-url>
cd duckdb-extension-fluss-lake

# Method 2: If you've already cloned the project, initialize submodules
git clone <repository-url>
cd duckdb-extension-fluss-lake
git submodule update --init --recursive
```

### Update Submodules

If the project is updated, you may need to update submodules:

```bash
git submodule update --remote --recursive
```

**Important**: The project contains the following submodules:
- `duckdb/` - DuckDB source code (required for building the extension)
- `extension-ci-tools/` - DuckDB extension build tools (required for building and packaging)

If submodules are not properly initialized, the build will fail.

## Project Structure

```
duckdb-extension-fluss-lake/
├── rust-ffi/              # Rust FFI wrapper layer
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs         # C ABI interface
├── src/
│   ├── include/
│   │   ├── fluss_extension.hpp
│   │   └── fluss_ffi.h   # C++ FFI header
│   └── fluss_extension.cpp  # DuckDB extension implementation
├── CMakeLists.txt
└── extension_config.cmake
```

## Build Requirements

1. **Rust toolchain**: Rust and Cargo need to be installed
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **CMake 3.10+**

3. **DuckDB source code**: DuckDB headers are required
   - The project includes the `duckdb/` directory via Git Submodule
   - When cloning, make sure to use `--recurse-submodules` or run `git submodule update --init --recursive`
   - If the `duckdb/` subdirectory exists, the build system will auto-detect it
   - Otherwise, you need to set the `DUCKDB_DIR` CMake variable to point to the DuckDB source directory

4. **paimon-rust**: Dependencies will be automatically downloaded from GitHub repository
   - Uses the `poc` branch from `https://github.com/luoyuxia/paimon-rust`
   - Dependencies will be automatically downloaded on first build

## Supported Platforms

✅ **macOS** (Apple Silicon and Intel)  
✅ **Linux** (x86_64 and ARM64)

The build system will automatically detect the platform and configure the appropriate linking options.

## Quick Start (Using Pre-built Extension)

If you're using **macOS** and **DuckDB 1.4.2**, you can directly download the pre-built extension without manual building:

```bash
# Download pre-built extension
curl -L -o fluss.duckdb_extension \
  https://github.com/luoyuxia/duckdb-extension-fluss-lake/releases/download/0.0.1-beta/fluss.duckdb_extension

# Load in DuckDB
duckdb -unsigned
# Then in the DuckDB shell, execute:
LOAD '/path/to/fluss.duckdb_extension';
```

**Important Notes**:
- ⚠️ Pre-built extension is only for **macOS** systems
- ⚠️ Only tested with **DuckDB 1.4.2**, other versions may not be compatible
- ⚠️ If you need Linux version or extensions for other DuckDB versions, please refer to the build steps below to build manually

## Build Steps

### Using Build Script (Recommended)

The easiest way is to use the provided build script, which works automatically on both macOS and Linux:

```bash
./build.sh
```

**Update Dependencies**: If the `poc` branch of `paimon-rust` is updated, you need to update dependencies:

```bash
./build.sh --update-deps
# or shorthand
./build.sh -u
```

This will run `cargo update` to fetch the latest dependency code.

### Manual Command Line Build

#### macOS and Linux

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

**Note**: If you encounter version mismatch errors, you need to specify the correct DuckDB version:

```bash
# First check your DuckDB version
python3 -c "import duckdb; conn = duckdb.connect(); print(conn.execute('SELECT library_version FROM pragma_version()').fetchone()[0])"

# Then build the extension with that version
cmake .. -DCMAKE_BUILD_TYPE=Release -DDUCKDB_VERSION=v1.4.2
```

For CPP ABI, the version must exactly match the DuckDB version you're using. The build system will try to auto-detect, but if detection fails, you need to specify it manually.

After building, you'll find in the `build/` directory:
- `fluss.duckdb_extension` - Loadable extension file
- `libfluss_extension.a` - Static library file

## Usage

After loading the extension, you can use the `fluss_read` table function to read Paimon tables:

```sql
-- Load extension
LOAD 'fluss';

-- Read Paimon table
-- Supports log tables and primary key tables with Deletion Vector enabled
SELECT * FROM fluss_read('/path/to/warehouse', 'database_name', 'table_name');
```

**Note**: Currently only supports log tables and primary key tables with Deletion Vector enabled. If you try to read unsupported table types, you may encounter errors.

## Current Status

✅ Completed:
- Rust FFI wrapper layer
- CMake build configuration
- DuckDB Table Function framework
- Basic type mapping

### Supported Table Types

The extension currently supports the following Paimon table types:

- ✅ **Log Table**
- ✅ **Primary Key Table**: Only supports primary key tables with Deletion Vector enabled

⚠️ **Not Supported**:
- Primary key tables without Deletion Vector enabled

## Notes

1. **Git Dependencies**: Rust FFI dependencies are fetched from GitHub repository, using the `poc` branch. If you need to modify, edit `rust-ffi/Cargo.toml`

2. **Dependency Updates**: Cargo locks dependency versions in the `Cargo.lock` file. If the `poc` branch of `paimon-rust` is updated:
   - Use `./build.sh --update-deps` to update automatically
   - Or manually run `cd rust-ffi && cargo update`
   - Or delete `rust-ffi/Cargo.lock` to let Cargo re-resolve dependencies
