# DuckDB Fluss Extension

A DuckDB extension that enables seamless reading of data lake tables (currently Apache Paimon, with Apache Iceberg support coming soon) with real-time streaming capabilities through Fluss. This extension combines the power of lake storage formats with Fluss's streaming capabilities, allowing you to query both historical snapshot data and real-time log data in a single query.

## Overview

The Fluss extension provides a unified interface to read data from data lake tables that are integrated with Fluss. Currently, it supports **Apache Paimon** (with **Apache Iceberg** support coming soon). It addresses the **data freshness challenge** in traditional data lakes by combining:

- **Historical Data Layer (Lake Storage)**: Cost-effective lake storage with minute-level freshness (currently Paimon, Iceberg coming soon)
- **Real-time Layer (Fluss)**: Streaming layer providing second-level data freshness

The extension implements a two-phase reading strategy:

1. **Phase 1 - Paimon Snapshot**: Reads historical data from Paimon's lake storage format (up to the last snapshot point)
2. **Phase 2 - Fluss Streaming**: Reads real-time log data from Fluss, starting from the snapshot offset to the latest offset

This allows you to query complete datasets that include both historical and real-time data in a single DuckDB query, achieving **second-level data freshness** instead of the traditional minute-level freshness of data lakes.

## Background & Motivation

### The Data Freshness Challenge

Traditional data lakes typically provide **minute-level data freshness** due to batch processing and periodic data ingestion. This latency can be a significant limitation for real-time analytics, monitoring, and decision-making scenarios that require up-to-the-second data visibility.

### Fluss as the Real-time Layer

**Fluss** serves as a **real-time streaming layer** on top of the data lake, enabling **second-level data freshness**. The architecture works as follows:

1. **Historical Data (Data Lake)**: Stored in lake storage format (currently Paimon, Iceberg support coming soon), providing cost-effective storage for large volumes of historical data
2. **Real-time Data (Fluss)**: Streamed in real-time, capturing the latest changes and events as they happen
3. **Unified Query**: DuckDB queries seamlessly combine both layers, giving you access to complete datasets with minimal latency

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Pipeline                             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Data Sources → Fluss (Real-time Stream) → Data Lake         │
│                    ↓                        ↓                │
│              Real-time Layer          Historical Layer       │
│              (Second-level)           (Minute-level)         │
│                    ↓                        ↓                │
│              ┌──────────────────────────────────┐           │
│              │   DuckDB Fluss Extension          │           │
│              │   (Unified Query Interface)       │           │
│              └──────────────────────────────────┘           │
│                           ↓                                  │
│                    Complete Dataset                          │
│              (Historical + Real-time)                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

When you query a table through this extension:

1. **Historical Snapshot**: First reads all historical data from the data lake (currently Paimon append-only tables, Iceberg support coming soon) up to the last snapshot point
2. **Real-time Stream**: Then reads the incremental changes from Fluss (from snapshot offset to latest offset)
3. **Unified Result**: Returns a complete, up-to-date dataset with second-level freshness

This architecture provides the best of both worlds:
- **Cost Efficiency**: Historical data stored in cost-effective lake storage
- **Real-time Capability**: Latest data available through streaming layer
- **Unified Access**: Single query interface for both layers
- **Data Freshness**: Achieve second-level data freshness instead of minute-level

## ⚠️ Experimental Status

**This extension is currently in experimental stage** and under active development. Please note:

- 🔬 **Feature Limitations**: 
  - Currently only supports **Paimon append-only tables (log tables)**
  - **Apache Iceberg** support is in development and coming soon
  - Other Paimon table types (primary key tables, update tables) are not yet supported
- 🐛 **May be unstable**: May contain bugs or performance issues
- 📝 **API may change**: Interfaces and features may change in future versions
- ⚡ **Not recommended for production**: Use with caution, please test in a test environment first

If you encounter issues or have suggestions for improvement, please feel free to submit an Issue or Pull Request.

## Features

- ✅ **Second-level Data Freshness**: Achieve real-time data access (second-level) instead of traditional minute-level freshness
- ✅ **Unified Query Interface**: Query both Paimon snapshot data and Fluss streaming data in a single SQL query
- ✅ **Automatic Phase Transition**: Seamlessly transitions from historical to real-time data
- ✅ **Hybrid Architecture**: Combines cost-effective lake storage with real-time streaming capabilities
- ✅ **Type Mapping**: Automatic conversion between Paimon/Arrow types and DuckDB types
- ✅ **Multi-bucket Support**: Handles partitioned tables with multiple buckets
- ✅ **Cross-platform**: Supports macOS (Apple Silicon & Intel) and Linux (x86_64 & ARM64)

## Architecture

The extension consists of three main components:

1. **C++ Extension Layer** (`src/fluss_extension.cpp`): DuckDB table function implementation
2. **Rust FFI Layer** (`rust-ffi/`): Rust bindings for Paimon and Fluss libraries
3. **CMake Build System**: Automated build configuration for both components

```
┌─────────────────┐
│   DuckDB SQL    │
└────────┬────────┘
         │
┌────────▼─────────────────┐
│  fluss_read()            │
│  Table Function          │
└────────┬─────────────────┘
         │
┌────────▼─────────────────┐
│  C++ Extension Layer      │
│  (fluss_extension.cpp)    │
└────────┬─────────────────┘
         │
┌────────▼─────────────────┐
│  Rust FFI Layer          │
│  (fluss_ffi.rs)          │
└────────┬─────────────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌──▼────┐
│Paimon │ │ Fluss │
│Rust   │ │ Rust  │
└───────┘ └───────┘
```

## Quick Start

For a complete step-by-step guide on setting up Fluss cluster, configuring lakehouse storage, and querying data, see the [Quick Start Guide for Paimon](docs/QUICKSTART_PAIMON.md).

### Prerequisites
**DuckDB** (1.4.2+)
- The extension includes DuckDB source code as a submodule

### Using Pre-built Extension (macOS only)

If you're using **macOS** and **DuckDB 1.4.2**, you can directly download the pre-built extension:

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
- ⚠️ If you need Linux version or extensions for other DuckDB versions, please build manually

## Usage

The extension enables you to query data with **second-level freshness** by automatically combining historical data from the data lake (currently Paimon, with Iceberg support coming soon) with real-time data from Fluss. 

> **New to Fluss?** See the [Quick Start Guide for Paimon](docs/QUICKSTART_PAIMON.md) for a complete setup guide.

After loading the extension, you can use the `fluss_read` table function to read tables with Fluss integration:

```sql
-- Load extension
LOAD 'path/to/fluss.duckdb_extension';

-- Read table with Fluss streaming (currently supports Paimon append-only tables)
-- Syntax: fluss_read(bootstrap_server, database, table)
SELECT * FROM fluss_read('localhost:9092', 'my_database', 'my_table');

-- You can also use it in more complex queries
SELECT 
    column1,
    column2,
    COUNT(*) as count
FROM fluss_read('localhost:9092', 'my_database', 'my_table')
WHERE column1 > 100
GROUP BY column1, column2
ORDER BY count DESC;
```

### Function Parameters

- `bootstrap_server` (string): Fluss bootstrap server address (e.g., `'localhost:9092'`)
- `database` (string): Database name (currently Paimon database, Iceberg namespace coming soon)
- `table` (string): Table name (currently supports Paimon append-only tables only)

### How It Works

The extension implements a two-phase reading strategy to achieve second-level data freshness:

1. **Connection**: The extension connects to Fluss using the bootstrap server
2. **Table Discovery**: Retrieves table metadata from Fluss, including lake storage catalog properties and snapshot information (currently Paimon, Iceberg coming soon)
3. **Phase 1 - Snapshot Reading**: Reads historical data from lake storage up to the snapshot point (minute-level freshness). Currently supports Paimon append-only tables.
4. **Phase 2 - Streaming Reading**: Reads real-time log data from Fluss from snapshot offset to latest offset (second-level freshness)
5. **Unified Result**: Returns all data as a single result set with second-level freshness

This approach ensures that:
- **Historical data** is efficiently read from cost-effective lake storage
- **Real-time data** is streamed from Fluss with minimal latency
- **Complete dataset** includes both layers, providing up-to-the-second data visibility

**Note**: Currently only supports Paimon append-only tables (log tables). If you try to read unsupported table types, you may encounter errors.

## Supported Table Types

### Current Support

The extension currently supports:

- ✅ **Apache Paimon**: 
  - **Append-only tables (Log Tables)**: Fully supported
  - ⚠️ Primary key tables and other table types are not yet supported

### Coming Soon

- 🔜 **Apache Iceberg**: Support for Iceberg tables is in development and will be available in future releases

⚠️ **Not Currently Supported**:
- Paimon primary key tables (with or without Deletion Vector)
- Paimon update tables
- Apache Iceberg tables (coming soon)

## Project Structure

```
duckdb-extension-fluss-lake/
├── rust-ffi/              # Rust FFI wrapper layer
│   ├── Cargo.toml         # Rust dependencies (paimon-rust, fluss)
│   ├── Cargo.lock         # Locked dependency versions
│   └── src/
│       ├── lib.rs         # C ABI interface entry point
│       ├── fluss_ffi.rs   # Fluss FFI bindings
│       └── paimon_ffi.rs  # Paimon FFI bindings
├── src/
│   ├── include/
│   │   ├── fluss_extension.hpp  # Extension header
│   │   └── fluss_ffi.h          # C++ FFI header (generated from Rust)
│   └── fluss_extension.cpp      # DuckDB extension implementation
├── test/
│   └── sql/
│       └── fluss.test     # SQL test cases
├── CMakeLists.txt         # CMake build configuration
├── extension_config.cmake # Extension configuration for DuckDB build system
├── build.sh               # Build script (macOS/Linux)
└── README.md              # This file
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

5. **fluss-rust**: Local dependency (needs to be available at build time)
   - Currently points to a local path (see `rust-ffi/Cargo.toml`)
   - Will be updated to use a Git dependency in the future

## Building from Source

### Prerequisites

1. **Rust toolchain** (1.70+)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **CMake** (3.10+)
   ```bash
   # macOS
   brew install cmake
   
   # Linux (Ubuntu/Debian)
   sudo apt-get install cmake
   ```

3. **DuckDB** (1.4.2+)
    - The extension includes DuckDB source code as a submodule
    - For standalone builds, ensure DuckDB headers are available

### Clone the Project

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

**Important**: The project contains the following submodules:
- `duckdb/` - DuckDB source code (required for building the extension)
- `extension-ci-tools/` - DuckDB extension build tools (required for building and packaging)

If submodules are not properly initialized, the build will fail.

### Update Submodules

If the project is updated, you may need to update submodules:

```bash
git submodule update --remote --recursive
```

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

## Supported Platforms

✅ **macOS** (Apple Silicon and Intel)  
✅ **Linux** (x86_64 and ARM64)

The build system will automatically detect the platform and configure the appropriate linking options.

## Current Status

✅ **Completed**:
- Rust FFI wrapper layer for both Paimon and Fluss
- CMake build configuration with Rust integration
- DuckDB Table Function framework
- Two-phase reading (Paimon snapshot + Fluss streaming)
- Basic type mapping between Arrow and DuckDB types
- Multi-bucket support for partitioned tables
- Automatic phase transition logic
- Support for Paimon append-only tables (log tables)

🔄 **In Progress**:
- Apache Iceberg support
- Performance optimizations
- Error handling improvements

📋 **Planned**:
- Support for Paimon primary key tables
- Support for Paimon update tables
- Write support (writing to Paimon/Fluss/Iceberg)
- Filter pushdown optimizations
- Column pruning optimizations


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

See [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Apache Paimon](https://paimon.apache.org/) - Lake storage format
- [DuckDB](https://duckdb.org/) - Analytical database
- [paimon-rust](https://github.com/apache/paimon-rust) - Rust bindings for Paimon