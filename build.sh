#!/bin/bash

# Build script for DuckDB Fluss Extension
# Supports both macOS and Linux

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Detect platform
if [[ "$OSTYPE" == "darwin"* ]]; then
    PLATFORM="macOS"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    PLATFORM="Linux"
else
    echo -e "${RED}Unsupported platform: $OSTYPE${NC}"
    exit 1
fi

echo -e "${GREEN}Building DuckDB Fluss Extension for ${PLATFORM}${NC}"

# Check for required tools
echo "Checking for required tools..."

# Check CMake
if ! command -v cmake &> /dev/null; then
    echo -e "${RED}Error: cmake is not installed${NC}"
    exit 1
fi
CMAKE_VERSION=$(cmake --version | head -n1)
echo "  ✓ CMake: $CMAKE_VERSION"

# Check Cargo (Rust)
if ! command -v cargo &> /dev/null; then
    echo -e "${YELLOW}Warning: cargo is not installed. Rust FFI library will not be built.${NC}"
    echo "  Install Rust from: https://rustup.rs/"
else
    CARGO_VERSION=$(cargo --version)
    echo "  ✓ Cargo: $CARGO_VERSION"
fi

# Check rustc
if ! command -v rustc &> /dev/null; then
    echo -e "${YELLOW}Warning: rustc is not installed.${NC}"
else
    RUSTC_VERSION=$(rustc --version)
    echo "  ✓ Rustc: $RUSTC_VERSION"
fi

echo ""

# Check if user wants to update Rust dependencies
UPDATE_DEPS=false
if [[ "$1" == "--update-deps" ]] || [[ "$1" == "-u" ]]; then
    UPDATE_DEPS=true
    echo -e "${YELLOW}Will update Rust dependencies before building${NC}"
fi

# Check if submodules are initialized
if [ ! -e "duckdb/.git" ] || [ ! -e "extension-ci-tools/.git" ]; then
    echo -e "${RED}Error: Git submodules are not initialized!${NC}"
    echo -e "${YELLOW}Please run one of the following:${NC}"
    echo "  git submodule update --init --recursive"
    echo "  or clone with: git clone --recurse-submodules <repository-url>"
    exit 1
fi

# Update Rust dependencies if requested
if [ "$UPDATE_DEPS" = true ] && command -v cargo &> /dev/null; then
    echo -e "${GREEN}Updating Rust dependencies...${NC}"
    cd rust-ffi
    cargo update
    cd ..
    echo ""
fi

# Create build directory
BUILD_DIR="build"
if [ -d "$BUILD_DIR" ]; then
    echo -e "${YELLOW}Build directory exists, cleaning...${NC}"
    rm -rf "$BUILD_DIR"
fi
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure with CMake
echo -e "${GREEN}Configuring with CMake...${NC}"
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build
echo -e "${GREEN}Building...${NC}"
cmake --build . --config Release

# Check if build was successful
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Build successful!${NC}"
    echo ""
    echo "Extension files:"
    if [ -f "fluss.duckdb_extension" ]; then
        echo "  - fluss.duckdb_extension (loadable extension)"
        ls -lh fluss.duckdb_extension
    fi
    if [ -f "libfluss_extension.a" ]; then
        echo "  - libfluss_extension.a (static library)"
        ls -lh libfluss_extension.a
    fi
    echo ""
    echo -e "${GREEN}You can now load the extension in DuckDB:${NC}"
    echo "  LOAD 'fluss';"
    echo ""
    echo -e "${YELLOW}Note:${NC} If paimon-rust poc branch was updated, run:"
    echo "  ./build.sh --update-deps"
    echo "  to update dependencies before rebuilding"
else
    echo -e "${RED}✗ Build failed!${NC}"
    exit 1
fi
