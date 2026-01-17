cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME fluss)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME} CXX)
include_directories(src/include)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_SOURCES src/fluss_extension.cpp)

# Find Rust compiler
find_program(CARGO cargo)
if(NOT CARGO)
    message(WARNING "Cargo not found. Rust FFI library will not be built.")
    set(RUST_BUILD_ENABLED FALSE)
else()
    set(RUST_BUILD_ENABLED TRUE)
    message(STATUS "Found Cargo: ${CARGO}")
endif()

# Build Rust FFI library
if(RUST_BUILD_ENABLED)
    set(RUST_FFI_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust-ffi)
    # Use binary directory relative to extension source directory for Rust target
    # This ensures it works both as standalone and as DuckDB extension
    set(RUST_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/rust-target)
    
    # Determine build type - use generator expression for multi-config generators
    if(CMAKE_CONFIGURATION_TYPES)
        # Multi-config generator (Xcode, Visual Studio)
        set(RUST_BUILD_TYPE "$<$<CONFIG:Debug>:debug>$<$<CONFIG:Release>:release>$<$<CONFIG:RelWithDebInfo>:release>")
    else()
        # Single-config generator (Unix Makefiles, Ninja)
        # Default to release if CMAKE_BUILD_TYPE is not set or empty
        if(NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
            set(RUST_BUILD_TYPE "release")
        elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
            set(RUST_BUILD_TYPE "debug")
        else()
            set(RUST_BUILD_TYPE "release")
        endif()
    endif()
    
    # Set library name based on platform
    if(APPLE)
        set(RUST_LIB_NAME "libfluss_ffi.a")
    elseif(UNIX)
        set(RUST_LIB_NAME "libfluss_ffi.a")
    elseif(WIN32)
        set(RUST_LIB_NAME "fluss_ffi.lib")
    endif()
    
    # For single-config, use direct path; for multi-config, we'll handle it in the custom command
    if(CMAKE_CONFIGURATION_TYPES)
        set(RUST_LIB_PATH_DEBUG ${RUST_TARGET_DIR}/debug/${RUST_LIB_NAME})
        set(RUST_LIB_PATH_RELEASE ${RUST_TARGET_DIR}/release/${RUST_LIB_NAME})
        set(RUST_LIB_PATH ${RUST_LIB_PATH_RELEASE})  # Default for IMPORTED_LOCATION
    else()
        set(RUST_LIB_PATH ${RUST_TARGET_DIR}/${RUST_BUILD_TYPE}/${RUST_LIB_NAME})
    endif()
    
    # Create a custom target for building Rust library
    set(FLUSS_FFI_BUILD_TARGET ${TARGET_NAME}_rust_build)
    
    if(CMAKE_CONFIGURATION_TYPES)
        # Multi-config: build both debug and release
        add_custom_target(${FLUSS_FFI_BUILD_TARGET}
            COMMAND ${CARGO} build --manifest-path ${RUST_FFI_DIR}/Cargo.toml
                    --target-dir ${RUST_TARGET_DIR}
            COMMAND ${CARGO} build --manifest-path ${RUST_FFI_DIR}/Cargo.toml
                    --target-dir ${RUST_TARGET_DIR} --release
            WORKING_DIRECTORY ${RUST_FFI_DIR}
            COMMENT "Building Rust FFI library (debug and release)"
            VERBATIM
        )
    else()
        # Single-config: build based on CMAKE_BUILD_TYPE
        # Determine if we need --release flag
        # Default to release build if CMAKE_BUILD_TYPE is not set
        if(NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "" OR 
           CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
            set(CARGO_RELEASE_FLAG "--release")
        else()
            set(CARGO_RELEASE_FLAG "")
        endif()
        
        add_custom_target(${FLUSS_FFI_BUILD_TARGET}
            COMMAND ${CARGO} build --manifest-path ${RUST_FFI_DIR}/Cargo.toml
                    --target-dir ${RUST_TARGET_DIR}
                    ${CARGO_RELEASE_FLAG}
            WORKING_DIRECTORY ${RUST_FFI_DIR}
            COMMENT "Building Rust FFI library (${RUST_BUILD_TYPE})"
            VERBATIM
        )
    endif()
    
    # Create imported library target with a unique name to avoid conflicts
    # Use a name that's scoped to this extension
    set(FLUSS_FFI_TARGET_NAME ${TARGET_NAME}_fluss_ffi)
    add_library(${FLUSS_FFI_TARGET_NAME} STATIC IMPORTED)
    
    # Make the imported library depend on the build target
    add_dependencies(${FLUSS_FFI_TARGET_NAME} ${FLUSS_FFI_BUILD_TARGET})
    
    if(CMAKE_CONFIGURATION_TYPES)
        set_target_properties(${FLUSS_FFI_TARGET_NAME} PROPERTIES
            IMPORTED_LOCATION_RELEASE ${RUST_LIB_PATH_RELEASE}
            IMPORTED_LOCATION_DEBUG ${RUST_LIB_PATH_DEBUG}
            IMPORTED_LOCATION_RELWITHDEBINFO ${RUST_LIB_PATH_RELEASE}
        )
    else()
        set_target_properties(${FLUSS_FFI_TARGET_NAME} PROPERTIES
            IMPORTED_LOCATION ${RUST_LIB_PATH}
        )
    endif()
    
    # Set a variable for use in linking
    set(fluss_ffi ${FLUSS_FFI_TARGET_NAME})
    
    # Find Rust standard library (needed for linking)
    execute_process(
        COMMAND ${CARGO} --version
        OUTPUT_VARIABLE CARGO_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    # Try to find Rust sysroot and standard library
    execute_process(
        COMMAND rustc --print sysroot
        OUTPUT_VARIABLE RUST_SYSROOT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    
    if(RUST_SYSROOT)
        message(STATUS "Rust sysroot: ${RUST_SYSROOT}")
        
        # Find Rust target triple
        execute_process(
            COMMAND rustc -vV
            OUTPUT_VARIABLE RUSTC_VERSION_OUTPUT
            ERROR_QUIET
        )
        
        # Extract host target from rustc output
        string(REGEX MATCH "host: ([^ ]+)" _ "${RUSTC_VERSION_OUTPUT}")
        if(CMAKE_MATCH_1)
            set(RUST_TARGET_TRIPLE ${CMAKE_MATCH_1})
            message(STATUS "Rust target triple: ${RUST_TARGET_TRIPLE}")
        endif()
        
        # Set Rust library search paths for Linux
        if(UNIX AND NOT APPLE)
            # On Linux, we may need to link against Rust standard library
            # The static library from Rust should be self-contained, but we'll set up paths just in case
            set(RUST_LIB_DIR "${RUST_SYSROOT}/lib/rustlib/${RUST_TARGET_TRIPLE}/lib")
            if(EXISTS "${RUST_LIB_DIR}")
                message(STATUS "Found Rust lib directory: ${RUST_LIB_DIR}")
            endif()
        endif()
    endif()
endif()

# Check if we're building as part of DuckDB's main project
# This is true when build_static_extension function is available
if(COMMAND build_static_extension)
    # Building as part of DuckDB main project
    build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
    build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
    
    # Link OpenSSL in both the static library as the loadable extension
    target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    
    # Link Rust FFI library if available
    if(RUST_BUILD_ENABLED)
        target_link_libraries(${EXTENSION_NAME} ${FLUSS_FFI_TARGET_NAME})
        target_link_libraries(${LOADABLE_EXTENSION_NAME} ${FLUSS_FFI_TARGET_NAME})
        
        # Platform-specific linking for Rust dependencies
        if(APPLE)
            # macOS requires Security and CoreFoundation frameworks for some Rust crates
            target_link_libraries(${EXTENSION_NAME}
                "-framework Security"
                "-framework CoreFoundation"
            )
            target_link_libraries(${LOADABLE_EXTENSION_NAME}
                "-framework Security"
                "-framework CoreFoundation"
            )
        elseif(UNIX AND NOT APPLE)
            # Linux: Rust static libraries should be self-contained, but may need pthread
            # Most modern Rust staticlibs include everything needed
            # If linking issues occur, you may need to add: -lpthread -ldl -lm
            # For now, we rely on the static library being self-contained
            message(STATUS "Linking Rust FFI library for Linux")
        endif()
    endif()
    
    install(
      TARGETS ${EXTENSION_NAME}
      EXPORT "${DUCKDB_EXPORT_SET}"
      LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
      ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
else()
    # Standalone build for CLion/IDE development
    # Note: This requires DuckDB headers to be available
    # You can set DUCKDB_DIR to point to DuckDB source directory if needed
    
    # Try to find DuckDB headers
    if(NOT DEFINED DUCKDB_DIR)
        # Try common locations
        if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/duckdb")
            set(DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/duckdb")
        elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb")
            set(DUCKDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb")
        endif()
    endif()
    
    if(EXISTS "${DUCKDB_DIR}")
        message(STATUS "Found DuckDB at: ${DUCKDB_DIR}")
        include_directories("${DUCKDB_DIR}/src/include")
    else()
        message(WARNING "DuckDB headers not found. Please set DUCKDB_DIR to DuckDB source directory.")
        message(WARNING "For standalone development, you may need to:")
        message(WARNING "  1. Clone DuckDB repository")
        message(WARNING "  2. Set DUCKDB_DIR CMake variable to DuckDB source path")
    endif()
    
    # Build static extension library (for development/testing)
    add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
    target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    if(RUST_BUILD_ENABLED)
        target_link_libraries(${EXTENSION_NAME} ${FLUSS_FFI_TARGET_NAME})
        # Platform-specific linking for Rust dependencies
        if(APPLE)
            target_link_libraries(${EXTENSION_NAME} 
                "-framework Security"
                "-framework CoreFoundation"
            )
        elseif(UNIX AND NOT APPLE)
            # Linux: Rust static libraries should be self-contained
            message(STATUS "Linking Rust FFI library for Linux (static)")
        endif()
    endif()
    
    # Build loadable extension (simplified version for standalone build)
    # First build as a shared library without .duckdb_extension suffix
    add_library(${LOADABLE_EXTENSION_NAME}_raw SHARED ${EXTENSION_SOURCES})
    target_link_libraries(${LOADABLE_EXTENSION_NAME}_raw OpenSSL::SSL OpenSSL::Crypto)
    if(RUST_BUILD_ENABLED)
        target_link_libraries(${LOADABLE_EXTENSION_NAME}_raw ${FLUSS_FFI_TARGET_NAME})
        # Platform-specific linking for Rust dependencies
        if(APPLE)
            target_link_libraries(${LOADABLE_EXTENSION_NAME}_raw
                "-framework Security"
                "-framework CoreFoundation"
            )
        elseif(UNIX AND NOT APPLE)
            # Linux: Rust static libraries should be self-contained
            message(STATUS "Linking Rust FFI library for Linux (shared)")
        endif()
    endif()
    
    # Set output name based on platform
    if(APPLE)
        set(RAW_LIB_NAME "lib${TARGET_NAME}.dylib")
    elseif(UNIX)
        set(RAW_LIB_NAME "lib${TARGET_NAME}.so")
    else()
        set(RAW_LIB_NAME "${TARGET_NAME}.dll")
    endif()
    
    set_target_properties(${LOADABLE_EXTENSION_NAME}_raw PROPERTIES
        OUTPUT_NAME ${TARGET_NAME}
        PREFIX "lib"
    )
    target_compile_definitions(${LOADABLE_EXTENSION_NAME}_raw PRIVATE DUCKDB_BUILD_LOADABLE_EXTENSION)
    
    # On macOS, use undefined dynamic lookup for loadable extensions
    if(APPLE)
        set_target_properties(${LOADABLE_EXTENSION_NAME}_raw PROPERTIES
            LINK_FLAGS "-undefined dynamic_lookup"
        )
    endif()
    
    # Find Python for metadata script
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(NOT Python3_FOUND)
        find_program(PYTHON3 python3 python)
    else()
        set(PYTHON3 ${Python3_EXECUTABLE})
    endif()
    
    
    # Get extension version from git
    find_package(Git QUIET)
    if(GIT_FOUND)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} tag --points-at HEAD
            OUTPUT_VARIABLE GIT_TAG
            OUTPUT_STRIP_TRAILING_WHITESPACE
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            ERROR_QUIET
        )
        if(GIT_TAG)
            set(EXTENSION_VERSION ${GIT_TAG})
        else()
            execute_process(
                COMMAND ${GIT_EXECUTABLE} --no-pager log -1 --format=%h
                OUTPUT_VARIABLE GIT_HASH
                OUTPUT_STRIP_TRAILING_WHITESPACE
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                ERROR_QUIET
            )
            if(GIT_HASH)
                set(EXTENSION_VERSION ${GIT_HASH})
            else()
                set(EXTENSION_VERSION "dev")
            endif()
        endif()
    else()
        set(EXTENSION_VERSION "dev")
    endif()
    
    # Detect platform using Python script or fallback
    set(PLATFORM_DETECTED FALSE)
    if(PYTHON3)
        # Try to detect platform using DuckDB Python package
        execute_process(
            COMMAND ${PYTHON3} -c "import duckdb; print(duckdb.execute('pragma platform').fetchone()[0])"
            OUTPUT_VARIABLE DUCKDB_PLATFORM
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(DUCKDB_PLATFORM)
            set(PLATFORM_DETECTED TRUE)
        endif()
    endif()
    
    # Fallback platform detection based on CMake variables
    if(NOT PLATFORM_DETECTED)
        if(APPLE)
            if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
                set(DUCKDB_PLATFORM "osx_arm64")
            else()
                set(DUCKDB_PLATFORM "osx_amd64")
            endif()
        elseif(UNIX)
            if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
                set(DUCKDB_PLATFORM "linux_arm64")
            else()
                set(DUCKDB_PLATFORM "linux_amd64")
            endif()
        else()
            set(DUCKDB_PLATFORM "windows_amd64")
        endif()
    endif()
    
    # Detect DuckDB version for CPP ABI (must match exactly)
    # Try multiple methods to detect the actual DuckDB version being used
    if(NOT DEFINED DUCKDB_VERSION)
        set(DUCKDB_VERSION_DETECTED FALSE)
        
        # Method 1: Try to get from Python DuckDB package
        if(PYTHON3)
            execute_process(
                COMMAND ${PYTHON3} -c "import duckdb; conn = duckdb.connect(); result = conn.execute('SELECT library_version FROM pragma_version()').fetchone(); print(result[0] if result else '')"
                OUTPUT_VARIABLE DUCKDB_LIB_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET
            )
            if(DUCKDB_LIB_VERSION AND NOT DUCKDB_LIB_VERSION STREQUAL "")
                set(DUCKDB_VERSION ${DUCKDB_LIB_VERSION})
                set(DUCKDB_VERSION_DETECTED TRUE)
                message(STATUS "Detected DuckDB version from Python: ${DUCKDB_VERSION}")
            endif()
        endif()
        
        # Method 2: Try to get from DuckDB source directory if available
        if(NOT DUCKDB_VERSION_DETECTED AND EXISTS "${DUCKDB_DIR}")
            # Try to read version from DuckDB's version file or git
            find_package(Git QUIET)
            if(GIT_FOUND)
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} describe --tags --exact-match HEAD
                    OUTPUT_VARIABLE DUCKDB_GIT_TAG
                    OUTPUT_STRIP_TRAILING_WHITESPACE
                    WORKING_DIRECTORY ${DUCKDB_DIR}
                    ERROR_QUIET
                )
                if(DUCKDB_GIT_TAG AND NOT DUCKDB_GIT_TAG STREQUAL "")
                    set(DUCKDB_VERSION ${DUCKDB_GIT_TAG})
                    set(DUCKDB_VERSION_DETECTED TRUE)
                    message(STATUS "Detected DuckDB version from git tag: ${DUCKDB_VERSION}")
                else()
                    # Try to get version from git describe
                    execute_process(
                        COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=0
                        OUTPUT_VARIABLE DUCKDB_GIT_TAG
                        OUTPUT_STRIP_TRAILING_WHITESPACE
                        WORKING_DIRECTORY ${DUCKDB_DIR}
                        ERROR_QUIET
                    )
                    if(DUCKDB_GIT_TAG AND NOT DUCKDB_GIT_TAG STREQUAL "")
                        set(DUCKDB_VERSION ${DUCKDB_GIT_TAG})
                        set(DUCKDB_VERSION_DETECTED TRUE)
                        message(STATUS "Detected DuckDB version from git describe: ${DUCKDB_VERSION}")
                    endif()
                endif()
            endif()
        endif()
        
        # Default DuckDB version if not detected
        # Note: For CPP ABI, version must match exactly with the DuckDB version you're using
        if(NOT DUCKDB_VERSION_DETECTED)
            # Try to get version from installed DuckDB if available
            # This is a fallback - user should ideally specify the version
            set(DUCKDB_VERSION "v1.4.2" CACHE STRING "DuckDB version for extension metadata (for CPP ABI)")
            message(WARNING "Could not auto-detect DuckDB version!")
            message(WARNING "  Using default: ${DUCKDB_VERSION}")
            message(WARNING "  If this doesn't match your DuckDB version, specify with:")
            message(WARNING "    cmake .. -DDUCKDB_VERSION=v1.4.2")
            message(WARNING "  Or check your DuckDB version with: python3 -c 'import duckdb; print(duckdb.__version__)'")
        endif()
    else()
        set(DUCKDB_VERSION ${DUCKDB_VERSION} CACHE STRING "DuckDB version for extension metadata (for CPP ABI)")
        message(STATUS "Using manually specified DuckDB version: ${DUCKDB_VERSION}")
    endif()
    
    message(STATUS "DuckDB Platform: ${DUCKDB_PLATFORM}")
    message(STATUS "DuckDB Version: ${DUCKDB_VERSION}")
    message(STATUS "Extension Version: ${EXTENSION_VERSION}")
    
    # Create the final extension file with metadata
    set(EXTENSION_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_NAME}.duckdb_extension)
    
    if(PYTHON3)
        # Use the append_extension_metadata.py script
        # Use generator expression to get the actual file path
        # Since we're using DUCKDB_CPP_EXTENSION_ENTRY, we need CPP ABI type
        # For CPP ABI, -dv should be the DuckDB version (not C API version)
        
        add_custom_command(
            OUTPUT ${EXTENSION_OUTPUT}
            DEPENDS ${LOADABLE_EXTENSION_NAME}_raw
            COMMAND ${PYTHON3} ${CMAKE_CURRENT_SOURCE_DIR}/extension-ci-tools/scripts/append_extension_metadata.py
                -l $<TARGET_FILE:${LOADABLE_EXTENSION_NAME}_raw>
                -o ${EXTENSION_OUTPUT}
                -n ${TARGET_NAME}
                -dv ${DUCKDB_VERSION}
                -ev ${EXTENSION_VERSION}
                -p ${DUCKDB_PLATFORM}
                --abi-type CPP
            COMMENT "Appending metadata to extension file (DuckDB version: ${DUCKDB_VERSION}, ABI: CPP)"
            VERBATIM
        )
    else()
        # Fallback: just copy the library (will not work, but at least builds)
        message(WARNING "Python not found. Extension will be built without metadata (may not load correctly).")
        add_custom_command(
            OUTPUT ${EXTENSION_OUTPUT}
            DEPENDS ${LOADABLE_EXTENSION_NAME}_raw
            COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${LOADABLE_EXTENSION_NAME}_raw> ${EXTENSION_OUTPUT}
            COMMENT "Copying extension file (without metadata - may not work)"
            VERBATIM
        )
    endif()
    
    # Create a custom target for the extension with metadata
    add_custom_target(${LOADABLE_EXTENSION_NAME} ALL DEPENDS ${EXTENSION_OUTPUT})
endif()
