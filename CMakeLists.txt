cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME paimon)

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

set(EXTENSION_SOURCES src/paimon_extension.cpp)

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
        set(RUST_LIB_NAME "libpaimon_ffi.a")
    elseif(UNIX)
        set(RUST_LIB_NAME "libpaimon_ffi.a")
    elseif(WIN32)
        set(RUST_LIB_NAME "paimon_ffi.lib")
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
    set(PAIMON_FFI_BUILD_TARGET ${TARGET_NAME}_rust_build)
    
    if(CMAKE_CONFIGURATION_TYPES)
        # Multi-config: build both debug and release
        add_custom_target(${PAIMON_FFI_BUILD_TARGET}
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
        
        add_custom_target(${PAIMON_FFI_BUILD_TARGET}
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
    set(PAIMON_FFI_TARGET_NAME ${TARGET_NAME}_paimon_ffi)
    add_library(${PAIMON_FFI_TARGET_NAME} STATIC IMPORTED)
    
    # Make the imported library depend on the build target
    add_dependencies(${PAIMON_FFI_TARGET_NAME} ${PAIMON_FFI_BUILD_TARGET})
    
    if(CMAKE_CONFIGURATION_TYPES)
        set_target_properties(${PAIMON_FFI_TARGET_NAME} PROPERTIES
            IMPORTED_LOCATION_RELEASE ${RUST_LIB_PATH_RELEASE}
            IMPORTED_LOCATION_DEBUG ${RUST_LIB_PATH_DEBUG}
            IMPORTED_LOCATION_RELWITHDEBINFO ${RUST_LIB_PATH_RELEASE}
        )
    else()
        set_target_properties(${PAIMON_FFI_TARGET_NAME} PROPERTIES
            IMPORTED_LOCATION ${RUST_LIB_PATH}
        )
    endif()
    
    # Set a variable for use in linking
    set(paimon_ffi ${PAIMON_FFI_TARGET_NAME})
    
    # Find Rust standard library (needed for linking)
    execute_process(
        COMMAND ${CARGO} --version
        OUTPUT_VARIABLE CARGO_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    # Try to find Rust sysroot
    execute_process(
        COMMAND rustc --print sysroot
        OUTPUT_VARIABLE RUST_SYSROOT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    
    if(RUST_SYSROOT)
        message(STATUS "Rust sysroot: ${RUST_SYSROOT}")
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
        target_link_libraries(${EXTENSION_NAME} ${PAIMON_FFI_TARGET_NAME})
        target_link_libraries(${LOADABLE_EXTENSION_NAME} ${PAIMON_FFI_TARGET_NAME})
        if(APPLE)
            target_link_libraries(${EXTENSION_NAME}
                "-framework Security"
                "-framework CoreFoundation"
            )
            target_link_libraries(${LOADABLE_EXTENSION_NAME}
                "-framework Security"
                "-framework CoreFoundation"
            )
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
        target_link_libraries(${EXTENSION_NAME} ${PAIMON_FFI_TARGET_NAME})
        # Link Rust standard library and other dependencies
        if(APPLE)
            target_link_libraries(${EXTENSION_NAME} 
                "-framework Security"
                "-framework CoreFoundation"
            )
        endif()
    endif()
    
    # Build loadable extension (simplified version for standalone build)
    add_library(${LOADABLE_EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    if(RUST_BUILD_ENABLED)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} ${PAIMON_FFI_TARGET_NAME})
        # Link Rust standard library and other dependencies
        if(APPLE)
            target_link_libraries(${LOADABLE_EXTENSION_NAME}
                "-framework Security"
                "-framework CoreFoundation"
            )
        endif()
    endif()
    set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
        OUTPUT_NAME ${TARGET_NAME}
        PREFIX ""
        SUFFIX ".duckdb_extension"
    )
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DUCKDB_BUILD_LOADABLE_EXTENSION)
    
    # On macOS, use undefined dynamic lookup for loadable extensions
    if(APPLE)
        set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
            LINK_FLAGS "-undefined dynamic_lookup"
        )
    endif()
endif()
