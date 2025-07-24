cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME fastlane)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

# Check if we have the FastLanes submodule
if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/fastlanes/CMakeLists.txt")
    message(FATAL_ERROR "FastLanes submodule not found. Please run: git submodule update --init --recursive")
endif()

# Store original compiler settings
set(ORIGINAL_C_COMPILER ${CMAKE_C_COMPILER})
set(ORIGINAL_CXX_COMPILER ${CMAKE_CXX_COMPILER})

# Find Clang compilers
find_program(CLANG_C NAMES clang)
find_program(CLANG_CXX NAMES clang++)

if (NOT CLANG_C OR NOT CLANG_CXX)
    message(FATAL_ERROR "Clang compilers not found. FastLanes requires Clang.")
endif()

# Temporarily switch to Clang for FastLanes build
set(CMAKE_C_COMPILER ${CLANG_C} CACHE STRING "C Compiler" FORCE)
set(CMAKE_CXX_COMPILER ${CLANG_CXX} CACHE STRING "C++ Compiler" FORCE)

# Add FastLanes library from submodule (will use Clang)
add_subdirectory(third_party/fastlanes)
include_directories(third_party/fastlanes/src/include)

# Restore original compiler settings for the rest of the build
set(CMAKE_C_COMPILER ${ORIGINAL_C_COMPILER} CACHE STRING "C Compiler" FORCE)
set(CMAKE_CXX_COMPILER ${ORIGINAL_CXX_COMPILER} CACHE STRING "C++ Compiler" FORCE)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES 
    src/fastlane_extension.cpp
    src/scanner/read_fastlane.cpp
    src/writer/write_fastlane.cpp
    src/writer/write_fastlane_stream.cpp
    src/type_mapping.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL and FastLanes
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes::core)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes::core)
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
