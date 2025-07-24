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

# Find Clang compilers
find_program(CLANG_C NAMES clang)
find_program(CLANG_CXX NAMES clang++)

if (NOT CLANG_C OR NOT CLANG_CXX)
    message(FATAL_ERROR "Clang compilers not found. FastLanes requires Clang.")
endif()

# Get number of CPU cores for parallel build
include(ProcessorCount)
ProcessorCount(NUM_CORES)
if (NUM_CORES EQUAL 0)
    set(NUM_CORES 1)
endif()

# Build FastLanes separately with Clang using ExternalProject
include(ExternalProject)

ExternalProject_Add(
    fastlanes_external
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fastlanes
    BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build
    CMAKE_ARGS
        -DCMAKE_C_COMPILER=${CLANG_C}
        -DCMAKE_CXX_COMPILER=${CLANG_CXX}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/fastlanes_install
        -DFLS_BUILD_SHARED_LIBS=OFF
        -DCMAKE_CXX_STANDARD=20
        -DCMAKE_CXX_STANDARD_REQUIRED=ON
        -DCMAKE_CXX_FLAGS="-std=c++20 -stdlib=libc++"
        -DCMAKE_C_FLAGS="-std=c17"
    BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${NUM_CORES}
    BUILD_BYPRODUCTS 
        ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a
        ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.so
)

# Set up FastLanes include and library paths
set(FASTLANES_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fastlanes/src/include)

# Create imported target for FastLanes
add_library(FastLanes STATIC IMPORTED)
set_target_properties(FastLanes PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a
    INTERFACE_INCLUDE_DIRECTORIES ${FASTLANES_INCLUDE_DIR}
    INTERFACE_COMPILE_FEATURES cxx_std_20
)

# Add dependency to ensure FastLanes is built before our extension
add_dependencies(FastLanes fastlanes_external)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(${FASTLANES_INCLUDE_DIR})

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
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes)
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
