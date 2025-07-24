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
    BUILD_BYPRODUCTS 
        ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a
        ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.so
)

# Set up FastLanes include and library paths
set(FASTLANES_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fastlanes/src/include)

# Try to find the library in the build directory
if (EXISTS ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a)
    set(FASTLANES_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a)
elseif (EXISTS ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.so)
    set(FASTLANES_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.so)
else()
    message(FATAL_ERROR "FastLanes library not found in build directory")
endif()

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
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${FASTLANES_LIBRARY})
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${FASTLANES_LIBRARY})
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

# Add dependency on FastLanes build
add_dependencies(${EXTENSION_NAME} fastlanes_external)
add_dependencies(${LOADABLE_EXTENSION_NAME} fastlanes_external)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
