cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME fastlane)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

# Check if FastLanes library exists (must be built separately)
set(FASTLANES_LIB_PATH "${CMAKE_CURRENT_BINARY_DIR}/fastlanes_build/src/libFastLanes.a")
if (NOT EXISTS "${FASTLANES_LIB_PATH}")
    message(FATAL_ERROR "FastLanes library not found at ${FASTLANES_LIB_PATH}. Please run: ./build_fastlanes.sh")
endif()

# Set up FastLanes include and library paths
set(FASTLANES_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fastlanes/src/include)

# Create imported target for FastLanes
add_library(FastLanes STATIC IMPORTED)
set_target_properties(FastLanes PROPERTIES
    IMPORTED_LOCATION ${FASTLANES_LIB_PATH}
    INTERFACE_INCLUDE_DIRECTORIES ${FASTLANES_INCLUDE_DIR}
)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(${FASTLANES_INCLUDE_DIR})

# Extension sources including the C++20 facade
set(EXTENSION_SOURCES 
    src/fastlane_extension.cpp
    src/scanner/read_fastlane.cpp
    src/writer/write_fastlane.cpp
    src/writer/write_fastlane_stream.cpp
    src/type_mapping.cpp
    src/fastlanes_facade.cpp
)

# Build the extension with C++20 support for FastLanes
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Set C++20 for the facade source file specifically
set_source_files_properties(src/fastlanes_facade.cpp PROPERTIES
    COMPILE_FEATURES cxx_std_20
)

# Link everything together
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto FastLanes)
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
