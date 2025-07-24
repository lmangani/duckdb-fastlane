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
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)
include_directories(${FASTLANES_INCLUDE_DIR})

# Separate sources for C++20 facade and standard DuckDB extension
set(FASTLANES_FACADE_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fastlanes_facade.cpp
)

set(EXTENSION_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fastlane_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/scanner/read_fastlane.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/write_fastlane.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/write_fastlane_stream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/type_mapping.cpp
)

# Build the C++20 facade library
add_library(fastlanes_facade STATIC ${FASTLANES_FACADE_SOURCES})
set_target_properties(fastlanes_facade PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_link_libraries(fastlanes_facade FastLanes)
target_compile_definitions(fastlanes_facade PRIVATE FLS_STATIC)

# Build the main extension (standard DuckDB C++ version)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link everything together
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto fastlanes_facade)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto fastlanes_facade)
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
