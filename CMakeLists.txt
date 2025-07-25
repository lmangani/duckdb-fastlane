cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME fastlane)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

# Find FastLanes via vcpkg
find_package(FastLanes REQUIRED)

# Set up FastLanes include and library paths
# FastLanes is now provided by vcpkg via find_package(FastLanes REQUIRED)
message(STATUS "FastLanes_INCLUDE_DIRS: ${FastLanes_INCLUDE_DIRS}")
message(STATUS "FastLanes_LIBRARIES: ${FastLanes_LIBRARIES}")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

# Separate sources for C++20 facade and standard DuckDB extension
set(FASTLANES_FACADE_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fastlanes_facade.cpp
)

set(FASTLANES_CONVERTER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/converter/csv_to_fastlane.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/converter/json_to_fastlane.cpp
)

set(EXTENSION_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/fastlane_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/scanner/read_fastlane.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/write_fastlane.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/writer/write_fastlane_stream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/type_mapping.cpp
    ${FASTLANES_CONVERTER_SOURCES}
)

# Build the C++20 facade library
add_library(fastlanes_facade STATIC ${FASTLANES_FACADE_SOURCES})
set_target_properties(fastlanes_facade PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(fastlanes_facade PRIVATE ${FastLanes_INCLUDE_DIRS})
target_link_libraries(fastlanes_facade ${FastLanes_LIBRARIES})
target_compile_definitions(fastlanes_facade PRIVATE FLS_STATIC)

# Build the main extension (standard DuckDB C++ version)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link everything together
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto fastlanes_facade ${FastLanes_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto fastlanes_facade ${FastLanes_LIBRARIES})

# Add FastLanes include directories to main extension targets
target_include_directories(${EXTENSION_NAME} PRIVATE ${FastLanes_INCLUDE_DIRS})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${FastLanes_INCLUDE_DIRS})

target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
  TARGETS ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME} fastlanes_facade
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
