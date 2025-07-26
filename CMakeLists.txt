cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME fastlane)

# Find OpenSSL before building extensions
find_package(OpenSSL REQUIRED)

# Find FastLanes via vcpkg
find_package(FastLanes REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include ${FastLanes_INCLUDE_DIRS})

set(EXTENSION_SOURCES 
    src/fastlane_extension.cpp
    src/scan_fastlanes.cpp
    src/fastlanes_facade.cpp
)

# Common libraries needed for both targets
set(COMMON_LIBS 
    ${FastLanes_LIBRARIES}
    ${OPENSSL_LIBRARIES}
)

# Build extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Set C++20 standard for both targets
set_target_properties(${EXTENSION_NAME} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Link libraries
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COMMON_LIBS})
target_link_libraries(${EXTENSION_NAME} ${COMMON_LIBS})

# Add FastLanes compile definitions
target_compile_definitions(${EXTENSION_NAME} PRIVATE FLS_STATIC)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE FLS_STATIC)

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
