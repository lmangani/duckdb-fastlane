vcpkg_buildpath_length_warning(37)

if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

# FastLanes requires C++20
# Note: FastLanes prefers Clang but can work with GCC in some cases
# We'll allow both compilers but warn if not using Clang
if(NOT VCPKG_TARGET_IS_WINDOWS AND NOT VCPKG_TARGET_IS_OSX)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(WARNING "FastLanes prefers Clang compiler but using ${CMAKE_CXX_COMPILER_ID}. This may cause issues.")
    endif()
endif()

# Set C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use local FastLanes submodule
set(SOURCE_PATH "/usr/src/duckdb-fastlane/third_party/fastlanes")

# Check if the source exists
if(NOT EXISTS "${SOURCE_PATH}")
    message(FATAL_ERROR "FastLanes source not found at ${SOURCE_PATH}. Please ensure the submodule is initialized.")
endif()

# Configure FastLanes
vcpkg_configure_cmake(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DFLS_BUILD_SHARED_LIBS=OFF
        -DCMAKE_CXX_STANDARD=20
        -DCMAKE_CXX_STANDARD_REQUIRED=ON
)

vcpkg_install_cmake()

# Copy the static library to the expected location
file(GLOB FASTLANES_LIB "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-rel/src/libFastLanes.a")
if(FASTLANES_LIB)
    file(COPY ${FASTLANES_LIB} DESTINATION "${CURRENT_PACKAGES_DIR}/lib")
    file(COPY ${FASTLANES_LIB} DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib")
endif()

# Copy headers
file(GLOB_RECURSE FASTLANES_HEADERS "${SOURCE_PATH}/src/include/*.hpp")
if(FASTLANES_HEADERS)
    file(COPY ${FASTLANES_HEADERS} DESTINATION "${CURRENT_PACKAGES_DIR}/include")
endif()

# Create CMake config files
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/FastLanesConfig.cmake" [[
set(FastLanes_FOUND TRUE)
set(FastLanes_INCLUDE_DIRS "${CMAKE_CURRENT_LIST_DIR}/../../include")
set(FastLanes_LIBRARIES "${CMAKE_CURRENT_LIST_DIR}/../../lib/libFastLanes.a")
]])

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright) 