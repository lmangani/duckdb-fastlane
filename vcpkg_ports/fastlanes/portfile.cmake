vcpkg_buildpath_length_warning(37)

if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

# FastLanes requires Clang, so we need to ensure it's available
if(NOT VCPKG_TARGET_IS_WINDOWS)
    find_program(CLANG_CXX NAMES clang++ clang++-15 clang++-16 clang++-17 clang++-18)
    find_program(CLANG_C NAMES clang clang-15 clang-16 clang-17 clang-18)
    
    if(NOT CLANG_CXX OR NOT CLANG_C)
        message(FATAL_ERROR "FastLanes requires Clang compiler. Please install clang and clang++.")
    endif()
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
# Get the path relative to the vcpkg_ports directory
get_filename_component(VCPKG_PORTS_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(SOURCE_PATH "${VCPKG_PORTS_DIR}/../third_party/fastlanes")

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
        -DFLS_ENABLE_DATA=OFF
        -DFLS_BUILD_TESTING=OFF
        -DFLS_BUILD_BENCHMARKING=OFF
        -DFLS_BUILD_EXAMPLES=OFF
        -DFLS_BUILD_CUDA=OFF
        -DFLS_BUILD_PYTHON=OFF
        -DFLS_ENABLE_FSST_TESTING_AND_BENCHMARKING=OFF
        -DFLS_ENABLE_GALP_TESTING_AND_BENCHMARKING=OFF
        -DCMAKE_C_COMPILER=${CLANG_C}
        -DCMAKE_CXX_COMPILER=${CLANG_CXX}
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