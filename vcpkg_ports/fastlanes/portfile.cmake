vcpkg_buildpath_length_warning(37)

if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

# FastLanes REQUIRES Clang >= 13 on all platforms
# Find Clang compiler
find_program(CLANG_CXX NAMES clang++ clang++-15 clang++-16 clang++-17 clang++-18 clang++-19)
find_program(CLANG_C NAMES clang clang-15 clang-16 clang-17 clang-18 clang-19)

if(NOT CLANG_CXX OR NOT CLANG_C)
    message(FATAL_ERROR "FastLanes requires Clang compiler >= 13. Please install clang and clang++.")
endif()

# Check Clang version
execute_process(
    COMMAND ${CLANG_CXX} --version
    OUTPUT_VARIABLE CLANG_VERSION_OUTPUT
    ERROR_VARIABLE CLANG_VERSION_ERROR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Extract version number (simplified check)
if(CLANG_VERSION_OUTPUT MATCHES "clang version ([0-9]+)")
    set(CLANG_VERSION_MAJOR ${CMAKE_MATCH_1})
    if(CLANG_VERSION_MAJOR LESS 13)
        message(FATAL_ERROR "FastLanes requires Clang >= 13, but found version ${CLANG_VERSION_MAJOR}")
    endif()
    message(STATUS "Found Clang version ${CLANG_VERSION_MAJOR}")
else()
    message(WARNING "Could not determine Clang version, proceeding anyway")
endif()

# Use local FastLanes submodule
# Get the path relative to the vcpkg_ports directory
get_filename_component(VCPKG_PORTS_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(SOURCE_PATH "${VCPKG_PORTS_DIR}/../third_party/fastlanes")

# Check if the source exists
if(NOT EXISTS "${SOURCE_PATH}")
    message(FATAL_ERROR "FastLanes source not found at ${SOURCE_PATH}. Please ensure the submodule is initialized.")
endif()

# Configure FastLanes with platform-specific options
set(FASTLANES_OPTIONS
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

# Add macOS-specific options
if(VCPKG_TARGET_IS_OSX)
    list(APPEND FASTLANES_OPTIONS
        -DCMAKE_OSX_DEPLOYMENT_TARGET=10.15
        -DCMAKE_OSX_ARCHITECTURES=x86_64
    )
endif()

# Configure FastLanes
vcpkg_configure_cmake(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS ${FASTLANES_OPTIONS}
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