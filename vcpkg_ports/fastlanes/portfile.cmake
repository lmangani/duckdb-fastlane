vcpkg_buildpath_length_warning(37)

if(VCPKG_TARGET_IS_WINDOWS)
    vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
endif()

# Use local FastLanes submodule
get_filename_component(VCPKG_PORTS_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
set(SOURCE_PATH "${VCPKG_PORTS_DIR}/../third_party/fastlanes")

# Check if the source exists
if(NOT EXISTS "${SOURCE_PATH}")
    message(FATAL_ERROR "FastLanes source not found at ${SOURCE_PATH}. Please ensure the submodule is initialized.")
endif()

# Apply patches directly to the source before configuring
file(READ "${SOURCE_PATH}/CMakeLists.txt" CMAKE_CONTENT)
string(REPLACE 
    "if (NOT \"${CMAKE_CXX_COMPILER_ID}\" MATCHES \"Clang\")"
    "if (NOT \"${CMAKE_CXX_COMPILER_ID}\" MATCHES \"Clang|GNU\")"
    CMAKE_CONTENT "${CMAKE_CONTENT}")
string(REPLACE 
    "message(FATAL_ERROR \"Only Clang is supported!\")"
    "message(FATAL_ERROR \"Only Clang and GCC are supported!\")"
    CMAKE_CONTENT "${CMAKE_CONTENT}")
file(WRITE "${SOURCE_PATH}/CMakeLists.txt" "${CMAKE_CONTENT}")

# Configure FastLanes with minimal options
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
)

vcpkg_install_cmake()

# Copy the static library to the expected location
file(GLOB FASTLANES_LIB "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-rel/src/libFastLanes.a")
if(FASTLANES_LIB)
    file(COPY ${FASTLANES_LIB} DESTINATION "${CURRENT_PACKAGES_DIR}/lib")
    file(COPY ${FASTLANES_LIB} DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib")
else()
    message(WARNING "Could not find libFastLanes.a - build may have failed")
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