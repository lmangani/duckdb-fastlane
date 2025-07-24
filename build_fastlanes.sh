#!/bin/bash

# Build FastLanes separately using its own build system
set -e

echo "Building FastLanes library..."

# Create build directory
mkdir -p build/fastlanes_build
cd build/fastlanes_build

# Configure and build FastLanes
cmake ../../third_party/fastlanes \
    -DCMAKE_BUILD_TYPE=Release \
    -DFLS_BUILD_SHARED_LIBS=OFF \
    -DCMAKE_INSTALL_PREFIX=../fastlanes_install

# Build
cmake --build . --parallel

echo "FastLanes build complete!"
echo "Library location: build/fastlanes_build/src/libFastLanes.a" 