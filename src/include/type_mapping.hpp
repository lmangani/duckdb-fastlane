//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// type_mapping.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include <fastlanes.h> // Use vcpkg-provided FastLanes header
#include <data_type.hpp> // Use vcpkg-provided FastLanes data type header

namespace duckdb {

namespace ext_fastlane {

// Use the actual FastLanes data_t enum
using FastLanesDataType = data_t;

class TypeMapping {
public:
  // Convert DuckDB LogicalType to FastLanes data type
  static FastLanesDataType DuckDBToFastLanes(const LogicalType& duckdb_type);
  
  // Convert FastLanes data type to DuckDB LogicalType
  static LogicalType FastLanesToDuckDB(FastLanesDataType fastlanes_type);
  static LogicalType FastLanesToDuckDB(fastlanes::DataType fastlanes_type);
  
  // Get the size in bytes for a FastLanes data type
  static uint32_t GetFastLanesTypeSize(FastLanesDataType type);
  
  // Check if a DuckDB type is supported by FastLanes
  static bool IsSupported(const LogicalType& duckdb_type);
};

}  // namespace ext_fastlane
}  // namespace duckdb 