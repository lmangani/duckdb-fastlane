//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// type_mapping.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "fastlanes.h"

namespace duckdb {
namespace ext_fastlane {

// FastLanes data types from fastlanes.h
enum FastLanesDataType : uint8_t {
  FLS_INVALID    = 0,
  FLS_DOUBLE     = 1,
  FLS_INT8       = 2,
  FLS_INT16      = 3,
  FLS_INT32      = 4,
  FLS_INT64      = 5,
  FLS_UINT8      = 6,
  FLS_UINT16     = 7,
  FLS_UINT32     = 8,
  FLS_UINT64     = 9,
  FLS_STR        = 10,
  FLS_BOOLEAN    = 11,
  FLS_DATE       = 12,
  FLS_FLOAT      = 13,
  FLS_BYTE_ARRAY = 14,
  FLS_LIST       = 15,
  FLS_STRUCT     = 16,
  FLS_MAP        = 17,
  FLS_FALLBACK   = 18,
};

class TypeMapping {
public:
  // Convert DuckDB LogicalType to FastLanes data type
  static FastLanesDataType DuckDBToFastLanes(const LogicalType& duckdb_type);
  
  // Convert FastLanes data type to DuckDB LogicalType
  static LogicalType FastLanesToDuckDB(FastLanesDataType fastlanes_type);
  
  // Get the size in bytes for a FastLanes data type
  static uint32_t GetFastLanesTypeSize(FastLanesDataType type);
  
  // Check if a FastLanes data type is supported
  static bool IsSupported(FastLanesDataType type);
};

}  // namespace ext_fastlane
}  // namespace duckdb 