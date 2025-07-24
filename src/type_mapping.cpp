#include "type_mapping.hpp"

namespace duckdb {
namespace ext_fastlane {

FastLanesDataType TypeMapping::DuckDBToFastLanes(const LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case LogicalTypeId::BOOLEAN:
      return FLS_BOOLEAN;
    case LogicalTypeId::TINYINT:
      return FLS_INT8;
    case LogicalTypeId::SMALLINT:
      return FLS_INT16;
    case LogicalTypeId::INTEGER:
      return FLS_INT32;
    case LogicalTypeId::BIGINT:
      return FLS_INT64;
    case LogicalTypeId::UTINYINT:
      return FLS_UINT8;
    case LogicalTypeId::USMALLINT:
      return FLS_UINT16;
    case LogicalTypeId::UINTEGER:
      return FLS_UINT32;
    case LogicalTypeId::UBIGINT:
      return FLS_UINT64;
    case LogicalTypeId::FLOAT:
      return FLS_FLOAT;
    case LogicalTypeId::DOUBLE:
      return FLS_DOUBLE;
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::CHAR:
      return FLS_STR;
    case LogicalTypeId::DATE:
      return FLS_DATE;
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BIT:
      return FLS_BYTE_ARRAY;
    case LogicalTypeId::LIST:
      return FLS_LIST;
    case LogicalTypeId::STRUCT:
      return FLS_STRUCT;
    case LogicalTypeId::MAP:
      return FLS_MAP;
    default:
      return FLS_FALLBACK;
  }
}

LogicalType TypeMapping::FastLanesToDuckDB(FastLanesDataType fastlanes_type) {
  switch (fastlanes_type) {
    case FLS_BOOLEAN:
      return LogicalType::BOOLEAN;
    case FLS_INT8:
      return LogicalType::TINYINT;
    case FLS_INT16:
      return LogicalType::SMALLINT;
    case FLS_INT32:
      return LogicalType::INTEGER;
    case FLS_INT64:
      return LogicalType::BIGINT;
    case FLS_UINT8:
      return LogicalType::UTINYINT;
    case FLS_UINT16:
      return LogicalType::USMALLINT;
    case FLS_UINT32:
      return LogicalType::UINTEGER;
    case FLS_UINT64:
      return LogicalType::UBIGINT;
    case FLS_FLOAT:
      return LogicalType::FLOAT;
    case FLS_DOUBLE:
      return LogicalType::DOUBLE;
    case FLS_STR:
      return LogicalType::VARCHAR;
    case FLS_DATE:
      return LogicalType::DATE;
    case FLS_BYTE_ARRAY:
      return LogicalType::BLOB;
    case FLS_LIST:
      return LogicalType::LIST(LogicalType::VARCHAR); // Default to list of strings
    case FLS_STRUCT:
      return LogicalType::STRUCT({}); // Empty struct
    case FLS_MAP:
      return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR); // Default map
    case FLS_INVALID:
    case FLS_FALLBACK:
    default:
      return LogicalType::VARCHAR; // Default to VARCHAR for unsupported types
  }
}

uint32_t TypeMapping::GetFastLanesTypeSize(FastLanesDataType type) {
  switch (type) {
    case FLS_BOOLEAN:
      return 1;
    case FLS_INT8:
    case FLS_UINT8:
      return 1;
    case FLS_INT16:
    case FLS_UINT16:
      return 2;
    case FLS_INT32:
    case FLS_UINT32:
    case FLS_FLOAT:
      return 4;
    case FLS_INT64:
    case FLS_UINT64:
    case FLS_DOUBLE:
      return 8;
    case FLS_DATE:
      return 4; // Assuming DATE is stored as 32-bit integer
    case FLS_STR:
    case FLS_BYTE_ARRAY:
    case FLS_LIST:
    case FLS_STRUCT:
    case FLS_MAP:
    case FLS_INVALID:
    case FLS_FALLBACK:
    default:
      return 0; // Variable size types
  }
}

bool TypeMapping::IsSupported(FastLanesDataType type) {
  switch (type) {
    case FLS_BOOLEAN:
    case FLS_INT8:
    case FLS_INT16:
    case FLS_INT32:
    case FLS_INT64:
    case FLS_UINT8:
    case FLS_UINT16:
    case FLS_UINT32:
    case FLS_UINT64:
    case FLS_FLOAT:
    case FLS_DOUBLE:
    case FLS_STR:
    case FLS_DATE:
    case FLS_BYTE_ARRAY:
      return true;
    case FLS_LIST:
    case FLS_STRUCT:
    case FLS_MAP:
    case FLS_INVALID:
    case FLS_FALLBACK:
    default:
      return false;
  }
}

}  // namespace ext_fastlane
}  // namespace duckdb 