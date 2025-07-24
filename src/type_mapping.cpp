#include "type_mapping.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace ext_fastlane {

FastLanesDataType TypeMapping::DuckDBToFastLanes(const LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case LogicalTypeId::BOOLEAN:
      return FastLanesDataType::BOOLEAN;
    case LogicalTypeId::TINYINT:
      return FastLanesDataType::INT8;
    case LogicalTypeId::SMALLINT:
      return FastLanesDataType::INT16;
    case LogicalTypeId::INTEGER:
      return FastLanesDataType::INT32;
    case LogicalTypeId::BIGINT:
      return FastLanesDataType::INT64;
    case LogicalTypeId::UTINYINT:
      return FastLanesDataType::UINT8;
    case LogicalTypeId::USMALLINT:
      return FastLanesDataType::UINT16;
    case LogicalTypeId::UINTEGER:
      return FastLanesDataType::UINT32;
    case LogicalTypeId::UBIGINT:
      return FastLanesDataType::UINT64;
    case LogicalTypeId::FLOAT:
      return FastLanesDataType::FLOAT;
    case LogicalTypeId::DOUBLE:
      return FastLanesDataType::DOUBLE;
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::CHAR:
      return FastLanesDataType::STR;
    case LogicalTypeId::DATE:
      return FastLanesDataType::DATE;
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BIT:
      return FastLanesDataType::BYTE_ARRAY;
    case LogicalTypeId::LIST:
      return FastLanesDataType::LIST;
    case LogicalTypeId::STRUCT:
      return FastLanesDataType::STRUCT;
    case LogicalTypeId::MAP:
      return FastLanesDataType::MAP;
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::TIMESTAMP_S:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIME_TZ:
    case LogicalTypeId::TIMESTAMP_TZ:
    case LogicalTypeId::INTERVAL:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UUID:
    case LogicalTypeId::DECIMAL:
    case LogicalTypeId::JSON:
    default:
      return FastLanesDataType::FALLBACK;
  }
}

LogicalType TypeMapping::FastLanesToDuckDB(FastLanesDataType fastlanes_type) {
  switch (fastlanes_type) {
    case FastLanesDataType::BOOLEAN:
      return LogicalType::BOOLEAN;
    case FastLanesDataType::INT8:
      return LogicalType::TINYINT;
    case FastLanesDataType::INT16:
      return LogicalType::SMALLINT;
    case FastLanesDataType::INT32:
      return LogicalType::INTEGER;
    case FastLanesDataType::INT64:
      return LogicalType::BIGINT;
    case FastLanesDataType::UINT8:
      return LogicalType::UTINYINT;
    case FastLanesDataType::UINT16:
      return LogicalType::USMALLINT;
    case FastLanesDataType::UINT32:
      return LogicalType::UINTEGER;
    case FastLanesDataType::UINT64:
      return LogicalType::UBIGINT;
    case FastLanesDataType::FLOAT:
      return LogicalType::FLOAT;
    case FastLanesDataType::DOUBLE:
      return LogicalType::DOUBLE;
    case FastLanesDataType::STR:
      return LogicalType::VARCHAR;
    case FastLanesDataType::DATE:
      return LogicalType::DATE;
    case FastLanesDataType::BYTE_ARRAY:
      return LogicalType::BLOB;
    case FastLanesDataType::LIST:
      return LogicalType::LIST(LogicalType::VARCHAR); // Default to list of strings
    case FastLanesDataType::STRUCT:
      return LogicalType::STRUCT({}); // Empty struct
    case FastLanesDataType::MAP:
      return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR); // Default map
    case FastLanesDataType::INVALID:
    case FastLanesDataType::FALLBACK:
    default:
      return LogicalType::VARCHAR; // Default fallback
  }
}

uint32_t TypeMapping::GetFastLanesTypeSize(FastLanesDataType type) {
  switch (type) {
    case FastLanesDataType::BOOLEAN:
      return sizeof(bool);
    case FastLanesDataType::INT8:
    case FastLanesDataType::UINT8:
      return sizeof(int8_t);
    case FastLanesDataType::INT16:
    case FastLanesDataType::UINT16:
      return sizeof(int16_t);
    case FastLanesDataType::INT32:
    case FastLanesDataType::UINT32:
    case FastLanesDataType::FLOAT:
      return sizeof(int32_t);
    case FastLanesDataType::INT64:
    case FastLanesDataType::UINT64:
    case FastLanesDataType::DOUBLE:
      return sizeof(int64_t);
    case FastLanesDataType::DATE:
      return sizeof(int32_t); // Date is typically stored as days since epoch
    case FastLanesDataType::STR:
    case FastLanesDataType::BYTE_ARRAY:
    case FastLanesDataType::LIST:
    case FastLanesDataType::STRUCT:
    case FastLanesDataType::MAP:
      return sizeof(char*); // Pointer to string/binary data
    case FastLanesDataType::INVALID:
    case FastLanesDataType::FALLBACK:
    default:
      return sizeof(char*); // Default to pointer size
  }
}

bool TypeMapping::IsSupported(const LogicalType& duckdb_type) {
  auto fastlanes_type = DuckDBToFastLanes(duckdb_type);
  return fastlanes_type != FastLanesDataType::INVALID && 
         fastlanes_type != FastLanesDataType::FALLBACK;
}

}  // namespace ext_fastlane
}  // namespace duckdb 