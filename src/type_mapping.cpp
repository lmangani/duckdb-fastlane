#include "type_mapping.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace ext_fastlane {

FastLanesDataType TypeMapping::DuckDBToFastLanes(const LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case LogicalTypeId::BOOLEAN:
      return BOOLEAN;
    case LogicalTypeId::TINYINT:
      return INT8;
    case LogicalTypeId::SMALLINT:
      return INT16;
    case LogicalTypeId::INTEGER:
      return INT32;
    case LogicalTypeId::BIGINT:
      return INT64;
    case LogicalTypeId::UTINYINT:
      return UINT8;
    case LogicalTypeId::USMALLINT:
      return UINT16;
    case LogicalTypeId::UINTEGER:
      return UINT32;
    case LogicalTypeId::UBIGINT:
      return UINT64;
    case LogicalTypeId::FLOAT:
      return FLOAT;
    case LogicalTypeId::DOUBLE:
      return DOUBLE;
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::CHAR:
      return STR;
    case LogicalTypeId::DATE:
      return DATE;
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BIT:
      return BYTE_ARRAY;
    case LogicalTypeId::LIST:
      return LIST;
    case LogicalTypeId::STRUCT:
      return STRUCT;
    case LogicalTypeId::MAP:
      return MAP;
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIME_TZ:
    case LogicalTypeId::TIMESTAMP_TZ:
    case LogicalTypeId::INTERVAL:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::UUID:
    case LogicalTypeId::DECIMAL:
    default:
      return FALLBACK;
  }
}

LogicalType TypeMapping::FastLanesToDuckDB(FastLanesDataType fastlanes_type) {
  switch (fastlanes_type) {
    case BOOLEAN:
      return LogicalType::BOOLEAN;
    case INT8:
      return LogicalType::TINYINT;
    case INT16:
      return LogicalType::SMALLINT;
    case INT32:
      return LogicalType::INTEGER;
    case INT64:
      return LogicalType::BIGINT;
    case UINT8:
      return LogicalType::UTINYINT;
    case UINT16:
      return LogicalType::USMALLINT;
    case UINT32:
      return LogicalType::UINTEGER;
    case UINT64:
      return LogicalType::UBIGINT;
    case FLOAT:
      return LogicalType::FLOAT;
    case DOUBLE:
      return LogicalType::DOUBLE;
    case STR:
      return LogicalType::VARCHAR;
    case DATE:
      return LogicalType::DATE;
    case BYTE_ARRAY:
      return LogicalType::BLOB;
    case LIST:
      return LogicalType::LIST(LogicalType::VARCHAR); // Default to list of strings
    case STRUCT:
      return LogicalType::STRUCT({}); // Empty struct
    case MAP:
      return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR); // Default map
    case INVALID:
    case FALLBACK:
    default:
      return LogicalType::VARCHAR; // Default fallback
  }
}

uint32_t TypeMapping::GetFastLanesTypeSize(FastLanesDataType type) {
  switch (type) {
    case BOOLEAN:
      return sizeof(bool);
    case INT8:
    case UINT8:
      return sizeof(int8_t);
    case INT16:
    case UINT16:
      return sizeof(int16_t);
    case INT32:
    case UINT32:
    case FLOAT:
      return sizeof(int32_t);
    case INT64:
    case UINT64:
    case DOUBLE:
      return sizeof(int64_t);
    case DATE:
      return sizeof(int32_t); // Date is typically stored as days since epoch
    case STR:
    case BYTE_ARRAY:
    case LIST:
    case STRUCT:
    case MAP:
      return sizeof(char*); // Pointer to string/binary data
    case INVALID:
    case FALLBACK:
    default:
      return sizeof(char*); // Default to pointer size
  }
}

bool TypeMapping::IsSupported(const LogicalType& duckdb_type) {
  auto fastlanes_type = DuckDBToFastLanes(duckdb_type);
  return fastlanes_type != INVALID && 
         fastlanes_type != FALLBACK;
}

}  // namespace ext_fastlane
}  // namespace duckdb 