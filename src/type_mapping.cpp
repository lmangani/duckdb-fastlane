#include "type_mapping.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

namespace ext_fastlane {

FastLanesDataType TypeMapping::DuckDBToFastLanes(const LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case LogicalTypeId::BOOLEAN:
      return data_t::BOOLEAN;
    case LogicalTypeId::TINYINT:
      return data_t::INT8;
    case LogicalTypeId::SMALLINT:
      return data_t::INT16;
    case LogicalTypeId::INTEGER:
      return data_t::INT32;
    case LogicalTypeId::BIGINT:
      return data_t::INT64;
    case LogicalTypeId::UTINYINT:
      return data_t::UINT8;
    case LogicalTypeId::USMALLINT:
      return data_t::UINT16;
    case LogicalTypeId::UINTEGER:
      return data_t::UINT32;
    case LogicalTypeId::UBIGINT:
      return data_t::UINT64;
    case LogicalTypeId::FLOAT:
      return data_t::FLOAT;
    case LogicalTypeId::DOUBLE:
      return data_t::DOUBLE;
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::CHAR:
      return data_t::STR;
    case LogicalTypeId::DATE:
      return data_t::DATE;
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BIT:
      return data_t::BYTE_ARRAY;
    case LogicalTypeId::LIST:
      return data_t::LIST;
    case LogicalTypeId::STRUCT:
      return data_t::STRUCT;
    case LogicalTypeId::MAP:
      return data_t::MAP;
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
      return data_t::FALLBACK;
  }
}

LogicalType TypeMapping::FastLanesToDuckDB(FastLanesDataType fastlanes_type) {
  switch (fastlanes_type) {
    case data_t::BOOLEAN:
      return LogicalType::BOOLEAN;
    case data_t::INT8:
      return LogicalType::TINYINT;
    case data_t::INT16:
      return LogicalType::SMALLINT;
    case data_t::INT32:
      return LogicalType::INTEGER;
    case data_t::INT64:
      return LogicalType::BIGINT;
    case data_t::UINT8:
      return LogicalType::UTINYINT;
    case data_t::UINT16:
      return LogicalType::USMALLINT;
    case data_t::UINT32:
      return LogicalType::UINTEGER;
    case data_t::UINT64:
      return LogicalType::UBIGINT;
    case data_t::FLOAT:
      return LogicalType::FLOAT;
    case data_t::DOUBLE:
      return LogicalType::DOUBLE;
    case data_t::STR:
      return LogicalType::VARCHAR;
    case data_t::DATE:
      return LogicalType::DATE;
    case data_t::BYTE_ARRAY:
      return LogicalType::BLOB;
    case data_t::LIST:
      return LogicalType::LIST(LogicalType::VARCHAR); // Default to list of strings
    case data_t::STRUCT:
      return LogicalType::STRUCT({}); // Empty struct
    case data_t::MAP:
      return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR); // Default map
    case data_t::INVALID:
    case data_t::FALLBACK:
    default:
      return LogicalType::VARCHAR; // Default fallback
  }
}

uint32_t TypeMapping::GetFastLanesTypeSize(FastLanesDataType type) {
  switch (type) {
    case data_t::BOOLEAN:
      return sizeof(bool);
    case data_t::INT8:
    case data_t::UINT8:
      return sizeof(int8_t);
    case data_t::INT16:
    case data_t::UINT16:
      return sizeof(int16_t);
    case data_t::INT32:
    case data_t::UINT32:
    case data_t::FLOAT:
      return sizeof(int32_t);
    case data_t::INT64:
    case data_t::UINT64:
    case data_t::DOUBLE:
      return sizeof(int64_t);
    case data_t::DATE:
      return sizeof(int32_t); // Date is typically stored as days since epoch
    case data_t::STR:
    case data_t::BYTE_ARRAY:
    case data_t::LIST:
    case data_t::STRUCT:
    case data_t::MAP:
      return sizeof(char*); // Pointer to string/binary data
    case data_t::INVALID:
    case data_t::FALLBACK:
    default:
      return sizeof(char*); // Default to pointer size
  }
}

bool TypeMapping::IsSupported(const LogicalType& duckdb_type) {
  auto fastlanes_type = DuckDBToFastLanes(duckdb_type);
  return fastlanes_type != data_t::INVALID && 
         fastlanes_type != data_t::FALLBACK;
}

}  // namespace ext_fastlane
}  // namespace duckdb 