#include "writer/write_fastlane.hpp"
#include "type_mapping.hpp"
#include "fastlanes_facade.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace ext_fastlane {

// Forward declarations
void SerializeColumnData(const Vector& vector, 
                        const LogicalType& logical_type,
                        FastLanesDataType fastlanes_type,
                        uint8_t* buffer_ptr,
                        idx_t row_count);

struct WriteFastlaneFunctionData : public TableFunctionData {
  WriteFastlaneFunctionData() = default;
  vector<LogicalType> logical_types;
  vector<string> column_names;
  const idx_t chunk_size = WriteFastlaneFunction::DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
};

struct WriteFastlaneGlobalState : public GlobalTableFunctionState {
  WriteFastlaneGlobalState() : sent_schema(false) {}
  atomic<bool> sent_schema;
  mutex lock;
  unique_ptr<FastLanesFacade> facade;
};

struct WriteFastlaneLocalState : public LocalTableFunctionState {
  unique_ptr<FastLanesFacade> facade;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState> WriteFastlaneFunction::InitLocal(
    ExecutionContext& context, TableFunctionInitInput& input,
    GlobalTableFunctionState* global_state) {
  auto local_state = make_uniq<WriteFastlaneLocalState>();
  local_state->facade = make_uniq<FastLanesFacade>();
  return local_state;
}

unique_ptr<GlobalTableFunctionState> WriteFastlaneFunction::InitGlobal(
    ClientContext& context, TableFunctionInitInput& input) {
  auto result = make_uniq<WriteFastlaneGlobalState>();
  result->facade = make_uniq<FastLanesFacade>();
  return result;
}

unique_ptr<FunctionData> WriteFastlaneFunction::Bind(ClientContext& context,
                                                    TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types,
                                                    vector<string>& names) {
  auto result = make_uniq<WriteFastlaneFunctionData>();

  // Set return schema - return the FastLanes data as BLOB
  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("fastlane_data");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("is_header");

  result->logical_types = input.input_table_types;
  result->column_names = input.input_table_names;
  return std::move(result);
}

void SerializeFastlaneData(const WriteFastlaneLocalState& local_state,
                          const DataChunk& input_chunk,
                          const vector<LogicalType>& logical_types,
                          const vector<string>& column_names,
                          std::unique_ptr<uint8_t[]>& fastlane_buffer,
                          size_t& buffer_size) {
  // Calculate required buffer size
  size_t total_size = 0;
  vector<size_t> column_sizes;
  
  // Header: magic bytes (8) + version (8) + column count (4) + column metadata
  total_size = 20; // Base header size
  
  // Calculate size for each column
  for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
    auto& vector = input_chunk.data[col_idx];
    auto& logical_type = logical_types[col_idx];
    
    if (TypeMapping::IsSupported(logical_type)) {
      auto fastlanes_type = TypeMapping::DuckDBToFastLanes(logical_type);
      size_t type_size = TypeMapping::GetFastLanesTypeSize(fastlanes_type);
      size_t column_size = input_chunk.size() * type_size;
      
      // Add column name length and name
      total_size += sizeof(uint32_t); // name length
      total_size += column_names[col_idx].length();
      
      // Add column type and size
      total_size += sizeof(uint8_t); // data type
      total_size += sizeof(uint32_t); // column size
      total_size += column_size; // actual data
      
      column_sizes.push_back(column_size);
    } else {
      column_sizes.push_back(0);
    }
  }
  
  // Allocate buffer
  buffer_size = total_size;
  fastlane_buffer = std::make_unique<uint8_t[]>(buffer_size);
  uint8_t* buffer_ptr = fastlane_buffer.get();
  
  // Write FastLanes magic bytes
  uint64_t magic_bytes = 0x656E614C74736146ULL; // "FastLane" in little-endian
  memcpy(buffer_ptr, &magic_bytes, sizeof(magic_bytes));
  buffer_ptr += sizeof(magic_bytes);
  
  // Write version
  uint64_t version = 0x0000342E312E3076ULL; // "v0.1.4" in little-endian
  memcpy(buffer_ptr, &version, sizeof(version));
  buffer_ptr += sizeof(version);
  
  // Write column count
  uint32_t column_count = input_chunk.ColumnCount();
  memcpy(buffer_ptr, &column_count, sizeof(column_count));
  buffer_ptr += sizeof(column_count);
  
  // Write column metadata and data
  for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
    auto& vector = input_chunk.data[col_idx];
    auto& logical_type = logical_types[col_idx];
    auto& column_name = column_names[col_idx];
    
    if (TypeMapping::IsSupported(logical_type)) {
      auto fastlanes_type = TypeMapping::DuckDBToFastLanes(logical_type);
      
      // Write column name length and name
      uint32_t name_length = column_name.length();
      memcpy(buffer_ptr, &name_length, sizeof(name_length));
      buffer_ptr += sizeof(name_length);
      memcpy(buffer_ptr, column_name.c_str(), name_length);
      buffer_ptr += name_length;
      
      // Write data type
      uint8_t data_type = static_cast<uint8_t>(fastlanes_type);
      memcpy(buffer_ptr, &data_type, sizeof(data_type));
      buffer_ptr += sizeof(data_type);
      
      // Write column size
      uint32_t column_size = column_sizes[col_idx];
      memcpy(buffer_ptr, &column_size, sizeof(column_size));
      buffer_ptr += sizeof(column_size);
      
      // Write actual data
      SerializeColumnData(vector, logical_type, fastlanes_type, buffer_ptr, input_chunk.size());
      buffer_ptr += column_size;
    }
  }
}

void SerializeColumnData(const Vector& vector, 
                        const LogicalType& logical_type,
                        FastLanesDataType fastlanes_type,
                        uint8_t* buffer_ptr,
                        idx_t row_count) {
  // Convert DuckDB data to FastLanes format
  switch (fastlanes_type) {
    case BOOLEAN: {
      auto bool_data = reinterpret_cast<bool*>(buffer_ptr);
      for (idx_t i = 0; i < row_count; i++) {
        auto value = vector.GetValue(i);
        bool_data[i] = value.GetValue<bool>();
      }
      break;
    }
    case INT32: {
      auto int_data = reinterpret_cast<int32_t*>(buffer_ptr);
      for (idx_t i = 0; i < row_count; i++) {
        auto value = vector.GetValue(i);
        int_data[i] = value.GetValue<int32_t>();
      }
      break;
    }
    case INT64: {
      auto int_data = reinterpret_cast<int64_t*>(buffer_ptr);
      for (idx_t i = 0; i < row_count; i++) {
        auto value = vector.GetValue(i);
        int_data[i] = value.GetValue<int64_t>();
      }
      break;
    }
    case DOUBLE: {
      auto double_data = reinterpret_cast<double*>(buffer_ptr);
      for (idx_t i = 0; i < row_count; i++) {
        auto value = vector.GetValue(i);
        double_data[i] = value.GetValue<double>();
      }
      break;
    }
    case STR: {
      auto str_data = reinterpret_cast<char**>(buffer_ptr);
      for (idx_t i = 0; i < row_count; i++) {
        auto value = vector.GetValue(i);
        if (value.IsNull()) {
          str_data[i] = nullptr;
        } else {
          auto str_val = value.GetValue<string>();
          // Allocate memory for string (in a real implementation, you'd use a proper string pool)
          str_data[i] = strdup(str_val.c_str());
        }
      }
      break;
    }
    default:
      // For unsupported types, fill with zeros
      memset(buffer_ptr, 0, row_count * TypeMapping::GetFastLanesTypeSize(fastlanes_type));
      break;
  }
}

void InsertMessageToChunk(std::unique_ptr<uint8_t[]>& fastlane_buffer,
                         size_t buffer_size,
                         DataChunk& output) {
  const auto ptr = reinterpret_cast<const char*>(fastlane_buffer.get());
  const auto len = buffer_size;
  const auto wrapped_buffer =
      make_buffer<FastlaneStringVectorBuffer>(std::move(fastlane_buffer), len);
  auto& vector = output.data[0];
  StringVector::AddBuffer(vector, wrapped_buffer);
  const auto data_ptr = reinterpret_cast<string_t*>(vector.GetData());
  *data_ptr = string_t(ptr, len);
  output.SetCardinality(1);
  output.Verify();
}

OperatorResultType WriteFastlaneFunction::Function(ExecutionContext& context,
                                                  TableFunctionInput& data_p, DataChunk& input,
                                                  DataChunk& output) {
  auto& local_state = data_p.local_state->Cast<WriteFastlaneLocalState>();
  auto& global_state = data_p.global_state->Cast<WriteFastlaneGlobalState>();
  auto& bind_data = data_p.bind_data->Cast<WriteFastlaneFunctionData>();

  if (input.size() == 0) {
    output.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
  }

  // Serialize the input data to FastLanes format
  std::unique_ptr<uint8_t[]> fastlane_buffer;
  size_t buffer_size;
  SerializeFastlaneData(local_state, input, bind_data.logical_types, 
                       bind_data.column_names, fastlane_buffer, buffer_size);
  
  InsertMessageToChunk(fastlane_buffer, buffer_size, output);
  
  // Set the header flag (true for first chunk, false for subsequent chunks)
  auto& header_vector = output.data[1];
  bool is_header = !global_state.sent_schema.load();
  header_vector.SetValue(0, Value::BOOLEAN(is_header));
  
  if (is_header) {
    global_state.sent_schema.store(true);
  }
  
  return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorFinalizeResultType WriteFastlaneFunction::FunctionFinal(ExecutionContext& context,
                                                               TableFunctionInput& data_p,
                                                               DataChunk& output) {
  output.SetCardinality(0);
  return OperatorFinalizeResultType::FINISHED;
}

TableFunction WriteFastlaneFunction::GetFunction() {
  TableFunction write_fastlane("write_fastlane", {LogicalType::TABLE}, nullptr, Bind, InitGlobal, InitLocal);
  write_fastlane.in_out_function = Function;
  write_fastlane.in_out_function_final = FunctionFinal;
  return write_fastlane;
}

void WriteFastlaneFunction::RegisterWriteFastlaneFunction(DatabaseInstance& db) {
  auto function = GetFunction();
  ExtensionUtil::RegisterFunction(db, function);
}

}  // namespace ext_fastlane
}  // namespace duckdb 