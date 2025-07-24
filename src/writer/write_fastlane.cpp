#include "writer/write_fastlane.hpp"
#include "type_mapping.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

// FastLanes includes
#include "fastlanes.hpp"

namespace duckdb {

namespace ext_fastlane {

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
  unique_ptr<fastlanes::Connection> connection;
};

struct WriteFastlaneLocalState : public LocalTableFunctionState {
  unique_ptr<fastlanes::Connection> connection;
  idx_t current_count = 0;
  bool checked_schema = false;
};

unique_ptr<LocalTableFunctionState> WriteFastlaneFunction::InitLocal(
    ExecutionContext& context, TableFunctionInitInput& input,
    GlobalTableFunctionState* global_state) {
  auto local_state = make_uniq<WriteFastlaneLocalState>();
  local_state->connection = make_uniq<fastlanes::Connection>();
  return local_state;
}

unique_ptr<GlobalTableFunctionState> WriteFastlaneFunction::InitGlobal(
    ClientContext& context, TableFunctionInitInput& input) {
  auto result = make_uniq<WriteFastlaneGlobalState>();
  result->connection = make_uniq<fastlanes::Connection>();
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
  // TODO: Implement actual FastLanes serialization
  // This would involve:
  // 1. Converting DuckDB data types to FastLanes data types
  // 2. Using FastLanes C++ API to encode the data
  // 3. Creating the proper FastLanes file format
  
  // For now, create a dummy buffer with some metadata
  buffer_size = 1024;
  fastlane_buffer = std::make_unique<uint8_t[]>(buffer_size);
  
  // Write FastLanes magic bytes at the beginning
  uint64_t magic_bytes = 0x656E614C74736146ULL; // "FastLane" in little-endian
  memcpy(fastlane_buffer.get(), &magic_bytes, sizeof(magic_bytes));
  
  // Write version
  uint64_t version = 0x0000342E312E3076ULL; // "v0.1.4" in little-endian
  memcpy(fastlane_buffer.get() + 8, &version, sizeof(version));
  
  // Write column count
  uint32_t column_count = logical_types.size();
  memcpy(fastlane_buffer.get() + 16, &column_count, sizeof(column_count));
  
  // Fill the rest with dummy data
  for (size_t i = 20; i < buffer_size; i++) {
    fastlane_buffer[i] = static_cast<uint8_t>(i % 256);
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
  TableFunction write_fastlane("write_fastlane");
  write_fastlane.bind = Bind;
  write_fastlane.init_local = InitLocal;
  write_fastlane.init_global = InitGlobal;
  write_fastlane.function = Function;
  write_fastlane.function_final = FunctionFinal;
  return write_fastlane;
}

void WriteFastlaneFunction::RegisterWriteFastlaneFunction(DatabaseInstance& db) {
  auto function = GetFunction();
  ExtensionUtil::RegisterFunction(db, function);
}

}  // namespace ext_fastlane
}  // namespace duckdb 