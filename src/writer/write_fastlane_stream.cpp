#include "write_fastlane_stream.hpp"
#include "type_mapping.hpp"
#include "fastlanes_facade.hpp"

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "table_function/read_fastlane.hpp"

namespace duckdb {

namespace ext_fastlane {

namespace {

struct FastlaneWriteBindData : public TableFunctionData {
  vector<LogicalType> sql_types;
  vector<string> column_names;
  // Storage::ROW_GROUP_SIZE (122880), which seems to be the default
  // for Parquet, is higher than the usual number used in FastLanes writers.
  // Using a value of 65536 results in better performance for streaming.
  idx_t row_group_size = 65536;
  bool row_group_size_set = false;
  optional_idx row_groups_per_file;
  static constexpr const idx_t BYTES_PER_ROW = 1024;
  idx_t row_group_size_bytes{};
};

struct FastlaneWriteGlobalState : public GlobalFunctionData {
  unique_ptr<FastLanesFacade> facade;
  string file_path;
  idx_t current_rowgroup = 0;
  idx_t rows_in_current_rowgroup = 0;
  unique_ptr<BufferedFileWriter> file_writer;
};

struct FastlaneWriteLocalState : public LocalFunctionData {
  explicit FastlaneWriteLocalState(ClientContext& context, const vector<LogicalType>& types)
      : buffer(context, types, ColumnDataAllocatorType::HYBRID) {
    buffer.InitializeAppend(append_state);
  }

  ColumnDataCollection buffer;
  ColumnDataAppendState append_state;
};

// Forward declarations
void WriteRowgroupToFastLanes(FastlaneWriteGlobalState& global_state,
                             FastlaneWriteLocalState& local_state,
                             const FastlaneWriteBindData& bind_data);
void WriteRowgroupHeader(FastlaneWriteGlobalState& global_state,
                        FastlaneWriteLocalState& local_state,
                        const FastlaneWriteBindData& bind_data);
void WriteRowgroupData(FastlaneWriteGlobalState& global_state,
                      FastlaneWriteLocalState& local_state,
                      const FastlaneWriteBindData& bind_data);
void WriteColumnData(FastlaneWriteGlobalState& global_state,
                    FastlaneWriteLocalState& local_state,
                    idx_t col_idx,
                    const LogicalType& logical_type,
                    FastLanesDataType fastlanes_type);
void WriteFastLanesFooter(FastlaneWriteGlobalState& global_state);

unique_ptr<FunctionData> FastlaneWriteBind(ClientContext& context,
                                          CopyFunctionBindInput& input,
                                          const vector<string>& names,
                                          const vector<LogicalType>& sql_types) {
  D_ASSERT(names.size() == sql_types.size());
  auto bind_data = make_uniq<FastlaneWriteBindData>();
  bool row_group_size_bytes_set = false;

  for (auto& option : input.info.options) {
    const auto loption = StringUtil::Lower(option.first);
    if (option.second.size() != 1) {
      // All FastLanes write options require exactly one argument
      throw BinderException("%s requires exactly one argument",
                            StringUtil::Upper(loption));
    }

    if (loption == "row_group_size" || loption == "chunk_size") {
      if (bind_data->row_group_size_set) {
        throw BinderException(
            "ROW_GROUP_SIZE and ROW_GROUP_SIZE_BYTES are mutually exclusive");
      }
      bind_data->row_group_size = option.second[0].GetValue<uint64_t>();
      bind_data->row_group_size_set = true;
    } else if (loption == "row_group_size_bytes") {
      auto roption = option.second[0];
      if (roption.GetTypeMutable().id() == LogicalTypeId::VARCHAR) {
        bind_data->row_group_size_bytes = DBConfig::ParseMemoryLimit(roption.ToString());
      } else {
        bind_data->row_group_size_bytes = option.second[0].GetValue<uint64_t>();
      }
      row_group_size_bytes_set = true;
    } else if (loption == "row_groups_per_file") {
      bind_data->row_groups_per_file = option.second[0].GetValue<uint64_t>();
    } else {
      throw BinderException("Unknown option for FastLanes: %s", StringUtil::Upper(loption));
    }
  }

  bind_data->sql_types = sql_types;
  bind_data->column_names = names;

  return std::move(bind_data);
}

unique_ptr<GlobalFunctionData> FastlaneWriteInitializeGlobal(ClientContext& context,
                                                            FunctionData& bind_data,
                                                            const string& file_path) {
  auto result = make_uniq<FastlaneWriteGlobalState>();
  result->facade = make_uniq<FastLanesFacade>();
  result->file_path = file_path;
  result->file_writer = make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(context), file_path);
  return std::move(result);
}

void FastlaneWriteSink(ExecutionContext& context, FunctionData& bind_data_p,
                      GlobalFunctionData& gstate, LocalFunctionData& lstate,
                      DataChunk& input) {
  auto& bind_data = bind_data_p.Cast<FastlaneWriteBindData>();
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  auto& local_state = lstate.Cast<FastlaneWriteLocalState>();

  // Add the input data to our buffer
  local_state.buffer.Append(local_state.append_state, input);

  // Check if we need to write a rowgroup
  if (local_state.buffer.Count() >= bind_data.row_group_size) {
    // Write the buffered data to FastLanes format
    WriteRowgroupToFastLanes(global_state, local_state, bind_data);
    
    // Reset the buffer
    local_state.buffer.Reset();
    local_state.buffer.InitializeAppend(local_state.append_state);
    global_state.current_rowgroup++;
  }
}

void WriteRowgroupToFastLanes(FastlaneWriteGlobalState& global_state,
                             FastlaneWriteLocalState& local_state,
                             const FastlaneWriteBindData& bind_data) {
  // Create a FastLanes rowgroup from the buffer
  try {
    // Use FastLanes C++ API to write the rowgroup
    // Use the facade instead of direct FastLanes connection
    // auto connection = fastlanes::Connection();
    
    // Set rowgroup size
    connection.set_n_vectors_per_rowgroup(bind_data.row_group_size);
    
    // Convert the buffer to FastLanes format and write
    // This is a simplified implementation - in practice, you'd need to:
    // 1. Convert ColumnDataCollection to FastLanes format
    // 2. Use FastLanes encoding functions
    // 3. Write the encoded data to the file
    
    // For now, write a placeholder rowgroup header
    WriteRowgroupHeader(global_state, local_state, bind_data);
    
    // Write the actual data
    WriteRowgroupData(global_state, local_state, bind_data);
    
  } catch (const std::exception& e) {
    throw IOException("Failed to write FastLanes rowgroup: %s", e.what());
  }
}

void WriteRowgroupHeader(FastlaneWriteGlobalState& global_state,
                        FastlaneWriteLocalState& local_state,
                        const FastlaneWriteBindData& bind_data) {
  // Write rowgroup header information
  // This would include metadata about the rowgroup, column types, etc.
  
  // Write magic bytes for rowgroup
  uint64_t rowgroup_magic = 0x7075676F77726152ULL; // "Rowgroup" in little-endian
  global_state.file_writer->WriteData((const_data_ptr_t)&rowgroup_magic, sizeof(rowgroup_magic));
  
  // Write rowgroup size
  uint32_t rowgroup_size = local_state.buffer.Count();
  global_state.file_writer->WriteData((const_data_ptr_t)&rowgroup_size, sizeof(rowgroup_size));
  
  // Write column count
  uint32_t column_count = bind_data.sql_types.size();
  global_state.file_writer->WriteData((const_data_ptr_t)&column_count, sizeof(column_count));
}

void WriteRowgroupData(FastlaneWriteGlobalState& global_state,
                      FastlaneWriteLocalState& local_state,
                      const FastlaneWriteBindData& bind_data) {
  // Write each column's data
  for (idx_t col_idx = 0; col_idx < bind_data.sql_types.size(); col_idx++) {
    auto& logical_type = bind_data.sql_types[col_idx];
    
    if (TypeMapping::IsSupported(logical_type)) {
      auto fastlanes_type = TypeMapping::DuckDBToFastLanes(logical_type);
      
      // Write column type
      uint8_t data_type = static_cast<uint8_t>(fastlanes_type);
      global_state.file_writer->WriteData((const_data_ptr_t)&data_type, sizeof(data_type));
      
      // Write column data
      WriteColumnData(global_state, local_state, col_idx, logical_type, fastlanes_type);
    }
  }
}

void WriteColumnData(FastlaneWriteGlobalState& global_state,
                    FastlaneWriteLocalState& local_state,
                    idx_t col_idx,
                    const LogicalType& logical_type,
                    FastLanesDataType fastlanes_type) {
  // Get the column data from the buffer
  auto& chunk = local_state.buffer.GetChunk(0); // Assuming single chunk for simplicity
  auto& vector = chunk.data[col_idx];
  
  // Convert and write the data
  switch (fastlanes_type) {
    case BOOLEAN: {
      for (idx_t i = 0; i < chunk.size(); i++) {
        auto value = vector.GetValue(i);
        bool bool_val = value.GetValue<bool>();
        global_state.file_writer->WriteData((const_data_ptr_t)&bool_val, sizeof(bool_val));
      }
      break;
    }
    case INT32: {
      for (idx_t i = 0; i < chunk.size(); i++) {
        auto value = vector.GetValue(i);
        int32_t int_val = value.GetValue<int32_t>();
        global_state.file_writer->WriteData((const_data_ptr_t)&int_val, sizeof(int_val));
      }
      break;
    }
    case INT64: {
      for (idx_t i = 0; i < chunk.size(); i++) {
        auto value = vector.GetValue(i);
        int64_t int_val = value.GetValue<int64_t>();
        global_state.file_writer->WriteData((const_data_ptr_t)&int_val, sizeof(int_val));
      }
      break;
    }
    case DOUBLE: {
      for (idx_t i = 0; i < chunk.size(); i++) {
        auto value = vector.GetValue(i);
        double double_val = value.GetValue<double>();
        global_state.file_writer->WriteData((const_data_ptr_t)&double_val, sizeof(double_val));
      }
      break;
    }
    case STR: {
      for (idx_t i = 0; i < chunk.size(); i++) {
        auto value = vector.GetValue(i);
        if (value.IsNull()) {
          uint32_t str_len = 0;
          global_state.file_writer->WriteData((const_data_ptr_t)&str_len, sizeof(str_len));
        } else {
          auto str_val = value.GetValue<string>();
          uint32_t str_len = str_val.length();
          global_state.file_writer->WriteData((const_data_ptr_t)&str_len, sizeof(str_len));
          global_state.file_writer->WriteData((const_data_ptr_t)str_val.c_str(), str_len);
        }
      }
      break;
    }
    default:
      // For unsupported types, write zeros
      for (idx_t i = 0; i < chunk.size(); i++) {
        uint8_t zero = 0;
        global_state.file_writer->WriteData((const_data_ptr_t)&zero, sizeof(zero));
      }
      break;
  }
}

void FastlaneWriteCombine(ExecutionContext& context, FunctionData& bind_data,
                         GlobalFunctionData& gstate, LocalFunctionData& lstate) {
  // Nothing to do for FastLanes - all data is written in the sink
}

void FastlaneWriteFinalize(ClientContext& context, FunctionData& bind_data,
                          GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  
  // Write any remaining data in buffers
  // Write FastLanes footer/metadata
  // Close the file
  
  try {
    // Write FastLanes footer
    WriteFastLanesFooter(global_state);
    
    // Close the file writer
    global_state.file_writer->Close();
    
  } catch (const std::exception& e) {
    throw IOException("Failed to finalize FastLanes file: %s", e.what());
  }
}

void WriteFastLanesFooter(FastlaneWriteGlobalState& global_state) {
  // Write FastLanes footer with metadata
  // This would include:
  // - Total row count
  // - Schema information
  // - Rowgroup offsets
  // - File statistics
  
  // Write footer magic
  uint64_t footer_magic = 0x65746F6F66736146ULL; // "FastLanesFooter" in little-endian
  global_state.file_writer->WriteData((const_data_ptr_t)&footer_magic, sizeof(footer_magic));
  
  // Write total row count
  uint64_t total_rows = global_state.rows_in_current_rowgroup;
  global_state.file_writer->WriteData((const_data_ptr_t)&total_rows, sizeof(total_rows));
  
  // Write rowgroup count
  uint32_t rowgroup_count = global_state.current_rowgroup;
  global_state.file_writer->WriteData((const_data_ptr_t)&rowgroup_count, sizeof(rowgroup_count));
}

unique_ptr<LocalFunctionData> FastlaneWriteInitializeLocal(ExecutionContext& context,
                                                          FunctionData& bind_data_p) {
  auto& bind_data = bind_data_p.Cast<FastlaneWriteBindData>();
  return make_uniq<FastlaneWriteLocalState>(context.client, bind_data.sql_types);
}

CopyFunctionExecutionMode FastlaneWriteExecutionMode(bool preserve_insertion_order,
                                                    bool supports_batch_index) {
  if (!preserve_insertion_order) {
    return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
  }
  if (supports_batch_index) {
    return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
  }
  return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

idx_t FastlaneWriteDesiredBatchSize(ClientContext& context, FunctionData& bind_data_p) {
  auto& bind_data = bind_data_p.Cast<FastlaneWriteBindData>();
  return bind_data.row_group_size;
}

bool FastlaneWriteRotateFiles(FunctionData& bind_data_p,
                             const optional_idx& file_size_bytes) {
  auto& bind_data = bind_data_p.Cast<FastlaneWriteBindData>();
  return file_size_bytes.IsValid() || bind_data.row_groups_per_file.IsValid();
}

bool FastlaneWriteRotateNextFile(GlobalFunctionData& gstate, FunctionData& bind_data_p,
                                const optional_idx& file_size_bytes) {
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  auto& bind_data = bind_data_p.Cast<FastlaneWriteBindData>();
  
  // Check if we need to rotate files
  if (bind_data.row_groups_per_file.IsValid()) {
    return global_state.current_rowgroup >= bind_data.row_groups_per_file.GetIndex();
  }
  
  if (file_size_bytes.IsValid()) {
    // Check if current file size exceeds the limit
    // This would require tracking the current file size
    return false; // Placeholder
  }
  
  return false;
}

}  // namespace

void RegisterFastlaneStreamCopyFunction(DatabaseInstance& db) {
  CopyFunction function("fls");
  function.copy_to_bind = FastlaneWriteBind;
  function.copy_to_initialize_global = FastlaneWriteInitializeGlobal;
  function.copy_to_initialize_local = FastlaneWriteInitializeLocal;
  function.copy_to_sink = FastlaneWriteSink;
  function.copy_to_combine = FastlaneWriteCombine;
  function.copy_to_finalize = FastlaneWriteFinalize;
  function.execution_mode = FastlaneWriteExecutionMode;
  function.copy_from_bind = MultiFileFunction<MultiFileList>::MultiFileBindCopy;
  function.copy_from_function = ReadFastlaneStreamFunction();
  function.desired_batch_size = FastlaneWriteDesiredBatchSize;
  function.rotate_files = FastlaneWriteRotateFiles;
  function.rotate_next_file = FastlaneWriteRotateNextFile;

  function.extension = "fls";
  ExtensionUtil::RegisterFunction(db, function);

  function.name = "fastlane";
  function.extension = "fastlane";
  ExtensionUtil::RegisterFunction(db, function);
}

}  // namespace ext_fastlane
}  // namespace duckdb 