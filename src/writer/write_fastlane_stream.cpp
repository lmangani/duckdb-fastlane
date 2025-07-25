#include "write_fastlane_stream.hpp"
#include "type_mapping.hpp"
#include "fastlanes_facade.hpp"

#include "duckdb/common/multi_file/multi_file_function.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "table_function/scan_fastlanes.hpp"

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
  auto& bind_data_cast = bind_data.Cast<FastlaneWriteBindData>();
  
  // Initialize the FastLanes facade
  result->facade = make_uniq<FastLanesFacade>();
  if (!result->facade->createFile(file_path, bind_data_cast.sql_types, bind_data_cast.column_names)) {
    throw IOException("Failed to create FastLanes file: " + file_path);
  }
  
  result->file_path = file_path;
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
  try {
    // Use FastLanes C++ API to write the rowgroup
    if (!global_state.facade) {
      throw IOException("FastLanes facade not initialized");
    }
    
    // Convert the buffer data to FastLanes format
    auto& buffer = local_state.buffer;
    if (buffer.Count() == 0) {
      return; // Nothing to write
    }

    for (auto& chunk_ptr : local_state.buffer.Chunks()) {
        DataChunk chunk;
        chunk.Initialize(Allocator::DefaultAllocator(), chunk_ptr.GetTypes());
        chunk_ptr.Copy(chunk);
        if (!global_state.facade->writeChunk(chunk)) {
            throw IOException("Failed to write chunk to FastLanes");
        }
    }
    
    global_state.current_rowgroup++;
    global_state.rows_in_current_rowgroup += buffer.Count();
    
  } catch (const std::exception& e) {
    throw IOException("Failed to write FastLanes rowgroup: %s", e.what());
  }
}

void WriteRowgroupHeader(FastlaneWriteGlobalState& global_state,
                        FastlaneWriteLocalState& local_state,
                        const FastlaneWriteBindData& bind_data) {
  // FastLanes handles rowgroup headers internally
  // This function is not needed when using the FastLanes API
}

void WriteRowgroupData(FastlaneWriteGlobalState& global_state,
                      FastlaneWriteLocalState& local_state,
                      const FastlaneWriteBindData& bind_data) {
  // FastLanes handles data writing internally through the API
  // This function is not needed when using the FastLanes API
}

void WriteColumnData(FastlaneWriteGlobalState& global_state,
                    FastlaneWriteLocalState& local_state,
                    idx_t col_idx,
                    const LogicalType& logical_type,
                    FastLanesDataType fastlanes_type) {
  // Extract data from the ColumnDataCollection and convert to FastLanes format
  // This is a simplified implementation - in practice, you'd need to handle all data types properly
  
  idx_t row_count = local_state.buffer.Count();
  
  // Extract the actual data from the buffer
  // In a real implementation, you'd iterate through the ColumnDataCollection properly
  // and convert each value to the appropriate FastLanes format
  
  // For now, this is a placeholder that would need to be implemented with proper data extraction
  // from the ColumnDataCollection
}

void FastlaneWriteCombine(ExecutionContext& context, FunctionData& bind_data,
                         GlobalFunctionData& gstate, LocalFunctionData& lstate) {
  auto& bind_data_cast = bind_data.Cast<FastlaneWriteBindData>();
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  auto& local_state = lstate.Cast<FastlaneWriteLocalState>();

  if (local_state.buffer.Count() > 0) {
    WriteRowgroupToFastLanes(global_state, local_state, bind_data_cast);
    local_state.buffer.Reset();
    local_state.buffer.InitializeAppend(local_state.append_state);
  }
}

void FastlaneWriteFinalize(ClientContext& context, FunctionData& bind_data,
                          GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  
  try {
    if (global_state.facade) {
      global_state.facade->finalizeFile();
    }
    

    
  } catch (const std::exception& e) {
    throw IOException("Failed to finalize FastLanes file: %s", e.what());
  }
}

void WriteFastLanesFooter(FastlaneWriteGlobalState& global_state) {
  // FastLanes handles footer writing internally
  // This function is not needed when using the FastLanes API
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
    return false; // Not implemented yet
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
  function.copy_from_function = ScanFastlanesStreamFunction();
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