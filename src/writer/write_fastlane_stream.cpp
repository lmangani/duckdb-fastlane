#include "write_fastlane_stream.hpp"
#include "type_mapping.hpp"

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"

// FastLanes includes
#include "fastlanes.hpp"

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
  unique_ptr<fastlanes::Connection> connection;
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
  result->connection = make_uniq<fastlanes::Connection>();
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
    // TODO: Implement actual FastLanes writing
    // This would involve:
    // 1. Converting the buffer data to FastLanes format
    // 2. Writing to the file using FastLanes C++ API
    // 3. Resetting the buffer
    
    // For now, just reset the buffer
    local_state.buffer.Reset();
    local_state.buffer.InitializeAppend(local_state.append_state);
    global_state.current_rowgroup++;
  }
}

void FastlaneWriteCombine(ExecutionContext& context, FunctionData& bind_data,
                         GlobalFunctionData& gstate, LocalFunctionData& lstate) {
  // Nothing to do for FastLanes - all data is written in the sink
}

void FastlaneWriteFinalize(ClientContext& context, FunctionData& bind_data,
                          GlobalFunctionData& gstate) {
  auto& global_state = gstate.Cast<FastlaneWriteGlobalState>();
  
  // TODO: Implement finalization
  // This would involve:
  // 1. Writing any remaining data in buffers
  // 2. Writing FastLanes footer/metadata
  // 3. Closing the file
  
  // For now, just create a placeholder file
  try {
    // Create a simple FastLanes file with basic metadata
    auto connection = fastlanes::Connection();
    // TODO: Use the connection to write the actual FastLanes file
  } catch (const std::exception& e) {
    throw IOException("Failed to write FastLanes file: %s", e.what());
  }
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
  
  // TODO: Implement file rotation logic
  // For now, always return false (no rotation)
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
  function.copy_from_bind = MultiFileFunction<MultiFileInfo>::MultiFileBindCopy;
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