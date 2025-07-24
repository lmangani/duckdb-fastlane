#include "table_function/read_fastlane.hpp"
#include "fastlanes_facade.hpp"
#include "type_mapping.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/table_scan_info.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

namespace ext_fastlane {

struct ReadFastlaneBindData : public TableFunctionData {
  vector<string> files;
  vector<LogicalType> sql_types;
  vector<string> column_names;
  bool auto_detect = false;
};

struct ReadFastlaneGlobalState : public GlobalTableFunctionState {
  ReadFastlaneGlobalState() : current_file(0) {}
  
  idx_t current_file;
  vector<unique_ptr<FastLanesFacade>> readers;
  
  idx_t MaxThreads() const override {
    return 1; // Single-threaded for now
  }
};

struct ReadFastlaneLocalState : public LocalTableFunctionState {
  ReadFastlaneLocalState() : current_reader_idx(0) {}
  
  idx_t current_reader_idx;
  vector<Value> current_chunk_values;
  idx_t current_chunk_rows = 0;
  idx_t current_chunk_pos = 0;
};

static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                   vector<LogicalType>& return_types, vector<string>& names) {
  auto result = make_uniq<ReadFastlaneBindData>();
  
  // Handle file paths
  auto& inputs = input.inputs;
  if (inputs.empty()) {
    throw BinderException("read_fastlane requires at least one file path");
  }
  
  // Get file paths
  for (auto& input : inputs) {
    if (input.type() != LogicalType::VARCHAR) {
      throw BinderException("read_fastlane file paths must be strings");
    }
    result->files.push_back(input.GetValue<string>());
  }
  
  // Try to read the first file to get schema
  FastLanesFacade facade;
  if (!facade.openFile(result->files[0])) {
    throw BinderException("Failed to open FastLanes file: " + result->files[0]);
  }
  
  // Get column types and names
  result->sql_types = facade.getColumnTypes();
  result->column_names = facade.getColumnNames();
  
  return_types = result->sql_types;
  names = result->column_names;
  
  facade.closeFile();
  
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context, TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<ReadFastlaneBindData>();
  auto result = make_uniq<ReadFastlaneGlobalState>();
  
  // Initialize readers for all files
  for (const auto& file : bind_data.files) {
    auto reader = make_uniq<FastLanesFacade>();
    if (!reader->openFile(file)) {
      throw IOException("Failed to open FastLanes file: " + file);
    }
    result->readers.push_back(std::move(reader));
  }
  
  return std::move(result);
}

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                   GlobalTableFunctionState* global_state) {
  return make_uniq<ReadFastlaneLocalState>();
}

static void Scan(ClientContext& context, TableFunctionInput& data_p, DataChunk& output) {
  auto& data = data_p.bind_data->Cast<ReadFastlaneBindData>();
  auto& global_state = data_p.global_state->Cast<ReadFastlaneGlobalState>();
  auto& local_state = data_p.local_state->Cast<ReadFastlaneLocalState>();
  
  output.Reset();
  
  while (global_state.current_file < global_state.readers.size()) {
    auto& reader = global_state.readers[global_state.current_file];
    
    // Read next chunk
    idx_t rows_read = 0;
    if (reader->readNextChunk(local_state.current_chunk_values, rows_read)) {
      // Convert values to output chunk
      // TODO: Implement actual data conversion
      output.SetCardinality(rows_read);
      break;
    } else {
      // Move to next file
      global_state.current_file++;
      local_state.current_reader_idx = 0;
    }
  }
}

void RegisterReadFastlaneStream(DatabaseInstance& db) {
  TableFunction read_fastlane("read_fastlane", {LogicalType::VARCHAR}, Scan, Bind, InitGlobal, InitLocal);
  read_fastlane.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
  
  ExtensionUtil::RegisterFunction(db, read_fastlane);
  
  // Register replacement scan for .fls and .fastlane files
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadFastlaneReplacementScan);
}

static unique_ptr<TableRef> ReadFastlaneReplacementScan(ClientContext& context, const string& table_name, optional_ptr<ReplacementScanData> data) {
  auto lower_name = StringUtil::Lower(table_name);
  if (!StringUtil::EndsWith(lower_name, ".fls") && !StringUtil::EndsWith(lower_name, ".fastlane")) {
    return nullptr;
  }
  
  auto table_function = make_uniq<TableFunctionRef>();
  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
  table_function->function = make_uniq<FunctionExpression>("read_fastlane", std::move(children));
  
  return std::move(table_function);
}

}  // namespace ext_fastlane
}  // namespace duckdb 