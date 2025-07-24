#include "table_function/read_fastlane.hpp"
#include "type_mapping.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

// FastLanes includes
#include "fastlanes.hpp"

namespace duckdb {

namespace ext_fastlane {

struct ReadFastlaneBindData : public TableFunctionData {
  ReadFastlaneBindData() = default;
  vector<string> file_paths;
  vector<LogicalType> return_types;
  vector<string> return_names;
};

struct ReadFastlaneGlobalState : public GlobalTableFunctionState {
  ReadFastlaneGlobalState() = default;
  unique_ptr<fastlanes::Connection> connection;
  unique_ptr<fastlanes::TableReader> table_reader;
  unique_ptr<fastlanes::Table> table;
  idx_t current_rowgroup = 0;
  idx_t total_rowgroups = 0;
};

struct ReadFastlaneLocalState : public LocalTableFunctionState {
  ReadFastlaneLocalState() = default;
  unique_ptr<fastlanes::RowgroupReader> rowgroup_reader;
  idx_t current_chunk = 0;
  idx_t total_chunks = 0;
};

struct ReadFastlaneStream : TableFunction {
  static TableFunction Function() {
    TableFunction read_fastlane("read_fastlane");
    read_fastlane.arguments = {LogicalType::VARCHAR};
    read_fastlane.projection_pushdown = true;
    read_fastlane.filter_pushdown = false;
    read_fastlane.filter_prune = false;
    read_fastlane.bind = Bind;
    read_fastlane.init_global = InitGlobal;
    read_fastlane.init_local = InitLocal;
    read_fastlane.function = Scan;
    return read_fastlane;
  }

  static unique_ptr<TableRef> ScanReplacement(ClientContext& context,
                                              ReplacementScanInput& input,
                                              optional_ptr<ReplacementScanData> data) {
    auto table_name = ReplacementScan::GetFullPath(input);
    if (!ReplacementScan::CanReplace(table_name, {"fls", "fastlane"})) {
      return nullptr;
    }

    auto table_function = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    auto table_name_expr = make_uniq<ConstantExpression>(Value(table_name));
    children.push_back(std::move(table_name_expr));
    auto function_expr = make_uniq<FunctionExpression>("read_fastlane", std::move(children));
    table_function->function = std::move(function_expr);

    if (!FileSystem::HasGlob(table_name)) {
      auto& fs = FileSystem::GetFileSystem(context);
      table_function->alias = fs.ExtractBaseName(table_name);
    }

    return std::move(table_function);
  }

  static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types, vector<string>& names) {
    auto result = make_uniq<ReadFastlaneBindData>();
    
    // Get the file path from the bind input
    auto file_path = input.inputs[0].GetValue<string>();
    result->file_paths.push_back(file_path);

    // Try to read the schema from the FastLanes file
    try {
      fastlanes::Connection connection;
      auto table_reader = connection.read_fls(file_path);
      auto table = table_reader->materialize();
      
      if (table && table->get_n_rowgroups() > 0) {
        // Get the first rowgroup to determine schema
        auto& first_rowgroup = table->GetRowgroup(0);
        
        // TODO: Extract column names and types from the rowgroup
        // For now, create a basic schema
        auto column_names = first_rowgroup.get_column_names();
        auto data_types = first_rowgroup.get_data_types();
        
        for (size_t i = 0; i < column_names.size(); i++) {
          if (i < data_types.size()) {
            // Convert FastLanes data type to DuckDB type
            auto duckdb_type = TypeMapping::FastLanesToDuckDB(
                static_cast<FastLanesDataType>(data_types[i]));
            return_types.push_back(duckdb_type);
          } else {
            return_types.push_back(LogicalType::VARCHAR);
          }
          names.push_back(column_names[i]);
        }
      }
    } catch (const std::exception& e) {
      // File doesn't exist or can't be read - create a basic schema
      return_types.emplace_back(LogicalType::VARCHAR);
      names.emplace_back("column_1");
    }
    
    result->return_types = return_types;
    result->return_names = names;
    
    return std::move(result);
  }

  static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context, TableFunctionInitInput& input) {
    auto result = make_uniq<ReadFastlaneGlobalState>();
    auto& bind_data = input.bind_data->Cast<ReadFastlaneBindData>();
    
    // Initialize FastLanes connection
    result->connection = make_uniq<fastlanes::Connection>();
    
    // Try to read the FastLanes file
    try {
      auto file_path = bind_data.file_paths[0];
      result->table_reader = result->connection->read_fls(file_path);
      result->table = result->table_reader->materialize();
      
      if (result->table) {
        result->total_rowgroups = result->table->get_n_rowgroups();
      } else {
        result->total_rowgroups = 0;
      }
    } catch (const std::exception& e) {
      // File doesn't exist or can't be read - this is okay for testing
      result->total_rowgroups = 0;
    }
    
    return std::move(result);
  }

  static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                       GlobalTableFunctionState* global_state) {
    auto result = make_uniq<ReadFastlaneLocalState>();
    auto& global = global_state->Cast<ReadFastlaneGlobalState>();
    
    if (global.table_reader && global.current_rowgroup < global.total_rowgroups) {
      try {
        // Get the current rowgroup reader
        result->rowgroup_reader = global.table_reader->get_rowgroup_reader(global.current_rowgroup);
        
        // TODO: Get actual chunk count from the rowgroup
        // For now, assume one chunk per rowgroup
        result->total_chunks = 1;
      } catch (const std::exception& e) {
        // Handle error gracefully
        result->total_chunks = 0;
      }
    }
    
    return std::move(result);
  }

  static OperatorResultType Scan(ExecutionContext& context, TableFunctionInput& data_p, DataChunk& output) {
    auto& global_state = data_p.global_state->Cast<ReadFastlaneGlobalState>();
    auto& local_state = data_p.local_state->Cast<ReadFastlaneLocalState>();
    
    if (!local_state.rowgroup_reader || local_state.current_chunk >= local_state.total_chunks) {
      // Move to next rowgroup
      global_state.current_rowgroup++;
      if (global_state.current_rowgroup >= global_state.total_rowgroups) {
        output.SetCardinality(0);
        return OperatorResultType::FINISHED;
      }
      
      // Initialize local state for next rowgroup
      try {
        local_state.rowgroup_reader = global_state.table_reader->get_rowgroup_reader(global_state.current_rowgroup);
        local_state.current_chunk = 0;
        local_state.total_chunks = 1; // Placeholder
      } catch (const std::exception& e) {
        output.SetCardinality(0);
        return OperatorResultType::FINISHED;
      }
    }
    
    // TODO: Actually read data from FastLanes and populate the output chunk
    // This would involve:
    // 1. Getting the chunk data from the rowgroup reader
    // 2. Converting FastLanes data types to DuckDB types
    // 3. Populating the output DataChunk
    
    // For now, create empty output
    output.SetCardinality(0);
    local_state.current_chunk++;
    
    return OperatorResultType::HAVE_MORE_OUTPUT;
  }
};

TableFunction ReadFastlaneStreamFunction() { return ReadFastlaneStream::Function(); }

void RegisterReadFastlaneStream(DatabaseInstance& db) {
  auto function = ReadFastlaneStream::Function();
  ExtensionUtil::RegisterFunction(db, function);
  // So we can accept a list of paths as well e.g., ['file_1.fls','file_2.fls']
  function.arguments = {LogicalType::LIST(LogicalType::VARCHAR)};
  ExtensionUtil::RegisterFunction(db, function);
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadFastlaneStream::ScanReplacement);
}

}  // namespace ext_fastlane
}  // namespace duckdb 