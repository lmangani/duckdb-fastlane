//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// writer/write_fastlane.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace ext_fastlane {



class WriteFastlaneFunction {
 public:
  //! note: this is the number of vectors per chunk
  static constexpr idx_t DEFAULT_CHUNK_SIZE = 120;

  static TableFunction GetFunction();
  static void RegisterWriteFastlaneFunction(DatabaseInstance& db);

 private:
  static unique_ptr<LocalTableFunctionState> InitLocal(
      ExecutionContext& context, TableFunctionInitInput& input,
      GlobalTableFunctionState* global_state);
  static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context,
                                                         TableFunctionInitInput& input);
  static unique_ptr<FunctionData> Bind(ClientContext& context,
                                       TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types,
                                       vector<string>& names);
  static OperatorResultType Function(ExecutionContext& context,
                                     TableFunctionInput& data_p, DataChunk& input,
                                     DataChunk& output);
  static OperatorFinalizeResultType FunctionFinal(ExecutionContext& context,
                                                  TableFunctionInput& data_p,
                                                  DataChunk& output);
};

}  // namespace ext_fastlane
}  // namespace duckdb 