#include "scan_fastlanes.hpp"
#include "fastlanes_facade.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <memory>

namespace duckdb {

struct FastLanesScanState : public GlobalTableFunctionState {
    std::unique_ptr<FastLanesFacade> facade;
    bool initialized = false;
};

struct FastLanesBindData : public TableFunctionData {
    std::string file_path;
    std::vector<LogicalType> return_types;
    std::vector<std::string> return_names;
};

static unique_ptr<FunctionData> FastLanesBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &return_names) {
    auto result = make_uniq<FastLanesBindData>();
    
    // Get the file path from the function arguments
    if (input.inputs.size() != 1) {
        throw BinderException("scan_fastlanes requires exactly one argument (file path)");
    }
    
    if (input.inputs[0].type() != LogicalType::VARCHAR) {
        throw BinderException("scan_fastlanes file path must be a string");
    }
    
    result->file_path = input.inputs[0].GetValue<string>();
    
    // Create a temporary facade to get the schema
    auto temp_facade = std::make_unique<FastLanesFacade>();
    if (!temp_facade->openFile(result->file_path)) {
        throw BinderException("Failed to open FastLanes file: " + result->file_path);
    }
    
    // For now, use a simple schema - we'll extract the real schema later
    return_types = {LogicalType::VARCHAR};
    return_names = {"data"};
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FastLanesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<FastLanesScanState>();
    auto &bind_data = input.bind_data->Cast<FastLanesBindData>();
    
    result->facade = std::make_unique<FastLanesFacade>();
    if (!result->facade->openFile(bind_data.file_path)) {
        throw IOException("Failed to open FastLanes file: " + bind_data.file_path);
    }
    
    result->initialized = true;
    return std::move(result);
}

static void FastLanesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<FastLanesScanState>();
    
    if (!state.initialized || !state.facade) {
        output.SetCardinality(0);
        return;
    }
    
    if (!state.facade->readNextChunk(output)) {
        output.SetCardinality(0);
        return;
    }
}

void ScanFastLanes::Register(DatabaseInstance &db) {
    TableFunction scan_fastlanes("scan_fastlanes", {LogicalType::VARCHAR}, FastLanesScan, FastLanesBind, FastLanesInitGlobal);
    scan_fastlanes.named_parameters["file"] = LogicalType::VARCHAR;
    
    ExtensionUtil::RegisterFunction(db, scan_fastlanes);
}

} // namespace duckdb 