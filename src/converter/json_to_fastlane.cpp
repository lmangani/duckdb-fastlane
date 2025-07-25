#include "table_function/json_to_fastlane.hpp"
#include "fastlanes_facade.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

namespace ext_fastlane {

struct JsonToFastlaneBindData : public TableFunctionData {
    string input_file;
    string output_file;
    bool auto_detect = true;
};

struct JsonToFastlaneGlobalState : public GlobalTableFunctionState {
    JsonToFastlaneGlobalState() : processed(false) {}
    
    bool processed;
    
    idx_t MaxThreads() const override {
        return 1; // Single-threaded
    }
};

struct JsonToFastlaneLocalState : public LocalTableFunctionState {
    JsonToFastlaneLocalState() {}
};

static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                   vector<LogicalType>& return_types, vector<string>& names) {
    auto result = make_uniq<JsonToFastlaneBindData>();
    
    // Handle parameters
    auto& inputs = input.inputs;
    auto& named_parameters = input.named_parameters;
    
    if (inputs.size() < 2) {
        throw BinderException("json_to_fastlane requires at least 2 parameters: input_file, output_file");
    }
    
    if (inputs[0].type() != LogicalType::VARCHAR) {
        throw BinderException("json_to_fastlane input_file must be a string");
    }
    if (inputs[1].type() != LogicalType::VARCHAR) {
        throw BinderException("json_to_fastlane output_file must be a string");
    }
    
    result->input_file = inputs[0].GetValue<string>();
    result->output_file = inputs[1].GetValue<string>();
    
    // Check if auto_detect parameter is provided
    auto auto_detect_it = named_parameters.find("auto_detect");
    if (auto_detect_it != named_parameters.end()) {
        if (auto_detect_it->second.type() != LogicalType::BOOLEAN) {
            throw BinderException("json_to_fastlane auto_detect parameter must be a boolean");
        }
        result->auto_detect = auto_detect_it->second.GetValue<bool>();
    }
    
    // Return a simple status message
    return_types = {LogicalType::VARCHAR};
    names = {"status"};
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context, TableFunctionInitInput& input) {
    return make_uniq<JsonToFastlaneGlobalState>();
}

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                   GlobalTableFunctionState* global_state) {
    return make_uniq<JsonToFastlaneLocalState>();
}

static void Scan(ClientContext& context, TableFunctionInput& data_p, DataChunk& output) {
    auto& bind_data = data_p.bind_data->Cast<JsonToFastlaneBindData>();
    auto& global_state = data_p.global_state->Cast<JsonToFastlaneGlobalState>();
    
    output.Reset();
    
    if (global_state.processed) {
        // Already processed, return empty result
        output.SetCardinality(0);
        return;
    }
    
    try {
        // Use FastLanesFacade to handle the conversion
        FastLanesFacade facade;
        
        // For now, we'll create a simple implementation that uses the FastLanes CLI approach
        // In a real implementation, we'd need to extend the facade to support CSV/JSON conversion
        
        // Return success message (placeholder for now)
        output.SetCardinality(1);
        output.data[0].SetValue(0, Value("JSON to FastLanes conversion not yet implemented - use FastLanes CLI directly"));
        
        global_state.processed = true;
        
    } catch (const std::exception& e) {
        // Return error message
        output.SetCardinality(1);
        output.data[0].SetValue(0, Value("Error: " + string(e.what())));
        
        global_state.processed = true;
    }
}

TableFunction JsonToFastlaneFunction() {
    TableFunction json_to_fastlane("json_to_fastlane", 
                                 {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                                 Scan, Bind, InitGlobal, InitLocal);
    json_to_fastlane.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
    return json_to_fastlane;
}

void RegisterJsonToFastlane(DatabaseInstance& db) {
    auto function = JsonToFastlaneFunction();
    ExtensionUtil::RegisterFunction(db, function);
}

}  // namespace ext_fastlane
}  // namespace duckdb 