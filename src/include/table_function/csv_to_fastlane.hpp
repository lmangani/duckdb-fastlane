#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

namespace ext_fastlane {

TableFunction CsvToFastlaneFunction();
void RegisterCsvToFastlane(DatabaseInstance& db);

}  // namespace ext_fastlane
}  // namespace duckdb 