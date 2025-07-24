//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// table_function/read_fastlane.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
namespace ext_fastlane {

TableFunction ReadFastlaneStreamFunction();

void RegisterReadFastlaneStream(DatabaseInstance& db);

}  // namespace ext_fastlane
}  // namespace duckdb 