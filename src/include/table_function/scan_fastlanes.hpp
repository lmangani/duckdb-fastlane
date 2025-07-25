//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// table_function/scan_fastlanes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
namespace ext_fastlane {

TableFunction ScanFastlanesStreamFunction();

void RegisterScanFastlanesStream(DatabaseInstance& db);

}  // namespace ext_fastlane
}  // namespace duckdb 