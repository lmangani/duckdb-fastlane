//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// write_fastlane_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/copy_function.hpp"

namespace duckdb {
namespace ext_fastlane {

void RegisterFastlaneStreamCopyFunction(DatabaseInstance& db);

}  // namespace ext_fastlane
}  // namespace duckdb 