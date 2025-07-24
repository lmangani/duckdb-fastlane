#define DUCKDB_EXTENSION_MAIN

#include "fastlane_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// FastLanes table function includes
#include "table_function/read_fastlane.hpp"
#include "writer/write_fastlane.hpp"
#include "write_fastlane_stream.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

namespace {

struct FastlaneVersion {
  static void Register(DatabaseInstance& db) {
    auto fn = ScalarFunction("fastlane_version", {}, LogicalType::VARCHAR, ExecuteFn);
    ExtensionUtil::RegisterFunction(db, fn);
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    result.SetValue(0, StringVector::AddString(result, "FastLanes Extension v1.0.0"));
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
};

void LoadInternal(DatabaseInstance& db) {
  // Register version function
  FastlaneVersion::Register(db);
  
  // Register table functions
  ext_fastlane::RegisterReadFastlaneStream(db);
  ext_fastlane::WriteFastlaneFunction::RegisterWriteFastlaneFunction(db);
  
  // Register copy functions
  ext_fastlane::RegisterFastlaneStreamCopyFunction(db);
}

}  // namespace

void FastlaneExtension::Load(DuckDB& db) {
  LoadInternal(*db.instance);
}

std::string FastlaneExtension::Name() {
  return "fastlane";
}

std::string FastlaneExtension::Version() const {
#ifdef EXT_VERSION_FASTLANE
  return EXT_VERSION_FASTLANE;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void fastlane_init(duckdb::DatabaseInstance& db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::FastlaneExtension>();
}

DUCKDB_EXTENSION_API const char* fastlane_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
