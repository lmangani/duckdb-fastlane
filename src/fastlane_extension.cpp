#define DUCKDB_EXTENSION_MAIN

#include "fastlane_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "scan_fastlanes.hpp"

// FastLanes table function includes
#include "table_function/scan_fastlanes.hpp"
#include "writer/write_fastlane.hpp"
#include "write_fastlane_stream.hpp"
#include "table_function/csv_to_fastlane.hpp"
#include "table_function/json_to_fastlane.hpp"

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
  // Debug output to see if extension is loading
  if (std::getenv("DEBUG")) {
    std::cout << "FastLanes Extension: Loading..." << std::endl;
  }
  
  try {
    // Register version function
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Registering version function..." << std::endl;
    }
    FastlaneVersion::Register(db);
    
    // Register table functions
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Registering scan_fastlanes function..." << std::endl;
    }
          ext_fastlane::RegisterScanFastlanesStream(db);
    
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Registering write_fastlane function..." << std::endl;
    }
    ext_fastlane::WriteFastlaneFunction::RegisterWriteFastlaneFunction(db);
    
    // Register copy functions
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Registering copy functions..." << std::endl;
    }
    ext_fastlane::RegisterFastlaneStreamCopyFunction(db);
    
    // Register converter functions
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Registering converter functions..." << std::endl;
    }
    ext_fastlane::RegisterCsvToFastlane(db);
    ext_fastlane::RegisterJsonToFastlane(db);
    
    if (std::getenv("DEBUG")) {
      std::cout << "FastLanes Extension: Loaded successfully!" << std::endl;
    }
  } catch (const std::exception& e) {
    if (std::getenv("DEBUG")) {
      std::cerr << "FastLanes Extension: Error loading: " << e.what() << std::endl;
    }
    throw;
  }
}

}  // namespace

void FastlaneExtension::Load(DuckDB &db) {
    // Register the scan_fastlanes table function
    ScanFastLanes::Register(*db.instance);
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

DUCKDB_EXTENSION_API void fastlane_init(duckdb::DatabaseInstance &db) {
    duckdb::FastlaneExtension extension;
    duckdb::DuckDB db_wrapper(db);
    extension.Load(db_wrapper);
}

DUCKDB_EXTENSION_API const char *fastlane_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
