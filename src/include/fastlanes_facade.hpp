//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// fastlanes_facade.hpp
//
// C++20 facade using PIMPL pattern to isolate FastLanes headers
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

namespace ext_fastlane {

// Forward declarations - no C++20 headers included here
class FastLanesFacade {
private:
    class Impl; // Forward declaration
    std::unique_ptr<Impl> pImpl;
    
public:
    FastLanesFacade();
    ~FastLanesFacade();
    
    // Reading operations
    bool openFile(const std::string& file_path);
    std::vector<LogicalType> getColumnTypes();
    std::vector<std::string> getColumnNames();
    bool readNextChunk(std::vector<Value>& values, idx_t& rows_read);
    void closeFile();
    
    // Writing operations
    bool createFile(const std::string& file_path, 
                   const std::vector<LogicalType>& types,
                   const std::vector<std::string>& names);
    bool writeChunk(DataChunk &chunk);
    void finalizeFile();
    
    // Utility
    bool isValid() const;
};

}  // namespace ext_fastlane
}  // namespace duckdb 