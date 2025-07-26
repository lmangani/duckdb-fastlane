#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <memory>
#include <string>

namespace fastlanes {
    class Connection;
    class TableReader;
    class RowgroupReader;
    class Rowgroup;
}

namespace duckdb {

class FastLanesFacade {
public:
    FastLanesFacade();
    ~FastLanesFacade();

    // Reading methods
    bool openFile(const std::string& filename);
    bool readNextChunk(DataChunk& result);
    void closeFile();

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace duckdb 