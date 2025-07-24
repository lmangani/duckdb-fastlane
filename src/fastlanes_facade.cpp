//===----------------------------------------------------------------------===//
//                         DuckDB - fastlane
//
// fastlanes_facade.cpp
//
// Implementation of FastLanes facade - compiles with C++20
//
//===----------------------------------------------------------------------===//

#include "fastlanes_facade.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "type_mapping.hpp"

// FastLanes C++20 headers - only included in this file
#include "fastlanes.hpp"
#include "fls/connection.hpp"

namespace duckdb {

namespace ext_fastlane {

class FastLanesFacade::Impl {
public:
    std::unique_ptr<fastlanes::Connection> connection;
    std::unique_ptr<fastlanes::TableReader> table_reader;
    std::unique_ptr<fastlanes::RowgroupReader> rowgroup_reader;
    
    // Current state
    std::string current_file_path;
    std::vector<LogicalType> column_types;
    std::vector<std::string> column_names;
    bool file_open = false;
    
    Impl() = default;
    ~Impl() = default;
};

FastLanesFacade::FastLanesFacade() : pImpl(std::make_unique<Impl>()) {}

FastLanesFacade::~FastLanesFacade() = default;

bool FastLanesFacade::openFile(const std::string& file_path) {
    try {
        pImpl->connection = std::make_unique<fastlanes::Connection>();
        pImpl->table_reader = pImpl->connection->read_fls(file_path);
        pImpl->current_file_path = file_path;
        pImpl->file_open = true;
        
        // Get schema information
        auto table = pImpl->table_reader->materialize();
        auto schema = table->get_schema();
        
        pImpl->column_names.clear();
        pImpl->column_types.clear();
        
        for (const auto& column : schema->columns()) {
            pImpl->column_names.push_back(column->name());
            // Convert FastLanes type to DuckDB type
            auto fastlanes_type = column->type();
            pImpl->column_types.push_back(TypeMapping::FastLanesToDuckDB(fastlanes_type));
        }
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

std::vector<LogicalType> FastLanesFacade::getColumnTypes() {
    return pImpl->column_types;
}

std::vector<std::string> FastLanesFacade::getColumnNames() {
    return pImpl->column_names;
}

bool FastLanesFacade::readNextChunk(std::vector<Value>& values, idx_t& rows_read) {
    if (!pImpl->file_open || !pImpl->table_reader) {
        return false;
    }
    
    try {
        // Get next rowgroup
        if (!pImpl->rowgroup_reader) {
            pImpl->rowgroup_reader = pImpl->table_reader->get_rowgroup_reader();
        }
        
        if (!pImpl->rowgroup_reader) {
            return false; // No more data
        }
        
        // Read chunk data
        auto chunk = pImpl->rowgroup_reader->read_chunk();
        if (!chunk) {
            return false; // No more data
        }
        
        // Convert FastLanes data to DuckDB values
        rows_read = chunk->num_rows();
        values.clear();
        values.reserve(rows_read * pImpl->column_types.size());
        
        // TODO: Implement actual data conversion from FastLanes to DuckDB
        // This is a placeholder - actual implementation would convert the chunk data
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

void FastLanesFacade::closeFile() {
    pImpl->rowgroup_reader.reset();
    pImpl->table_reader.reset();
    pImpl->connection.reset();
    pImpl->file_open = false;
    pImpl->current_file_path.clear();
}

bool FastLanesFacade::createFile(const std::string& file_path, 
                                const std::vector<LogicalType>& types,
                                const std::vector<std::string>& names) {
    try {
        pImpl->connection = std::make_unique<fastlanes::Connection>();
        pImpl->column_types = types;
        pImpl->column_names = names;
        pImpl->current_file_path = file_path;
        pImpl->file_open = true;
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

bool FastLanesFacade::writeChunk(const std::vector<Value>& values, idx_t rows) {
    if (!pImpl->file_open || !pImpl->connection) {
        return false;
    }
    
    try {
        // TODO: Implement actual data writing to FastLanes
        // This is a placeholder - actual implementation would convert DuckDB values to FastLanes format
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

void FastLanesFacade::finalizeFile() {
    if (pImpl->file_open && pImpl->connection) {
        // TODO: Implement file finalization
        pImpl->connection->to_fls(pImpl->current_file_path);
    }
    
    pImpl->connection.reset();
    pImpl->file_open = false;
    pImpl->current_file_path.clear();
}

bool FastLanesFacade::isValid() const {
    return pImpl->file_open && pImpl->connection;
}

}  // namespace ext_fastlane
}  // namespace duckdb 