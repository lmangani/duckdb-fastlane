#include "fastlanes_facade.hpp"
#include "fastlanes.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector.hpp"
#include <memory>
#include <variant>
#include <iostream>

namespace duckdb {

struct FastLanesFacade::Impl {
    std::unique_ptr<fastlanes::Connection> connection;
    std::unique_ptr<fastlanes::TableReader> table_reader;
    std::unique_ptr<fastlanes::RowgroupReader> rowgroup_reader;
    std::unique_ptr<fastlanes::Rowgroup> rowgroup;
    bool initialized = false;
    size_t current_row = 0;
    size_t total_rows = 0;
};

FastLanesFacade::FastLanesFacade() : pImpl(std::make_unique<Impl>()) {}

FastLanesFacade::~FastLanesFacade() = default;

bool FastLanesFacade::openFile(const std::string& filename) {
    try {
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Opening FastLanes file: " << filename << std::endl;
        }
        
        // Create connection and read FLS file
        pImpl->connection = fastlanes::connect();
        pImpl->table_reader = pImpl->connection->read_fls(filename);
        
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: FLS file opened successfully" << std::endl;
        }
        
        // Get the first rowgroup reader
        pImpl->rowgroup_reader = pImpl->table_reader->get_rowgroup_reader(0);
        
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Got rowgroup reader" << std::endl;
        }
        
        // Materialize the rowgroup - this is the key step
        pImpl->rowgroup = pImpl->rowgroup_reader->materialize();
        
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Materialized rowgroup successfully" << std::endl;
            std::cerr << "DEBUG: Rowgroup has " << pImpl->rowgroup->ColCount() << " columns" << std::endl;
            std::cerr << "DEBUG: Rowgroup has " << pImpl->rowgroup->RowCount() << " rows" << std::endl;
        }
        
        pImpl->total_rows = pImpl->rowgroup->RowCount();
        pImpl->initialized = true;
        pImpl->current_row = 0;
        
        return true;
    } catch (const std::exception& e) {
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Error opening file: " << e.what() << std::endl;
        }
        return false;
    }
}

bool FastLanesFacade::readNextChunk(DataChunk& result) {
    if (!pImpl->initialized || !pImpl->rowgroup) {
        return false;
    }
    
    try {
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Reading chunk, current_row=" << pImpl->current_row 
                      << ", total_rows=" << pImpl->total_rows << std::endl;
        }
        
        // Check if we've read all rows
        if (pImpl->current_row >= pImpl->total_rows) {
            return false;
        }
        
        const size_t num_columns = pImpl->rowgroup->ColCount();
        const size_t chunk_size = std::min(static_cast<size_t>(STANDARD_VECTOR_SIZE), 
                                          pImpl->total_rows - pImpl->current_row);
        
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Reading chunk of " << chunk_size << " rows" << std::endl;
        }
        
        // Initialize the result DataChunk with the correct schema
        if (result.ColumnCount() == 0) {
            // If the result is not initialized, initialize it with the rowgroup schema
            std::vector<LogicalType> types(num_columns, LogicalType::VARCHAR);
            result.InitializeEmpty(duckdb::vector<LogicalType>(types.begin(), types.end()));
        }
        result.SetCardinality(chunk_size);
        
        // We just need to make sure we don't exceed the number of columns in the result
        size_t result_columns = result.ColumnCount();
        size_t columns_to_process = std::min(num_columns, result_columns);
        
        // Extract data from each column
        for (size_t col_idx = 0; col_idx < columns_to_process; ++col_idx) {
            if (getenv("DEBUG")) {
                std::cerr << "DEBUG: Processing column " << col_idx << std::endl;
            }
            
            // Access the column data using std::visit
            std::visit([&](const auto& column_ptr) {
                if constexpr (std::is_same_v<std::decay_t<decltype(column_ptr)>, std::monostate>) {
                    // Handle monostate (null column)
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        result.data[col_idx].SetValue(row_idx, Value());
                    }
                } else {
                    using ColumnType = std::decay_t<decltype(*column_ptr)>;
                
                if (getenv("DEBUG")) {
                    std::cerr << "DEBUG: Column " << col_idx << " type: " << typeid(ColumnType).name() << std::endl;
                }
                
                if constexpr (std::is_same_v<ColumnType, fastlanes::col_i32>) {
                    // Integer column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->data.size()) {
                            result.data[col_idx].SetValue(row_idx, Value::INTEGER(column_ptr->data[data_idx]));
                        }
                    }
                } else if constexpr (std::is_same_v<ColumnType, fastlanes::col_i64>) {
                    // Bigint column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->data.size()) {
                            result.data[col_idx].SetValue(row_idx, Value::BIGINT(column_ptr->data[data_idx]));
                        }
                    }
                } else if constexpr (std::is_same_v<ColumnType, fastlanes::flt_col_t>) {
                    // Float column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->data.size()) {
                            result.data[col_idx].SetValue(row_idx, Value::FLOAT(column_ptr->data[data_idx]));
                        }
                    }
                } else if constexpr (std::is_same_v<ColumnType, fastlanes::dbl_col_t>) {
                    // Double column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->data.size()) {
                            result.data[col_idx].SetValue(row_idx, Value::DOUBLE(column_ptr->data[data_idx]));
                        }
                    }
                } else if constexpr (std::is_same_v<ColumnType, fastlanes::str_col_t>) {
                    // String column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->data.size()) {
                            result.data[col_idx].SetValue(row_idx, Value(column_ptr->data[data_idx]));
                        }
                    }
                } else if constexpr (std::is_same_v<ColumnType, fastlanes::FLSStrColumn>) {
                    // FLS string column
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        size_t data_idx = pImpl->current_row + row_idx;
                        if (data_idx < column_ptr->fls_str_arr.size()) {
                            result.data[col_idx].SetValue(row_idx, Value(column_ptr->fls_str_arr[data_idx].to_string()));
                        }
                    }
                } else {
                    // Unknown type - set NULL
                    if (getenv("DEBUG")) {
                        std::cerr << "DEBUG: Unknown column type for column " << col_idx << std::endl;
                    }
                    for (size_t row_idx = 0; row_idx < chunk_size; ++row_idx) {
                        result.data[col_idx].SetValue(row_idx, Value());
                    }
                }
                }
            }, pImpl->rowgroup->internal_rowgroup[col_idx]);
        }
        
        pImpl->current_row += chunk_size;
        
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Successfully read chunk, new current_row=" << pImpl->current_row << std::endl;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        if (getenv("DEBUG")) {
            std::cerr << "DEBUG: Error reading chunk: " << e.what() << std::endl;
        }
        return false;
    }
}

void FastLanesFacade::closeFile() {
    pImpl->connection.reset();
    pImpl->table_reader.reset();
    pImpl->rowgroup_reader.reset();
    pImpl->rowgroup.reset();
    pImpl->initialized = false;
    pImpl->current_row = 0;
    pImpl->total_rows = 0;
}

} // namespace duckdb 