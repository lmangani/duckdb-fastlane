#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ScanFastLanes {
public:
    static void Register(DatabaseInstance& db);
};

} // namespace duckdb 