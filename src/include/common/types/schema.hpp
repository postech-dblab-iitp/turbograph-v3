#pragma once

#include <vector>
#include "common/types.hpp"

namespace duckdb {


class Schema {
   public:
    void setStoredTypes(std::vector<duckdb::LogicalType> types);
    void appendStoredTypes(std::vector<duckdb::LogicalType> types);
    std::vector<duckdb::LogicalType> getStoredTypes();
    std::vector<duckdb::LogicalType> &getStoredTypesRef();
    void setStoredColumnNames(std::vector<std::string> &names);
    std::vector<std::string> getStoredColumnNames();
    void removeColumn(uint64_t col_idx);

    std::string printStoredTypes();
    std::string printStoredColumnAndTypes();

    //! get the size of stored types
    uint64_t getStoredTypesSize() { return stored_types_size; }

   public:
    std::vector<duckdb::LogicalType> stored_types;
    std::vector<std::string> stored_column_names;
    uint64_t stored_types_size;
};


}  // namespace duckdb