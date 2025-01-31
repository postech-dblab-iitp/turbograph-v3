#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "kuzu/binder/bound_statement.h"
#include "kuzu/catalog/catalog_structs.h"
#include "kuzu/common/csv_reader/csv_reader.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class BoundCopyCSV : public BoundStatement {
public:
    BoundCopyCSV(CSVDescription csvDescription, TableSchema tableSchema)
        : BoundStatement{StatementType::COPY_CSV,
              BoundStatementResult::createSingleStringColumnResult()},
          csvDescription{std::move(csvDescription)}, tableSchema{std::move(tableSchema)} {}

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline TableSchema getTableSchema() const { return tableSchema; }

private:
    CSVDescription csvDescription;
    TableSchema tableSchema;
};

} // namespace binder
} // namespace kuzu
