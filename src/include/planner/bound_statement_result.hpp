#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundStatementResult {
public:
    BoundStatementResult() = default;
    explicit BoundStatementResult(Expressions columns, std::vector<std::string> columnNames)
        : columns{std::move(columns)}, columnNames{std::move(columnNames)} {}

    static std::shared_ptr<BoundStatementResult> createEmptyResult() { 
        return std::make_shared<BoundStatementResult>();
    }

    void addColumn(const std::string& columnName, std::shared_ptr<Expression> column) {
        columns.push_back(std::move(column));
        columnNames.push_back(columnName);
    }
    Expressions getColumns() const { return columns; }
    std::vector<std::string> getColumnNames() const { return columnNames; }
    std::vector<LogicalType> getColumnTypes() const {
        std::vector<LogicalType> columnTypes;
        for (auto& column : columns) {
            columnTypes.push_back(column->return_type);
        }
        return columnTypes;
    }

    std::shared_ptr<Expression> getSingleColumnExpr() const {
        return columns[0];
    }

private:
    BoundStatementResult(const BoundStatementResult& other)
        : columns{other.columns}, columnNames{other.columnNames} {}

private:
    Expressions columns;
    // ColumnNames might be different from column.toString() because the same column might have
    // different aliases, e.g. RETURN id AS a, id AS b
    // For both columns we currently refer to the same id expr object so we cannot resolve column
    // name properly from expression object.
    std::vector<std::string> columnNames;
};

}