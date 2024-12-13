#pragma once

#include <string>
#include <memory>
#include <string>
#include "cache/disk_aio/TypeDef.hpp"
#include "common/types.hpp"
#include "common/boost_typedefs.hpp"
#include "types/data_chunk.hpp"

namespace s62 {
    class Planner;
}

namespace duckdb {

class DuckDB;
class ClientContext;
class CypherPipelineExecutor;

typedef string LabelName;
typedef string TypeName;
typedef string PropertyName;
typedef string PropertySQLType;
typedef vector<PropertyName> PropertyNames;
typedef vector<PropertySQLType> PropertySQLTypes;
typedef vector<duckdb::LogicalType> LogicalTypes;
typedef size_t ResultSetSize;

struct QueryResultSetWrapper {
    ResultSetSize result_set_size = 0;
    PropertyNames property_names;
    std::vector<DataChunk*> result_chunks;

    void SetPropertyNames(PropertyNames& stored_names) {
        property_names.reserve(stored_names.size());
        for (auto& stored_name: stored_names) {
            property_names.push_back(move(stored_name));
        }
    }

    void SetResultChunks(vector<unique_ptr<DataChunk>>& results) {
        for (size_t i = 0; i < results.size(); i++) {
            result_chunks.push_back(new DataChunk());
        }
        for (size_t i = 0; i < results.size(); i++) {
            result_chunks[i]->Move(*results[i]);
        }
        for (auto &it : result_chunks) {
            result_set_size += it->size();
        }
    }
};

class S62SocketAPIs {
public:
    S62SocketAPIs(std::string& workspace);
	S62SocketAPIs(DiskAioParameters& config);
    ~S62SocketAPIs();
    void ExecuteStatement(std::string& query, QueryResultSetWrapper& query_result_set_wrapper);

private:
    void SetupDefaultConfig();
    void SetupDatabase();
    void CompileQuery(std::string& query);

private:
    std::unique_ptr<DuckDB> database;
    std::shared_ptr<ClientContext> client;
    s62::Planner* planner;
    DiskAioParameters config;
};

}