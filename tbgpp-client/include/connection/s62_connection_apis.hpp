#pragma once

#include <memory>
#include <string>
#include "metadata.hpp"
#include "cache/disk_aio/TypeDef.hpp"
#include "cypher_prepared_statement.hpp"

namespace s62 {
    class Planner;
}

namespace duckdb {

class DuckDB;
class ClientContext;
class CypherPipelineExecutor;

class S62ConnectionAPIs {
public:
    S62ConnectionAPIs(std::string& workspace);
	S62ConnectionAPIs(DiskAioParameters& config);
    ~S62ConnectionAPIs();
	void GetNodesMetadata(NodeMetadataList& node_metadata_list);
	void GetEdgesMetadata(EdgeMetadataList& edge_metadata_list);
    std::unique_ptr<CypherPreparedStatement> PrepareStatement(std::string& query);
    void ExecuteStatement(std::string& query, QueryResultSetMetadata& query_result_set_metadata);

private:
    void SetupDefaultConfig();
    void SetupDatabase();
    void CompileQuery(std::string& query);
    size_t PrintResult(vector<CypherPipelineExecutor*>& executors);

private:
    std::unique_ptr<DuckDB> database;
    std::shared_ptr<ClientContext> client;
    s62::Planner* planner;
    DiskAioParameters config;
};

}