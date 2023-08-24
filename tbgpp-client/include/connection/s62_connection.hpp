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

class S62Connection {
public:
    S62Connection(std::string& workspace);
	S62Connection(DiskAioParameters& config);
    ~S62Connection();
	void GetNodesMetadata(NodeMetadataList& node_metadata_list);
	void GetEdgesMetadata(EdgeMetadataList& edge_metadata_list);
	void GetQueryResultsMetadata(std::string& query, QueryResultSetMetadata& query_result_set_metadata);
    CypherPreparedStatement PrepareStatement(std::string& query);
    void ExecuteStatement(CypherPreparedStatement& prepared_statement, size_t& result_count);

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