#include "connection/s62_connection.hpp"
#include "main/database.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog_wrapper.hpp"
#include "planner/planner.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "tbgppdbwrappers.hpp"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "gpopt/mdcache/CMDCache.h"

using namespace antlr4;
using namespace gpopt;

namespace duckdb {

typedef vector<idx_t> PartitionIndexes;
typedef vector<idx_t> PropertySchemaIDs;
typedef vector<PartitionIndexes> PartitionIndexesList;
typedef vector<std::string> Labels;
typedef vector<std::string> Types;

S62Connection::S62Connection(std::string& workspace) {
    SetupDefaultConfig();
	config.WORKSPACE = workspace;
    SetupDatabase();
}

S62Connection::S62Connection(DiskAioParameters& config): config(config) {
    SetupDatabase();
}

S62Connection::~S62Connection(){
  	delete ChunkCacheManager::ccm;
    std::cout << "Database Disconnected" << std::endl;
}

void S62Connection::SetupDefaultConfig() {
    config.NUM_THREADS = 1;
    config.NUM_TOTAL_CPU_CORES = 1;
    config.NUM_CPU_SOCKETS = 1;
    config.NUM_DISK_AIO_THREADS = config.NUM_CPU_SOCKETS * 2;
}

void S62Connection::SetupDatabase() {
    // create disk aio factory
    int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(config.NUM_THREADS);

    // create db
    database = std::make_unique<DuckDB>(config.WORKSPACE.c_str());

    // create cache manager
	ChunkCacheManager::ccm = new ChunkCacheManager(config.WORKSPACE.c_str());

    // craet client
	client = std::make_shared<ClientContext>(database->instance->shared_from_this());
    duckdb::SetClientWrapper(client, make_shared<CatalogWrapper>( database->instance->GetCatalogWrapper()));

    // create planner
    auto planner_config = s62::PlannerConfig();
    planner_config.INDEX_JOIN_ONLY = true;
    planner = new s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());

    // Print done
    std::cout << "Database Connected" << std::endl;
}

void S62Connection::GetNodesMetadata(NodeMetadataList& node_metadata_list) {
    // Get graph catalog
    ClientContext& client_context = *(client.get());
    Catalog& catalog = client->db->GetCatalog();
    GraphCatalogEntry* graph_cat_entry = 
        (GraphCatalogEntry*) catalog.GetEntry(client_context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, "graph1");

    // Get labels and partition indexes
    Labels vertex_labels;
    PartitionIndexesList vertex_partition_indexes_list;
    graph_cat_entry->GetVertexLabels(vertex_labels);
    vertex_partition_indexes_list.resize(vertex_labels.size(), PartitionIndexes());
    
    for (int i = 0; i < vertex_labels.size(); i++) {
        auto& vertex_label = vertex_labels[i];
        graph_cat_entry->GetVertexPartitionIndexesInLabel(client_context, vertex_label, vertex_partition_indexes_list[i]);
    }

    // Obtain metadata
    node_metadata_list.resize(vertex_labels.size(), NodeMetadata());
    for (int i = 0; i < vertex_labels.size(); i++) {
        auto& vertex_label = vertex_labels[i];
        auto& node_metadata = node_metadata_list[i];
        auto& partition_indexes = vertex_partition_indexes_list[i];
        for (auto& partition_index: partition_indexes) { // assume size of 1
            PartitionCatalogEntry* partition_cat_entry = (PartitionCatalogEntry*) catalog.GetEntry(client_context, DEFAULT_SCHEMA, partition_index);
            node_metadata.SetPropertyNames(partition_cat_entry->global_property_key_names);
            node_metadata.SetPropertySQLTypes(partition_cat_entry->global_property_typesid);
        }
    }
}

void S62Connection::GetEdgesMetadata(EdgeMetadataList& edge_metadata_list) {
    
    // Get graph catalog
    ClientContext& client_context = *(client.get());
    Catalog& catalog = client->db->GetCatalog();
    GraphCatalogEntry* graph_cat_entry = 
        (GraphCatalogEntry*) catalog.GetEntry(client_context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, "graph1");

    // Get types and partition indexes
    Types edge_types;
    PartitionIndexesList edge_partition_indexes_list;
    graph_cat_entry->GetEdgeTypes(edge_types);
    edge_partition_indexes_list.resize(edge_types.size(), PartitionIndexes());
    
    for (int i = 0; i < edge_types.size(); i++) {
        auto& edge_type = edge_types[i];
        graph_cat_entry->GetEdgePartitionIndexesInType(client_context, edge_type, edge_partition_indexes_list[i]);
    }

    // Obtain metadata
    edge_metadata_list.resize(edge_types.size(), EdgeMetadata());
    for (int i = 0; i < edge_types.size(); i++) {
        auto& edge_type = edge_types[i];
        auto& edge_metadata = edge_metadata_list[i];
        auto& partition_indexes = edge_partition_indexes_list[i];
        for (auto& partition_index: partition_indexes) { // assume size of 1
            PartitionCatalogEntry* partition_cat_entry = (PartitionCatalogEntry*) catalog.GetEntry(client_context, DEFAULT_SCHEMA, partition_index);
            edge_metadata.SetPropertyNames(partition_cat_entry->global_property_key_names);
            edge_metadata.SetPropertySQLTypes(partition_cat_entry->global_property_typesid);
        }
    }
}

void S62Connection::GetQueryResultsMetadata(std::string& query, QueryResultSetMetadata& query_result_set_metadata) {
    CompileQuery(query);
    auto executors = planner->genPipelineExecutors();
    if (executors.size() == 0) { std::cout << "Plan empty" << std::endl; return; }
    else {
        auto& output_schema = executors[executors.size()-1]->pipeline->GetSink()->schema;
        auto stored_names = output_schema.getStoredColumnNames();
        auto stored_types = output_schema.getStoredTypes();
        query_result_set_metadata.SetPropertyNames(stored_names);
        query_result_set_metadata.SetPropertySQLTypes(stored_types);
    }
}

void S62Connection::CompileQuery(std::string& query) {
    auto inputStream = ANTLRInputStream(query);
    auto cypherLexer = CypherLexer(&inputStream);
    auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

    auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);
    kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
    auto statement = transformer.transform();
    
    auto binder = kuzu::binder::Binder(client.get());
    auto boundStatement = binder.bind(*statement);
    kuzu::binder::BoundStatement * bst = boundStatement.get();

	planner->execute(bst);
}

}