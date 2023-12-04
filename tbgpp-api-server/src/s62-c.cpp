#include <memory>
#include <string>
#include "capi_internal.hpp"
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
#include "types/decimal.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "catalog/catalog_entry/list.hpp"

using namespace duckdb;
using namespace antlr4;
using namespace gpopt;

// Database
static std::unique_ptr<DuckDB> database;
static std::shared_ptr<ClientContext> client;
static s62::Planner* planner;
static DiskAioFactory* disk_aio_factory;

// Error Handling
static s62_error_code last_error_code = S62_NO_ERROR;
static std::string last_error_message;

// Message Constants
static const std::string INVALID_METADATA_MSG = "Invalid metadata";
static const std::string UNSUPPORTED_OPERATION_MSG = "Unsupported operation";
static const std::string INVALID_LABEL_MSG = "Invalid label";
static const std::string INVALID_METADATA_TYPE_MSG = "Invalid metadata type";
static const std::string INVALID_NUMBER_OF_PROPERTIES_MSG = "Invalid number of properties";
static const std::string INVALID_PROPERTY_MSG = "Invalid property";
static const std::string INVALID_PLAN_MSG = "Invalid plan";
static const std::string INVALID_PREPARED_STATEMENT_MSG = "Invalid prepared statement";
static const std::string INVALID_RESULT_SET_MSG = "Invalid result set";

s62_state s62_connect(const char *dbname) {
    try
    {
        // Setup configurations
        DiskAioParameters config;
        config.WORKSPACE = dbname;
        config.NUM_THREADS = 1;
        config.NUM_TOTAL_CPU_CORES = 1;
        config.NUM_CPU_SOCKETS = 1;
        config.NUM_DISK_AIO_THREADS = config.NUM_CPU_SOCKETS * 2;

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
		planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_IN_QUERY;
		planner_config.DEBUG_PRINT = false;
        planner = new s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());

        // Print done
        std::cout << "Database Connected" << std::endl;
        
        return S62_SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        last_error_message = e.what();
        last_error_code = S62_ERROR_CONNECTION_FAILED;

        return S62_ERROR;
    }
}

void s62_disconnect() {
    delete ChunkCacheManager::ccm;
    delete planner;
	delete disk_aio_factory;
    database.reset();
    client.reset();
    std::cout << "Database Disconnected" << std::endl;
}

s62_conn_state s62_is_connected() {
    if (database != NULL) {
        return S62_CONNECTED;
    } else {
        return S62_NOT_CONNECTED;
    }
}

s62_error_code s62_get_last_error(char **errmsg) {
    *errmsg = (char*)last_error_message.c_str();
    return last_error_code;
}

s62_version s62_get_version() {
    return "0.0.1";
}

inline static GraphCatalogEntry* s62_get_graph_catalog_entry() {
    Catalog& catalog = client->db->GetCatalog();
	return (GraphCatalogEntry*) catalog.GetEntry(*client.get(), CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
}

inline static PartitionCatalogEntry* s62_get_vertex_partition_catalog_entry(string label) {
	Catalog& catalog = client->db->GetCatalog();
	return (PartitionCatalogEntry*) catalog.GetEntry(*client.get(), CatalogType::PARTITION_ENTRY, 
								DEFAULT_SCHEMA, DEFAULT_VERTEX_PARTITION_PREFIX + label);
}

inline static PartitionCatalogEntry* s62_get_edge_partition_catalog_entry(string type) {
	Catalog& catalog = client->db->GetCatalog();
	return (PartitionCatalogEntry*) catalog.GetEntry(*client.get(), CatalogType::PARTITION_ENTRY, 
								DEFAULT_SCHEMA, DEFAULT_EDGE_PARTITION_PREFIX + type);
}

s62_num_metadata s62_get_metadata_from_catalog(s62_label_name label, bool like_flag, bool filter_flag, s62_metadata **_metadata) {
	if(label != NULL && !like_flag && !filter_flag) {
        last_error_message = UNSUPPORTED_OPERATION_MSG;
		last_error_code = S62_ERROR_UNSUPPORTED_OPERATION;
		return last_error_code;
	}

	// Get nodes metadata
	auto graph_cat = s62_get_graph_catalog_entry();

	// Get labels and types
	vector<string> labels, types;
	graph_cat->GetVertexLabels(labels);
	graph_cat->GetEdgeTypes(types);

	// Create s62_metadata linked list with labels
	s62_metadata *metadata = NULL;
	s62_metadata *prev = NULL;
	for(int i = 0; i < labels.size(); i++) {
		s62_metadata *new_metadata = (s62_metadata*)malloc(sizeof(s62_metadata));
		new_metadata->label_name = strdup(labels[i].c_str());
		new_metadata->type = S62_METADATA_TYPE::S62_NODE;
		new_metadata->next = NULL;
		if(prev == NULL) {
			metadata = new_metadata;
		} else {
			prev->next = new_metadata;
		}
		prev = new_metadata;
	}

	// Concat types
	for(int i = 0; i < types.size(); i++) {
		s62_metadata *new_metadata = (s62_metadata*)malloc(sizeof(s62_metadata));
		new_metadata->label_name = strdup(types[i].c_str());
		new_metadata->type = S62_METADATA_TYPE::S62_EDGE;
		new_metadata->next = NULL;
		if(prev == NULL) {
			metadata = new_metadata;
		} else {
			prev->next = new_metadata;
		}
		prev = new_metadata;
	}

	*_metadata = metadata;
	return labels.size() + types.size();
}

s62_state s62_close_metadata(s62_metadata *metadata) {
	if (metadata == NULL) {
		last_error_message = INVALID_METADATA_MSG.c_str();
		last_error_code = S62_ERROR_INVALID_METADATA;
		return S62_ERROR;
	}

	s62_metadata *next;
	while(metadata != NULL) {
		next = metadata->next;
		free(metadata->label_name);
		free(metadata);
		metadata = next;
	}
	return S62_SUCCESS;
}

static void s62_extract_width_scale_from_uint16(s62_property* property, uint16_t width_scale) {
	uint8_t width = width_scale >> 8;
	uint8_t scale = width_scale & 0xFF;
	property->precision = width;
	property->scale = scale;
}

static void s62_extract_width_scale_from_type(s62_property* property, LogicalType property_logical_type) {
	uint8_t width, scale;
	if (property_logical_type.GetDecimalProperties(width, scale)) {
		property->precision = width;
		property->scale = scale;
	} else {
		property->precision = 0;
		property->scale = 0;
	}
}

s62_num_properties s62_get_property_from_catalog(s62_label_name label, s62_metadata_type type, s62_property** _property) {
	if (label == NULL) {
		last_error_message = INVALID_LABEL_MSG;
		last_error_code = S62_ERROR_INVALID_LABEL;
		return S62_ERROR;
	}

	auto graph_cat_entry = s62_get_graph_catalog_entry();
	vector<idx_t> partition_indexes;
	s62_property *property = NULL;
	s62_property *prev = NULL;
	PartitionCatalogEntry *partition_cat_entry = NULL;

	if (type == S62_METADATA_TYPE::S62_NODE) {
		partition_cat_entry = s62_get_vertex_partition_catalog_entry(string(label));
	} else if (type == S62_METADATA_TYPE::S62_EDGE) {
		partition_cat_entry = s62_get_edge_partition_catalog_entry(string(label));
	} else {
		last_error_message = INVALID_METADATA_TYPE_MSG;
		last_error_code = S62_ERROR_INVALID_METADATA_TYPE;
		return S62_ERROR;
	}

	auto num_properties = partition_cat_entry->global_property_key_names.size();
	auto num_types = partition_cat_entry->global_property_typesid.size();

	if (num_properties != num_types) {
		last_error_message = INVALID_NUMBER_OF_PROPERTIES_MSG;
		last_error_code = S62_ERROR_INVALID_NUMBER_OF_PROPERTIES;
		return S62_ERROR;
	}

	for (s62_property_order i = 0; i < num_properties; i++) {
		auto property_name = partition_cat_entry->global_property_key_names[i];
		auto property_logical_type_id = partition_cat_entry->global_property_typesid[i];
		auto property_logical_type = LogicalType(property_logical_type_id);
		auto property_s62_type = ConvertCPPTypeToC(property_logical_type);
		auto property_sql_type = property_logical_type.ToString();

		s62_property *new_property = (s62_property*)malloc(sizeof(s62_property));
		new_property->label_name = label;
		new_property->label_type = type;
		new_property->order = i;
		new_property->property_name = strdup(property_name.c_str());
		new_property->property_type = property_s62_type;
		new_property->property_sql_type = strdup(property_sql_type.c_str());

		if (property_logical_type_id == LogicalTypeId::DECIMAL) {
			s62_extract_width_scale_from_uint16(new_property, partition_cat_entry->extra_typeinfo_vec[i]);
		} else {
			s62_extract_width_scale_from_type(new_property, property_logical_type);
		}

		new_property->next = NULL;
		if (prev == NULL) {
			property = new_property;
		} else {
			prev->next = new_property;
		}
		prev = new_property;
	}

	*_property = property;
	return num_properties;
}

s62_state s62_close_property(s62_property *property) {
	if (property == NULL) {
		last_error_message = INVALID_PROPERTY_MSG;
		last_error_code = S62_ERROR_INVALID_PROPERTY;
		return S62_ERROR;
	}

	s62_property *next;
	while(property != NULL) {
		next = property->next;
		free(property->property_name);
		free(property->property_sql_type);
		free(property);
		property = next;
	}
	return S62_SUCCESS;
}

static void s62_compile_query(string query) {
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

static void s62_get_label_name_type_from_ccolref(CColRef* col_ref, s62_property *new_property) {
	if (((CColRefTable*) col_ref)->GetMdidTable() != NULL) {
		OID table_obj_id = CMDIdGPDB::CastMdid(((CColRefTable*) col_ref)->GetMdidTable())->Oid();
		PropertySchemaCatalogEntry *ps_cat_entry = (PropertySchemaCatalogEntry *)client->db->GetCatalog().GetEntry(*client.get(), DEFAULT_SCHEMA, table_obj_id);
		D_ASSERT(ps_cat_entry != NULL);
		idx_t partition_oid = ps_cat_entry->partition_oid;
		auto graph_cat = s62_get_graph_catalog_entry();

		string label = graph_cat->GetLabelFromVertexPartitionIndex(*(client.get()), partition_oid);
		if (!label.empty()) {
			new_property->label_name = strdup(label.c_str());
			new_property->label_type = S62_METADATA_TYPE::S62_NODE;
			return;
		}

		string type = graph_cat->GetTypeFromEdgePartitionIndex(*(client.get()), partition_oid);
		if (!type.empty()) {
			new_property->label_name = strdup(type.c_str());
			new_property->label_type = S62_METADATA_TYPE::S62_EDGE;
			return;
		}
	}

	new_property->label_name = NULL;
	new_property->label_type = S62_METADATA_TYPE::S62_OTHER;
	return;
}

static void s62_extract_query_metadata(s62_prepared_statement* prepared_statement) {
    auto executors = planner->genPipelineExecutors();
	 if (executors.size() == 0) {
		last_error_message = INVALID_PLAN_MSG;
		last_error_code = S62_ERROR_INVALID_PLAN;
		return;
    }
    else {
		auto col_names = planner->getQueryOutputColNames();
		auto col_types = executors.back()->pipeline->GetSink()->schema.getStoredTypes();
		auto logical_schema = planner->getQueryOutputSchema();

		s62_property *property = NULL;
		s62_property *prev = NULL;

		for (s62_property_order i = 0; i < col_names.size(); i++) {
			s62_property *new_property = (s62_property*)malloc(sizeof(s62_property));

			auto property_name = col_names[i];
			auto property_logical_type = col_types[i];
			auto property_s62_type = ConvertCPPTypeToC(property_logical_type);
			auto property_sql_type = property_logical_type.ToString();

			new_property->order = i;
			new_property->property_name = strdup(property_name.c_str());
			new_property->property_type = property_s62_type;
			new_property->property_sql_type = strdup(property_sql_type.c_str());

			s62_get_label_name_type_from_ccolref(logical_schema.getColRefofIndex(i), new_property);
			s62_extract_width_scale_from_type(new_property, property_logical_type);

			new_property->next = NULL;
			if (prev == NULL) {
				property = new_property;
			} else {
				prev->next = new_property;
			}
			prev = new_property;
		}

		prepared_statement->num_properties = col_names.size();
		prepared_statement->property = property;
		prepared_statement->plan = strdup(jsonifyQueryPlan(executors).c_str());
    }
}

s62_prepared_statement* s62_prepare(s62_query query) {
	auto prep_stmt = (s62_prepared_statement*)malloc(sizeof(s62_prepared_statement));
	prep_stmt->query = query;
	prep_stmt->__internal_prepared_statement = reinterpret_cast<void*>(new CypherPreparedStatement(string(query)));
	s62_compile_query(string(query));
	s62_extract_query_metadata(prep_stmt);
    return prep_stmt;
}

s62_state s62_close_prepared_statement(s62_prepared_statement* prepared_statement) {
	if (prepared_statement == NULL) {
		last_error_message = INVALID_PREPARED_STATEMENT_MSG;
		last_error_code = S62_ERROR_INVALID_STATEMENT;
		return S62_ERROR;
	}

	auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	if (!cypher_stmt) {
		last_error_message = INVALID_PREPARED_STATEMENT_MSG;
		last_error_code = S62_ERROR_INVALID_STATEMENT;
		return S62_ERROR;
	}
	delete cypher_stmt;

	s62_close_property(prepared_statement->property);
	free(prepared_statement);
	return S62_SUCCESS;
}

s62_state s62_bind_value(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_value val) {
	auto value = reinterpret_cast<Value *>(val);
	auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	if (!cypher_stmt) {
        last_error_message = INVALID_PREPARED_STATEMENT_MSG;
        last_error_code = S62_ERROR_INVALID_STATEMENT;
		return S62_ERROR;
	}
	if (!cypher_stmt->bindValue(param_idx - 1, *value)) {
        last_error_message = "Can not bind to parameter number " + std::to_string(param_idx) + ", statement only has " + std::to_string(cypher_stmt->getNumParams()) + " parameter(s)";
        last_error_code = S62_ERROR_INVALID_PARAMETER_INDEX;
		return S62_ERROR;
	}
	return S62_SUCCESS;
}

s62_state s62_bind_boolean(s62_prepared_statement* prepared_statement, idx_t param_idx, bool val) {
	auto value = Value::BOOLEAN(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_int8(s62_prepared_statement* prepared_statement, idx_t param_idx, int8_t val) {
	auto value = Value::TINYINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_int16(s62_prepared_statement* prepared_statement, idx_t param_idx, int16_t val) {
	auto value = Value::SMALLINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_int32(s62_prepared_statement* prepared_statement, idx_t param_idx, int32_t val) {
	auto value = Value::INTEGER(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_int64(s62_prepared_statement* prepared_statement, idx_t param_idx, int64_t val) {
	auto value = Value::BIGINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

static hugeint_t s62_internal_hugeint(s62_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

s62_state s62_bind_hugeint(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_hugeint val) {
	auto value = Value::HUGEINT(s62_internal_hugeint(val));
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_uint8(s62_prepared_statement* prepared_statement, idx_t param_idx, uint8_t val) {
	auto value = Value::UTINYINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_uint16(s62_prepared_statement* prepared_statement, idx_t param_idx, uint16_t val) {
	auto value = Value::USMALLINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_uint32(s62_prepared_statement* prepared_statement, idx_t param_idx, uint32_t val) {
	auto value = Value::UINTEGER(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_uint64(s62_prepared_statement* prepared_statement, idx_t param_idx, uint64_t val) {
	auto value = Value::UBIGINT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_float(s62_prepared_statement* prepared_statement, idx_t param_idx, float val) {
	auto value = Value::FLOAT(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_double(s62_prepared_statement* prepared_statement, idx_t param_idx, double val) {
	auto value = Value::DOUBLE(val);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_date(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_date val) {
	auto value = Value::DATE(duckdb::date_t(val.days));
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_time(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_time val) {
	auto value = Value::TIME(duckdb::dtime_t(val.micros));
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_timestamp(s62_prepared_statement* prepared_statement, idx_t param_idx,
                                   s62_timestamp val) {
	auto value = Value::TIMESTAMP(duckdb::timestamp_t(val.micros));
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

s62_state s62_bind_varchar(s62_prepared_statement* prepared_statement, idx_t param_idx, const char *val) {
	try {
		auto value = Value(val);
		return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
	} catch (...) {
		return S62_ERROR;
	}
}

s62_state s62_bind_varchar_length(s62_prepared_statement* prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		auto value = Value(std::string(val, length));
		return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
	} catch (...) {
		return S62_ERROR;
	}
}

s62_state s62_bind_decimal(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_decimal val) {
	auto hugeint_val = s62_internal_hugeint(val.value);
	if (val.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		auto value = Value::DECIMAL(hugeint_val, val.width, val.scale);
		return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
	}
	auto value = hugeint_val.lower;
	auto duck_val = Value::DECIMAL((int64_t)value, val.width, val.scale);
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&duck_val);
}

s62_state s62_bind_null(s62_prepared_statement* prepared_statement, idx_t param_idx) {
	auto value = Value();
	return s62_bind_value(prepared_statement, param_idx, (s62_value)&value);
}

static void s62_register_resultset(s62_prepared_statement* prepared_statement, s62_resultset_wrapper** _results_set_wrp) {
	auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);

	// Create linked list of s62_resultset
	s62_resultset *result_set = NULL;
	s62_resultset *prev_result_set = NULL;

	for (auto data_chunk: cypher_prep_stmt->queryResults) {
		s62_resultset *new_result_set = (s62_resultset*)malloc(sizeof(s62_resultset));
		new_result_set->num_properties = prepared_statement->num_properties;
		new_result_set->next = NULL;

		// Create linked list of s62_result and register to result
		{
			s62_result *result = NULL;
			s62_result *prev_result = NULL;
			s62_property *property = prepared_statement->property;

			for (int i = 0; i < prepared_statement->num_properties; i++) {
				s62_result *new_result = (s62_result*)malloc(sizeof(s62_result));
				new_result->data_type = property->property_type;
				new_result->data_sql_type = property->property_sql_type;
				new_result->num_rows = data_chunk->size();
				new_result->__internal_data = (void*)(&data_chunk->data[i]);
				new_result->next = NULL;

				if (!result) {
					result = new_result;
				} else {
					prev_result->next = new_result;
				}
				prev_result = new_result;
				property = property->next;
			}
			new_result_set->result = result;
		}

		if (!result_set) {
			result_set = new_result_set;
		} else {
			prev_result_set->next = new_result_set;
		}
		prev_result_set = new_result_set;
	}

	s62_resultset_wrapper *result_set_wrp = (s62_resultset_wrapper*)malloc(sizeof(s62_resultset_wrapper));
	result_set_wrp->result_set = result_set;
	result_set_wrp->cursor = 0;
	result_set_wrp->num_total_rows = cypher_prep_stmt->getNumRows();
	*_results_set_wrp = result_set_wrp;
}

s62_num_rows s62_execute(s62_prepared_statement* prepared_statement, s62_resultset_wrapper** result_set_wrp) {
	auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	std::cout << cypher_prep_stmt->getBoundQuery() << std::endl;
	s62_compile_query(cypher_prep_stmt->getBoundQuery());
	auto executors = planner->genPipelineExecutors();
    if (executors.size() == 0) { 
		last_error_message = INVALID_PLAN_MSG;
		last_error_code = S62_ERROR_INVALID_PLAN;
		return S62_ERROR;
    }
    else {
        for( auto exec : executors ) { exec->ExecutePipeline(); }
		cypher_prep_stmt->copyResults(*(executors.back()->context->query_results));
		s62_register_resultset(prepared_statement, result_set_wrp);
    	return cypher_prep_stmt->getNumRows();
    }
}

s62_state s62_close_resultset(s62_resultset_wrapper* result_set_wrp) {
	if (result_set_wrp == NULL || result_set_wrp->result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_RESULT_SET;
		return S62_ERROR;
	}

	s62_resultset *next_result_set = NULL;
	s62_resultset *result_set = result_set_wrp->result_set;
	while (result_set != NULL) {
		next_result_set = result_set->next;
		s62_result *next_result = NULL;
		s62_result *result = result_set->result;
		while (result != NULL) {
			auto next_result = result->next;
			free(result);
			result = next_result;
		}
		free(result_set);
		result_set = next_result_set;
	}
	free(result_set_wrp);

	return S62_SUCCESS;
}

s62_fetch_state s62_fetch_next(s62_resultset_wrapper* result_set_wrp) {
	if (result_set_wrp == NULL || result_set_wrp->result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_RESULT_SET;
		return S62_ERROR_RESULT;
	}

	if (result_set_wrp->cursor >= result_set_wrp->num_total_rows) {
		return S62_END_OF_RESULT;
	}
	else {
		result_set_wrp->cursor++;
		if (result_set_wrp->cursor <= result_set_wrp->num_total_rows) {
			return S62_MORE_RESULT;
		} else {
			return S62_END_OF_RESULT;
		}
	}
}

static s62_result* s62_move_to_cursored_result(s62_resultset_wrapper* result_set_wrp, idx_t col_idx, size_t& local_cursor) {
	auto cursor = result_set_wrp->cursor - 1;
	size_t acc_rows = 0;

	s62_resultset *result_set = result_set_wrp->result_set;
	while (result_set != NULL) {
		auto num_rows = result_set->result->num_rows;
		if (cursor < acc_rows + num_rows) {
			break;
		}
		acc_rows += num_rows;
		result_set = result_set->next;
	}
	local_cursor = cursor - acc_rows;

	if (result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_RESULT_SET;
		return NULL;
	}

	auto result = result_set->result;
	for (int i = 0; i < col_idx; i++) {
		result = result->next;
	}

	if (result == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_COLUMN_INDEX;
		return NULL;
	}

	return result;
}

template <typename T, duckdb::LogicalTypeId TYPE_ID>
static T s62_get_value(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_RESULT_SET;
		return T();
	}

    size_t local_cursor;
    auto result = s62_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return T(); }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != TYPE_ID) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = S62_ERROR_INVALID_COLUMN_TYPE;
        return T();
    }
    else {
        return vec->GetValue(local_cursor).GetValue<T>();
    }
}

bool s62_get_bool(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
    return s62_get_value<bool, duckdb::LogicalTypeId::BOOLEAN>(result_set_wrp, col_idx);
}

int8_t s62_get_int8(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
    return s62_get_value<int8_t, duckdb::LogicalTypeId::TINYINT>(result_set_wrp, col_idx);
}

int16_t s62_get_int16(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<int16_t, duckdb::LogicalTypeId::SMALLINT>(result_set_wrp, col_idx);
}

int32_t s62_get_int32(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<int32_t, duckdb::LogicalTypeId::INTEGER>(result_set_wrp, col_idx);
}

int64_t s62_get_int64(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<int64_t, duckdb::LogicalTypeId::BIGINT>(result_set_wrp, col_idx);
}

s62_hugeint s62_get_hugeint(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	hugeint_t hugeint = s62_get_value<hugeint_t, duckdb::LogicalTypeId::HUGEINT>(result_set_wrp, col_idx);
	s62_hugeint result;
	result.lower = hugeint.lower;
	result.upper = hugeint.upper;
	return result;
}

uint8_t s62_get_uint8(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<uint8_t, duckdb::LogicalTypeId::UTINYINT>(result_set_wrp, col_idx);
}

uint16_t s62_get_uint16(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<uint16_t, duckdb::LogicalTypeId::SMALLINT>(result_set_wrp, col_idx);
}

uint32_t s62_get_uint32(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<uint32_t, duckdb::LogicalTypeId::INTEGER>(result_set_wrp, col_idx);
}

uint64_t s62_get_uint64(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<uint64_t, duckdb::LogicalTypeId::UBIGINT>(result_set_wrp, col_idx);
}

float s62_get_float(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<float, duckdb::LogicalTypeId::FLOAT>(result_set_wrp, col_idx);
}

double s62_get_double(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<double, duckdb::LogicalTypeId::DOUBLE>(result_set_wrp, col_idx);
}

s62_date s62_get_date(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::date_t date = s62_get_value<duckdb::date_t, duckdb::LogicalTypeId::DATE>(result_set_wrp, col_idx);
	s62_date result;
	result.days = date.days;
	return result;
}

s62_time s62_get_time(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::dtime_t time = s62_get_value<duckdb::dtime_t, duckdb::LogicalTypeId::TIME>(result_set_wrp, col_idx);
	s62_time result;
	result.micros = time.micros;
	return result;
}

s62_timestamp s62_get_timestamp(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::timestamp_t timestamp = s62_get_value<duckdb::timestamp_t, duckdb::LogicalTypeId::TIMESTAMP>(result_set_wrp, col_idx);
	s62_timestamp result;
	result.micros = timestamp.value;
	return result;
}

s62_string s62_get_varchar(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	string str = s62_get_value<string, duckdb::LogicalTypeId::VARCHAR>(result_set_wrp, col_idx);
	s62_string result;
	result.size = str.length();
	result.data = (char*)malloc(result.size + 1);
    strcpy(result.data, str.c_str());
	return result;
}

s62_decimal s62_get_decimal(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	// Decimal needs special handling
	auto default_value = s62_decimal{0,0,{0,0}};
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = S62_ERROR_INVALID_RESULT_SET;
		return default_value;
	}

    size_t local_cursor;
    auto result = s62_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return default_value; }
	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);
	auto data_type = vec->GetType();
	auto width = duckdb::DecimalType::GetWidth(data_type);
	auto scale = duckdb::DecimalType::GetScale(data_type);
	switch (data_type.InternalType()) {
		case duckdb::PhysicalType::INT16:
			return s62_decimal{width,scale,{s62_get_int16(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT32:
			return s62_decimal{width,scale,{s62_get_int32(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT64:
			return s62_decimal{width,scale,{s62_get_int64(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT128:
		{
			auto int128_val = s62_get_hugeint(result_set_wrp, col_idx);
			return s62_decimal{width,scale,{int128_val.lower,int128_val.upper}};
			break;
		}
		default:
		{
			return default_value;
		}
	}
}

uint64_t s62_get_id(s62_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return s62_get_value<uint64_t, duckdb::LogicalTypeId::ID>(result_set_wrp, col_idx);
}