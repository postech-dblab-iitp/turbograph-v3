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

// Error Handling
static s62_error_code last_error_code = S62_NO_ERROR;
static std::string last_error_message;

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
    database.reset();
    client.reset();
    std::cout << "Database Disconnected" << std::endl;
}

s62_conn_state s62_is_connected() {
    if (database != nullptr) {
        return S62_CONNECTED;
    } else {
        return S62_NOT_CONNECTED;
    }
}

s62_error_code s62_get_last_error(char *errmsg) {
    errmsg = (char*)last_error_message.c_str();
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
        last_error_message = "Unsupported operation";
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
	s62_metadata *metadata = nullptr;
	s62_metadata *prev = nullptr;
	for(int i = 0; i < labels.size(); i++) {
		s62_metadata *new_metadata = (s62_metadata*)malloc(sizeof(s62_metadata));
		new_metadata->label_name = strdup(labels[i].c_str());
		new_metadata->type = S62_METADATA_TYPE::S62_NODE;
		new_metadata->next = nullptr;
		if(prev == nullptr) {
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
		new_metadata->next = nullptr;
		if(prev == nullptr) {
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
	if (metadata == nullptr) {
		last_error_message = "Invalid metadata";
		last_error_code = S62_ERROR_INVALID_METADATA;
		return S62_ERROR;
	}

	s62_metadata *next;
	while(metadata != nullptr) {
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
		last_error_message = "Invalid label";
		last_error_code = S62_ERROR_INVALID_LABEL;
		return S62_ERROR;
	}

	auto graph_cat_entry = s62_get_graph_catalog_entry();
	vector<idx_t> partition_indexes;
	s62_property *property = nullptr;
	s62_property *prev = nullptr;
	PartitionCatalogEntry *partition_cat_entry = nullptr;

	if (type == S62_METADATA_TYPE::S62_NODE) {
		partition_cat_entry = s62_get_vertex_partition_catalog_entry(string(label));
	} else if (type == S62_METADATA_TYPE::S62_EDGE) {
		partition_cat_entry = s62_get_edge_partition_catalog_entry(string(label));
	} else {
		last_error_message = "Invalid metadata type";
		last_error_code = S62_ERROR_INVALID_METADATA_TYPE;
		return S62_ERROR;
	}

	auto num_properties = partition_cat_entry->global_property_key_names.size();
	auto num_types = partition_cat_entry->global_property_typesid.size();

	if (num_properties != num_types) {
		last_error_message = "Invalid number of properties";
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

		new_property->next = nullptr;
		if (prev == nullptr) {
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
	if (property == nullptr) {
		last_error_message = "Invalid property";
		last_error_code = S62_ERROR_INVALID_PROPERTY;
		return S62_ERROR;
	}

	s62_property *next;
	while(property != nullptr) {
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
	if (((CColRefTable*) col_ref)->GetMdidTable() != nullptr) {
		OID table_obj_id = CMDIdGPDB::CastMdid(((CColRefTable*) col_ref)->GetMdidTable())->Oid();
		PropertySchemaCatalogEntry *ps_cat_entry = (PropertySchemaCatalogEntry *)client->db->GetCatalog().GetEntry(*client.get(), DEFAULT_SCHEMA, table_obj_id);
		D_ASSERT(ps_cat_entry != nullptr);
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

	new_property->label_name = nullptr;
	new_property->label_type = S62_METADATA_TYPE::S62_OTHER;
	return;
}

static void s62_extract_query_metadata(s62_prepared_statement* prepared_statement) {
    auto executors = planner->genPipelineExecutors();
	 if (executors.size() == 0) {
		last_error_message = "Invalid plan";
		last_error_code = S62_ERROR_INVALID_PLAN;
		return;
    }
    else {
		auto col_names = planner->getQueryOutputColNames();
		auto col_types = executors.back()->pipeline->GetSink()->schema.getStoredTypes();
		auto logical_schema = planner->getQueryOutputSchema();

		s62_property *property = nullptr;
		s62_property *prev = nullptr;

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

			new_property->next = nullptr;
			if (prev == nullptr) {
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
	prep_stmt->__internal_prepared_statement = new CypherPreparedStatement(string(query));
	s62_compile_query(string(query));
	s62_extract_query_metadata(prep_stmt);
    return prep_stmt;
}

s62_state s62_close_prepared_statement(s62_prepared_statement* prepared_statement) {
	if (prepared_statement == nullptr) {
		last_error_message = "Invalid prepared statement";
		last_error_code = S62_ERROR_INVALID_STATEMENT;
		return S62_ERROR;
	}

	auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	if (!cypher_stmt) {
		last_error_message = "Invalid prepared statement";
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
        last_error_message = "Invalid prepared statement";
        last_error_code = S62_ERROR_INVALID_STATEMENT;
		return S62_ERROR;
	}
	if (param_idx <= 0 || param_idx > cypher_stmt->getNumParams()) {
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

s62_state s62_bind_interval(s62_prepared_statement* prepared_statement, idx_t param_idx, s62_interval val) {
	auto value = Value::INTERVAL(val.months, val.days, val.micros);
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


s62_state s62_execute(s62_prepared_statement* prepared_statement) {
    return S62_SUCCESS;
}

int s62_fetch(s62_prepared_statement* prep_query, int* value) {
    static int i = 0;
    if (i < 100 ) {
        *value = i;
        i++;
        return 1;
    } else {
        return 0;
    }
}