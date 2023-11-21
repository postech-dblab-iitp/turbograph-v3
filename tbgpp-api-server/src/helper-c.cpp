#include "capi_internal.hpp"

namespace duckdb {

LogicalTypeId ConvertCTypeToCPP(s62_type c_type) {
	switch (c_type) {
	case S62_TYPE_BOOLEAN:
		return LogicalTypeId::BOOLEAN;
	case S62_TYPE_TINYINT:
		return LogicalTypeId::TINYINT;
	case S62_TYPE_SMALLINT:
		return LogicalTypeId::SMALLINT;
	case S62_TYPE_INTEGER:
		return LogicalTypeId::INTEGER;
	case S62_TYPE_BIGINT:
		return LogicalTypeId::BIGINT;
	case S62_TYPE_UTINYINT:
		return LogicalTypeId::UTINYINT;
	case S62_TYPE_USMALLINT:
		return LogicalTypeId::USMALLINT;
	case S62_TYPE_UINTEGER:
		return LogicalTypeId::UINTEGER;
	case S62_TYPE_UBIGINT:
		return LogicalTypeId::UBIGINT;
	case S62_TYPE_HUGEINT:
		return LogicalTypeId::HUGEINT;
	case S62_TYPE_FLOAT:
		return LogicalTypeId::FLOAT;
	case S62_TYPE_DOUBLE:
		return LogicalTypeId::DOUBLE;
	case S62_TYPE_TIMESTAMP:
		return LogicalTypeId::TIMESTAMP;
	case S62_TYPE_DATE:
		return LogicalTypeId::DATE;
	case S62_TYPE_TIME:
		return LogicalTypeId::TIME;
	case S62_TYPE_VARCHAR:
		return LogicalTypeId::VARCHAR;
	case S62_TYPE_BLOB:
		return LogicalTypeId::BLOB;
	case S62_TYPE_INTERVAL:
		return LogicalTypeId::INTERVAL;
	case S62_TYPE_TIMESTAMP_S:
		return LogicalTypeId::TIMESTAMP_SEC;
	case S62_TYPE_TIMESTAMP_MS:
		return LogicalTypeId::TIMESTAMP_MS;
	case S62_TYPE_TIMESTAMP_NS:
		return LogicalTypeId::TIMESTAMP_NS;
	case S62_TYPE_UUID:
		return LogicalTypeId::UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return LogicalTypeId::INVALID;
	} // LCOV_EXCL_STOP
}

s62_type ConvertCPPTypeToC(const LogicalType &sql_type) {
	switch (sql_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return S62_TYPE_BOOLEAN;
	case LogicalTypeId::TINYINT:
		return S62_TYPE_TINYINT;
	case LogicalTypeId::SMALLINT:
		return S62_TYPE_SMALLINT;
	case LogicalTypeId::INTEGER:
		return S62_TYPE_INTEGER;
	case LogicalTypeId::BIGINT:
		return S62_TYPE_BIGINT;
	case LogicalTypeId::UTINYINT:
		return S62_TYPE_UTINYINT;
	case LogicalTypeId::USMALLINT:
		return S62_TYPE_USMALLINT;
	case LogicalTypeId::UINTEGER:
		return S62_TYPE_UINTEGER;
	case LogicalTypeId::UBIGINT:
		return S62_TYPE_UBIGINT;
	case LogicalTypeId::HUGEINT:
		return S62_TYPE_HUGEINT;
	case LogicalTypeId::FLOAT:
		return S62_TYPE_FLOAT;
	case LogicalTypeId::DOUBLE:
		return S62_TYPE_DOUBLE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return S62_TYPE_TIMESTAMP;
	case LogicalTypeId::TIMESTAMP_SEC:
		return S62_TYPE_TIMESTAMP_S;
	case LogicalTypeId::TIMESTAMP_MS:
		return S62_TYPE_TIMESTAMP_MS;
	case LogicalTypeId::TIMESTAMP_NS:
		return S62_TYPE_TIMESTAMP_NS;
	case LogicalTypeId::DATE:
		return S62_TYPE_DATE;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return S62_TYPE_TIME;
	case LogicalTypeId::VARCHAR:
		return S62_TYPE_VARCHAR;
	case LogicalTypeId::BLOB:
		return S62_TYPE_BLOB;
	case LogicalTypeId::INTERVAL:
		return S62_TYPE_INTERVAL;
	case LogicalTypeId::DECIMAL:
		return S62_TYPE_DECIMAL;
	case LogicalTypeId::ENUM:
		return S62_TYPE_ENUM;
	case LogicalTypeId::LIST:
		return S62_TYPE_LIST;
	case LogicalTypeId::STRUCT:
		return S62_TYPE_STRUCT;
	case LogicalTypeId::MAP:
		return S62_TYPE_MAP;
	case LogicalTypeId::UUID:
		return S62_TYPE_UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return S62_TYPE_INVALID;
	} // LCOV_EXCL_STOP
}

idx_t GetCTypeSize(s62_type type) {
	switch (type) {
	case S62_TYPE_BOOLEAN:
		return sizeof(bool);
	case S62_TYPE_TINYINT:
		return sizeof(int8_t);
	case S62_TYPE_SMALLINT:
		return sizeof(int16_t);
	case S62_TYPE_INTEGER:
		return sizeof(int32_t);
	case S62_TYPE_BIGINT:
		return sizeof(int64_t);
	case S62_TYPE_UTINYINT:
		return sizeof(uint8_t);
	case S62_TYPE_USMALLINT:
		return sizeof(uint16_t);
	case S62_TYPE_UINTEGER:
		return sizeof(uint32_t);
	case S62_TYPE_UBIGINT:
		return sizeof(uint64_t);
	case S62_TYPE_HUGEINT:
	case S62_TYPE_UUID:
		return sizeof(s62_hugeint);
	case S62_TYPE_FLOAT:
		return sizeof(float);
	case S62_TYPE_DOUBLE:
		return sizeof(double);
	case S62_TYPE_DATE:
		return sizeof(s62_date);
	case S62_TYPE_TIME:
		return sizeof(s62_time);
	case S62_TYPE_TIMESTAMP:
	case S62_TYPE_TIMESTAMP_S:
	case S62_TYPE_TIMESTAMP_MS:
	case S62_TYPE_TIMESTAMP_NS:
		return sizeof(s62_timestamp);
	case S62_TYPE_VARCHAR:
		return sizeof(const char *);
	case S62_TYPE_INTERVAL:
		return sizeof(s62_interval);
	case S62_TYPE_DECIMAL:
		return sizeof(s62_hugeint);
	default: // LCOV_EXCL_START
		// unsupported type
		D_ASSERT(0);
		return sizeof(const char *);
	} // LCOV_EXCL_STOP
}

std::string jsonifyQueryPlan(std::vector<CypherPipelineExecutor*>& executors) {
	json j = json::array( { json({}), } );
	
	// reverse-iterate executors
	json* current_root = &(j[0]);
	bool isRootOp = true;	// is true for only one operator
	
	for (auto it = executors.crbegin() ; it != executors.crend(); ++it) {
  		duckdb::CypherPipeline* pipeline = (*it)->pipeline;
		// reverse operator
		for (auto it2 = pipeline->operators.crbegin() ; it2 != pipeline->operators.crend(); ++it2) {
			current_root = operatorToVisualizerJSON( current_root, *it2, isRootOp );
			if( isRootOp ) { isRootOp = false; }
		}
		// source
		current_root = operatorToVisualizerJSON( current_root, pipeline->source, isRootOp );
		if( isRootOp ) { isRootOp = false; }
	}
	
	return j.dump(4);
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root) {
	json* content;
	if( is_root ) {
		(*j)["Plan"] = json({});
		content = &((*j)["Plan"]);
	} else {
		if( (*j)["Plans"].is_null() ) {
			// single child
			(*j)["Plans"] = json::array( { json({}), } );
		} else {
			// already made child with two childs. so pass
		}
		content = &((*j)["Plans"][0]);
	}
	(*content)["Node Type"] = op->ToString();
	// output shcma
	
	// add child when operator is 
	if( op->ToString().compare("AdjIdxJoin") == 0 ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "AdjIdxJoinBuild";
	} else if( op->ToString().compare("NodeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "NodeIdSeekBuild";
	} else if( op->ToString().compare("EdgeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "EdgeIdSeekBuild";
	}

	return content;
}

} // namespace duckdb