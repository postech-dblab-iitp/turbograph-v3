#include "api/c-api/capi_internal.hpp"

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
	case LogicalTypeId::ID:
		return S62_TYPE_ID;
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
	case S62_TYPE_ID:
		return sizeof(uint64_t);
	default: // LCOV_EXCL_START
		// unsupported type
		D_ASSERT(0);
		return sizeof(const char *);
	} // LCOV_EXCL_STOP
}

std::string pipelineToPostgresPlan(
    duckdb::CypherPipeline* pipeline,
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*>& parent_map,
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*>& rhs_map,
    int indent,
	bool is_executed);

// Helper to check if a pipeline's sink is a binary operator
bool isBinaryOperatorSink(duckdb::CypherPipeline* pipeline) {
    std::string sink_type = pipeline->GetSink()->ToString();
    return sink_type == "HashJoin";
}

// Helper to find a matching binary operator in the middle of lhs pipeline
bool hasMatchingOperator(duckdb::CypherPipeline* lhs_pipeline, duckdb::CypherPipeline* rhs_pipeline) {
    std::string rhs_op_type = rhs_pipeline->GetSink()->ToString();
    for (auto& op : lhs_pipeline->GetOperators()) {
        if (op->ToString() == rhs_op_type) {
            return true;
        }
    }
    return false;
}

// Main function to generate PostgreSQL-style query plan
std::string generatePostgresStylePlan(std::vector<CypherPipelineExecutor*>& executors, bool is_executed) {
    std::ostringstream oss;

    // Step 1: Map parent-child relationships and rhs mappings for binary operators
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*> parent_map;
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*> rhs_map;


    for (size_t i = 0; i < executors.size(); ++i) {
        auto* pipeline = executors[i]->pipeline;
        
        // Identify if the pipeline contains a binary operator as sink
        for (size_t j = i + 1; j < executors.size(); ++j) {
            auto* next_pipeline = executors[j]->pipeline;

            // For binary operators, look for middle of lhs and sink in rhs pipeline
            if (isBinaryOperatorSink(next_pipeline) && hasMatchingOperator(pipeline, next_pipeline)) {
                rhs_map[pipeline] = next_pipeline;  // Map left (lhs) to right (rhs) pipeline
                break;
            }

            // Unary operators connected through source-sink
            if (next_pipeline->GetSource() == pipeline->GetSink()) {
                parent_map[next_pipeline] = pipeline;
            }
        }
    }

    // Step 2: Start from the root pipeline and recursively output the plan tree
    auto root_pipeline = executors.back()->pipeline;
    oss << pipelineToPostgresPlan(root_pipeline, parent_map, rhs_map, 0, is_executed);

    return oss.str();
}

void printOperator(std::ostringstream& oss, duckdb::CypherPhysicalOperator* op, std::string& indent_str, bool is_root, bool is_executed) {
	std::string op_type = op->ToString();
	std::string params = op->ParamsToString();

	if (is_root) {
		if (is_executed) {
			oss << indent_str << op_type << "  (rows: " <<  op->processed_tuples << ", time: " << op->op_timer.elapsed().wall / 1000000.0 << ")" << "\n";
		}
		else {
			oss << indent_str << op_type << "\n";
		}
	} else {
		if (is_executed) {
			oss << indent_str << "-> " << op_type << "  (rows: " <<  op->processed_tuples << ", time: " << op->op_timer.elapsed().wall / 1000000.0 << ")" << "  (" << params << ")\n";
		}
		else {
			oss << indent_str << "-> " << op_type << "  (" << params << ")\n";
		}
	}
}

// Recursive helper to output pipeline and its operators
std::string pipelineToPostgresPlan(
    duckdb::CypherPipeline* pipeline,
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*>& parent_map,
    std::unordered_map<duckdb::CypherPipeline*, duckdb::CypherPipeline*>& rhs_map,
    int indent,
	bool is_executed) {
    
    std::ostringstream oss;

    // Traverse operators in this pipeline from top to bottom
	std::vector<CypherPhysicalOperator *> operators = pipeline->GetOperators();
	if (indent == 0) operators.push_back(pipeline->GetSink());
	operators.insert(operators.begin(), pipeline->GetSource());

	std::vector<int> idxscan_idents;

	// reverse
	for (auto it = operators.rbegin(); it != operators.rend(); ++it) {
		auto &op = *it;
        std::string op_type = op->ToString();
		std::string params = op->ParamsToString();
    	std::string indent_str(indent, ' ');
        
        // Check if the current operator is a binary operator
        if (op_type == "AdjIdxJoin" || op_type == "IdSeek" || op_type == "HashJoin") {
			printOperator(oss, op, indent_str, false, is_executed);
            // Handle right child pipeline recursively if found
            if (rhs_map.find(pipeline) != rhs_map.end()) {
                oss << pipelineToPostgresPlan(rhs_map[pipeline], parent_map, rhs_map, indent + 4, is_executed);
            }
			else {
				idxscan_idents.push_back(indent + 4);
			}
        } else {
            // Unary operators, simply add them to the output
			indent == 0 ? printOperator(oss, op, indent_str, true, is_executed) : printOperator(oss, op, indent_str, false, is_executed);
            if (parent_map.find(pipeline) != parent_map.end() && op == pipeline->GetSource()) {
                oss << pipelineToPostgresPlan(parent_map[pipeline], parent_map, rhs_map, indent + 4, is_executed);
            }
        }

		indent += 4;
    }

	for (auto ident : idxscan_idents) {
		oss << std::string(ident, ' ') << "-> Index Scan\n";
	}

    return oss.str();
}

} // namespace duckdb