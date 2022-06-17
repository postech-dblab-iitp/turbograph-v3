#pragma once

#include "common/common.hpp"
#include "third_party/yyjson/yyjson.h"
#include "common/enums/graph_component_type.hpp"
#include "common/assert.hpp"
#include "common/types/data_chunk.hpp"
//#include "common/types/value.hpp"

namespace duckdb {

inline Value JsonValToValue(yyjson_val *val, LogicalType &type) {
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			return Value::BOOLEAN(yyjson_get_bool(val));
		case LogicalTypeId::TINYINT:
			return Value::TINYINT((int8_t)yyjson_get_int(val));
		case LogicalTypeId::SMALLINT:
			return Value::SMALLINT((int16_t)yyjson_get_int(val));
		case LogicalTypeId::INTEGER:
			return Value::INTEGER(yyjson_get_int(val));
		case LogicalTypeId::BIGINT:
			return Value::BIGINT(yyjson_get_sint(val));
		case LogicalTypeId::UTINYINT:
			return Value::UTINYINT((uint8_t)yyjson_get_uint(val));
		case LogicalTypeId::USMALLINT:
			return Value::USMALLINT((uint16_t)yyjson_get_uint(val));
		case LogicalTypeId::UINTEGER:
			return Value::UINTEGER((uint32_t)yyjson_get_uint(val));
		case LogicalTypeId::UBIGINT:
			return Value::UBIGINT(yyjson_get_uint(val));
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt");
		case LogicalTypeId::DECIMAL:
			throw NotImplementedException("Do not support Decimal");
		case LogicalTypeId::FLOAT:
			return Value::FLOAT((float)yyjson_get_real(val));
		case LogicalTypeId::DOUBLE:
			return Value::DOUBLE(yyjson_get_real(val));
		case LogicalTypeId::VARCHAR:
			return Value(yyjson_get_str(val));
		default:
			throw NotImplementedException("Unsupported type");
	}
}

class GraphJsonFileReader {
private:
	static constexpr yyjson_read_flag JSON_READ_FLAG = YYJSON_READ_ALLOW_INF_AND_NAN | YYJSON_READ_ALLOW_TRAILING_COMMAS; //YYJSON_READ_ALLOW_COMMENTS

public:
	GraphJsonFileReader() {}
	~GraphJsonFileReader() {
		if (doc) {
			yyjson_doc_free(doc);
			doc = nullptr;
		}
	}

	void InitJsonFile(const char *json_file_path, GraphComponentType type_) {
		yyjson_read_err err;
		doc = yyjson_read_file(json_file_path, JSON_READ_FLAG, NULL, &err);

		if (!doc) {
			fprintf(stdout, "[JsonFileReader] Read error (%u): %s at position: %ld\n", err.code, err.msg, err.pos);
		}
		
		type = type_;

		if (type == GraphComponentType::VERTEX) {
			yyjson_val *root = yyjson_doc_get_root(doc);
			yyjson_obj_iter iter;
			yyjson_obj_iter_init(root, &iter);
			yyjson_val *key, *arr;

			key = yyjson_obj_iter_next(&iter);
			D_ASSERT(std::strncmp(yyjson_get_str(key), "vertices", 8) == 0);

			arr = yyjson_obj_iter_get_val(key);
			D_ASSERT(yyjson_is_arr(arr));

			yyjson_arr_iter_init(arr, &arr_iter);
		} else {
			throw NotImplementedException("Not implemented yet");
		}
	}

	bool ReadJsonFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		D_ASSERT(key_names.size() == types.size());
		D_ASSERT(key_names.size() == output.ColumnCount());

		if (type == GraphComponentType::VERTEX) {
			return ReadVertexJsonFile(key_names, types, output);
		} else if (type == GraphComponentType::EDGE) {
			return ReadEdgeJsonFile(key_names, types, output);
		}
		return true;
	}

	bool ReadVertexJsonFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		// Assume the input json file follows GraphSon Format
		idx_t current_index = 0;
		yyjson_val *val;

		while ((val = yyjson_arr_iter_next(&arr_iter))) {
			// val = a vertex
			yyjson_val *vertex_attr_key, *vertex_attr_val;
			yyjson_obj_iter vertex_attr_iter;
			yyjson_obj_iter_init(val, &vertex_attr_iter);
			for (int i = 0; i < key_names.size(); i++) {
				yyjson_val *attr = yyjson_obj_iter_get(&vertex_attr_iter, key_names[i].c_str());
				output.SetValue(i, current_index, JsonValToValue(attr, types[i]));
			}
			
			if (++current_index == STANDARD_VECTOR_SIZE) break;
			/*while ((vertex_attr_key = yyjson_obj_iter_next(&vertex_attr_iter))) {
				vertex_attr_val = yyjson_obj_iter_get_val(vertex_attr_key);
				printf("%s: %s\n", yyjson_get_str(vertex_attr_key), yyjson_get_type_desc(vertex_attr_val));
			}*/
		}
		output.SetCardinality(current_index);
		if (arr_iter.max == arr_iter.idx) return true;
		else return false;
	}

	bool ReadEdgeJsonFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		return true;
	}

public:


private:
	yyjson_doc *doc;
	yyjson_arr_iter arr_iter;
	GraphComponentType type;
};

} // namespace duckdb
