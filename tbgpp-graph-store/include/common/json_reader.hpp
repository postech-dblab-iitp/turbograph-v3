#pragma once

#include "common/common.hpp"
#include "third_party/yyjson/yyjson.h"
#include "common/enums/graph_component_type.hpp"
#include "common/assert.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

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
	}

	void ReadJsonFile(DataChunk &output) {
		if (type == GraphComponentType::VERTEX) {
			ReadVertexJsonFile(output);
		} else if (type == GraphComponentType::EDGE) {
			ReadEdgeJsonFile(output);
		}
	}

	void ReadVertexJsonFile(DataChunk &output) {
		// Assume the input json file follows GraphSon Format

		yyjson_val *obj = yyjson_doc_get_root(doc);
    	yyjson_obj_iter iter;
    	yyjson_obj_iter_init(obj, &iter);
    	yyjson_val *key, *arr;

		key = yyjson_obj_iter_next(&iter);
		D_ASSERT(yyjson_get_str(key) == "vertices");

		arr = yyjson_obj_iter_get_val(key);
		D_ASSERT(yyjson_is_arr(arr));

		yyjson_val *val;
		yyjson_arr_iter arr_iter;
		yyjson_arr_iter_init(arr, &arr_iter);
		while ((val = yyjson_arr_iter_next(&arr_iter))) {
			// val = a vertex
			yyjson_val *vertex_attr_key, *vertex_attr_val;
			yyjson_obj_iter vertex_attr_iter;
			yyjson_obj_iter_init(val, &vertex_attr_iter);
			while ((vertex_attr_key = yyjson_obj_iter_next(&vertex_attr_iter))) {
				vertex_attr_val = yyjson_obj_iter_get_val(vertex_attr_key);
				printf("%s: %s\n", yyjson_get_str(vertex_attr_key), yyjson_get_type_desc(vertex_attr_val));
			}
		}
	}

	void ReadEdgeJsonFile(DataChunk &output) {

	}

private:
	yyjson_doc *doc;
	yyjson_val *root;
	GraphComponentType type;
};

} // namespace duckdb
