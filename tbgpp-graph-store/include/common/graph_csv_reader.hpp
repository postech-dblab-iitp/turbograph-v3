#pragma once

#include "common/common.hpp"
#include "third_party/csv-parser/csv.hpp"
#include "common/enums/graph_component_type.hpp"
#include "common/assert.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {
    
class GraphCSVFileReader {

public:
    GraphCSVFileReader() {}
    ~GraphCSVFileReader() {}

    void InitCSVFile(const char *csv_file_path, GraphComponentType type_) {
        type = type_;

        // Initialize CSV Reader & iterator
        reader = new csv::CSVReader(csv_file_path);
        csv_it = reader->begin();

        vector<string> col_names = move(reader->get_col_names());
        for (size_t i = 0; i < col_names; i++)
            fprintf(stdout, "%s, ", col_names[i]);
        fprintf(stdout, "\n");
    }

	bool GetSchemaFromHeader(vector<string> &key_names, vector<LogicalType> &types) {
		// TODO
		return true;
	}

	int64_t GetKeyColumnIndexFromHeader() {
		// TODO
		return -1;
	}

	void GetSrcColumnIndexFromHeader(int64_t &src_column_idx, string &src_column_name) {
		// TODO
		src_column_idx = -1;
		return;
	}

	void GetDstColumnIndexFromHeader(int64_t &dst_column_idx, string &dst_column_name) {
		// TODO
		dst_column_idx = -1;
		return;
	}

	bool ReadCSVFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		D_ASSERT(key_names.size() == types.size());
		D_ASSERT(key_names.size() == output.ColumnCount());

		if (type == GraphComponentType::VERTEX) {
			return ReadVertexCSVFile(key_names, types, output);
		} else if (type == GraphComponentType::EDGE) {
			return ReadEdgeCSVFile(key_names, types, output);
		}
		return true;
	}

	bool ReadVertexCSVFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		//idx_t current_index = 0;
		
		//output.SetCardinality(current_index);
		
	}

	// Same Logic as ReadVertexJsonFile
	bool ReadEdgeCSVFile(vector<string> &key_names, vector<LogicalType> &types, DataChunk &output) {
		//idx_t current_index = 0;
		
		//output.SetCardinality(current_index);
	}

private:
    GraphComponentType type;
    csv::CSVReader *reader;
    csv::CSVReader::iterator csv_it;
    csv::CSVFormat csv_format;
};

} // namespace duckdb
