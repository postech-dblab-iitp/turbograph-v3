#pragma once

#include <sys/stat.h>
#include "common/common.hpp"
#include "common/assert.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"
#include "common/enums/graph_component_type.hpp"
#include "csv.hpp"

namespace duckdb {

inline void SetValueFromCSV(LogicalType type, DataChunk &output, size_t i, idx_t current_index, csv::CSVField &val) {
	auto data_ptr = output.data[i].GetData();
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			((bool *)data_ptr)[current_index] = val.get<bool>(); break;
		case LogicalTypeId::TINYINT:
			((int8_t *)data_ptr)[current_index] = val.get<int8_t>(); break;
		case LogicalTypeId::SMALLINT:
			((int16_t *)data_ptr)[current_index] = val.get<int16_t>(); break;
		case LogicalTypeId::INTEGER:
			((int32_t *)data_ptr)[current_index] = val.get<int32_t>(); break;
		case LogicalTypeId::BIGINT:
			((int64_t *)data_ptr)[current_index] = val.get<int64_t>(); break;
		case LogicalTypeId::UTINYINT:
			((uint8_t *)data_ptr)[current_index] = val.get<uint8_t>(); break;
		case LogicalTypeId::USMALLINT:
			((uint16_t *)data_ptr)[current_index] = val.get<uint16_t>(); break;
		case LogicalTypeId::UINTEGER:
			((uint32_t *)data_ptr)[current_index] = val.get<uint32_t>(); break;
		case LogicalTypeId::UBIGINT:
			((uint64_t *)data_ptr)[current_index] = val.get<uint64_t>(); break;
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt"); break;
		case LogicalTypeId::DECIMAL:
			throw NotImplementedException("Do not support Decimal"); break;
		case LogicalTypeId::FLOAT:
			((float *)data_ptr)[current_index] = val.get<float>(); break;
		case LogicalTypeId::DOUBLE:
			((double *)data_ptr)[current_index] = val.get<double>(); break;
		case LogicalTypeId::VARCHAR:
			((string_t *)data_ptr)[current_index] = StringVector::AddStringOrBlob(output.data[i], val.get<>()); break;
		default:
			throw NotImplementedException("Unsupported type");
	}
}

// template <> inline void SetValueFromCSV<LogicalTypeId::BOOLEAN>(DataChunk &output, size_t i, idx_t current_index, csv::CSVField &val) {
// 	auto data_ptr = output.data[i].GetData();
// 	((bool *)data_ptr)[current_index] = val.get<bool>();
// }

// template <> inline void SetValueFromCSV<LogicalTypeId::TINYINT>(DataChunk &output, size_t i, idx_t current_index, csv::CSVField &val) {
// 	auto data_ptr = output.data[i].GetData();
// 	((bool *)data_ptr)[current_index] = val.get<bool>();
// }

inline Value CSVValToValue(csv::CSVField &val, LogicalType &type) {
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			return Value::BOOLEAN(val.get<bool>());
		case LogicalTypeId::TINYINT:
			return Value::TINYINT(val.get<int8_t>());
		case LogicalTypeId::SMALLINT:
			return Value::SMALLINT(val.get<int16_t>());
		case LogicalTypeId::INTEGER:
			return Value::INTEGER(val.get<int32_t>());
		case LogicalTypeId::BIGINT:
			return Value::BIGINT(val.get<int64_t>());
		case LogicalTypeId::UTINYINT:
			return Value::UTINYINT(val.get<uint8_t>());
		case LogicalTypeId::USMALLINT:
			return Value::USMALLINT(val.get<uint16_t>());
		case LogicalTypeId::UINTEGER:
			return Value::UINTEGER(val.get<uint32_t>());
		case LogicalTypeId::UBIGINT:
			return Value::UBIGINT(val.get<uint64_t>());
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt");
		case LogicalTypeId::DECIMAL:
			throw NotImplementedException("Do not support Decimal");
		case LogicalTypeId::FLOAT:
			return Value::FLOAT(val.get<float>());
		case LogicalTypeId::DOUBLE:
			return Value::DOUBLE(val.get<double>());
		case LogicalTypeId::VARCHAR:
			return Value(val.get<>());
		default:
			throw NotImplementedException("Unsupported type");
	}
}

class GraphCSVFileReader {

public:
    GraphCSVFileReader() {}
    ~GraphCSVFileReader() {}

    size_t InitCSVFile(const char *csv_file_path, GraphComponentType type_, char delim) {
        type = type_;

        // Initialize CSV Reader & iterator
        csv::CSVFormat csv_form;
        csv_form.delimiter('|')
                .header_row(0);
        reader = new csv::CSVReader(csv_file_path, csv_form);
        csv_it = reader->begin();

		// Parse header
		size_t approximated_size_of_a_row = 0;
        vector<string> col_names = move(reader->get_col_names());
        for (size_t i = 0; i < col_names.size(); i++) {
            // Assume each element in the header column is of format 'key:type'
            std::string key_and_type = col_names[i]; 
            size_t delim_pos = key_and_type.find(':');
            if (delim_pos == std::string::npos) throw InvalidInputException("");
            std::string key = key_and_type.substr(0, delim_pos);
			if (key == "") {
				// special case
				std::string type_name = key_and_type.substr(delim_pos + 1);
				LogicalType type = StringToLogicalType(type_name, i);
				if (dst_column == -1) key_names.push_back(src_key_name + "_src");
				else key_names.push_back(dst_key_name + "_dst");
				key_types.push_back(move(type));
				approximated_size_of_a_row += GetTypeIdSize(type.InternalType());
			} else {
				std::string type_name = key_and_type.substr(delim_pos + 1);
				LogicalType type = StringToLogicalType(type_name, i);
				key_names.push_back(move(key));
				key_types.push_back(move(type));
				approximated_size_of_a_row += GetTypeIdSize(type.InternalType());
			}
        }

		// Estimate number of rows
		struct stat stat_buf;
		int rc = stat(csv_file_path, &stat_buf);
		D_ASSERT(rc != 0);
		size_t file_size = rc == 0 ? stat_buf.st_size : -1;
		fprintf(stdout, "file_size = %ld, approximated_size = %ld, approximated_num_rows = %ld\n", file_size, approximated_size_of_a_row,
						file_size / approximated_size_of_a_row);
		return file_size / approximated_size_of_a_row;
    }

	bool GetSchemaFromHeader(vector<string> &names, vector<LogicalType> &types) {
		D_ASSERT(names.empty() && types.empty());
		names.resize(key_names.size());
		types.resize(key_types.size());
		std::copy(key_names.begin(), key_names.end(), names.begin());
		std::copy(key_types.begin(), key_types.end(), types.begin());
		return true;
	}

	int64_t GetKeyColumnIndexFromHeader() {
		D_ASSERT(type == GraphComponentType::VERTEX);
		return key_column;
	}

	void GetSrcColumnInfo(int64_t &src_column_idx, string &src_column_name) {
		D_ASSERT(type == GraphComponentType::EDGE);
		src_column_idx = src_column;
		src_column_name = src_key_name;
		return;
	}

	void GetDstColumnInfo(int64_t &dst_column_idx, string &dst_column_name) {
		D_ASSERT(type == GraphComponentType::EDGE);
		dst_column_idx = dst_column;
		dst_column_name = dst_key_name;
		return;
	}

	bool ReadCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
		D_ASSERT(required_keys.size() == types.size());
		D_ASSERT(required_keys.size() == output.ColumnCount());

		if (type == GraphComponentType::VERTEX) {
			return ReadVertexCSVFile(required_keys, types, output);
		} else if (type == GraphComponentType::EDGE) {
			return ReadEdgeCSVFile(required_keys, types, output);
		}
		return true;
	}

	bool ReadVertexCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
		auto iter_end = reader->end();
		if (csv_it == iter_end) return true;

		idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
		for (auto &key: required_keys) {
			// Find keys in the schema and extract idxs
			auto key_it = std::find(key_names.begin(), key_names.end(), key);
			if (key_it != key_names.end()) {
				idx_t key_idx = key_it - key_names.begin();
				required_key_column_idxs.push_back(key_idx);
			} else {
				throw InvalidInputException("");
			}
		}
		for (; csv_it != iter_end; csv_it++) {
			if (current_index == STANDARD_VECTOR_SIZE) break;
			auto &row = *csv_it;
			for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
				csv::CSVField csv_field = row[required_key_column_idxs[i]];
				//output.SetValue(i, current_index, CSVValToValue(csv_field, types[i]));
				// output.SimpleSetValue(i, current_index, CSVValToValue(csv_field, types[i]));
				SetValueFromCSV(types[i], output, i, current_index, csv_field);
			}
			current_index++;
		}
		
		output.SetCardinality(current_index);
		return false;
	}

	// Same Logic as ReadVertexJsonFile
	bool ReadEdgeCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
		auto iter_end = reader->end();
		if (csv_it == iter_end) return true;

		idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
		for (auto &key: required_keys) {
			// Find keys in the schema and extract idxs
			auto key_it = std::find(key_names.begin(), key_names.end(), key);
			if (key_it != key_names.end()) {
				idx_t key_idx = key_it - key_names.begin();
				required_key_column_idxs.push_back(key_idx);
			} else {
				throw InvalidInputException("");
			}
		}
		for (; csv_it != iter_end; csv_it++) {
			if (current_index == STANDARD_VECTOR_SIZE) break;
			auto &row = *csv_it;
			for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
				csv::CSVField csv_field = move(row[required_key_column_idxs[i]]);
				output.SimpleSetValue(i, current_index, CSVValToValue(csv_field, types[i]));
				// SetValueFromCSV(types[i], output, i, current_index, csv_field);
				//output.SimpleSetValue<types[i]>(i, current_index, csv_field);
			}
			current_index++;
		}
		
		output.SetCardinality(current_index);
		return false;
	}
private:
    LogicalType StringToLogicalType(std::string &type_name, size_t column_idx) {
		const auto end = m.end();
		auto it = m.find(type_name);
		if (it != end) {
			return it->second;
		} else {
			if (type_name.find("ID") != std::string::npos) {
				// ID Column
				if (type == GraphComponentType::VERTEX) {
					D_ASSERT(key_column == -1);
					key_column = column_idx;
				} else { // type == GraphComponentType::EDGE
					D_ASSERT((src_column == -1) || (dst_column == -1));
					auto first_pos = type_name.find_first_of('(');
					auto last_pos = type_name.find_last_of(')');
					string label_name = type_name.substr(first_pos + 1, last_pos - first_pos - 1);
					if (src_column == -1) {
						src_key_name = move(label_name);
						src_column = column_idx;
					} else {
						dst_key_name = move(label_name);
						dst_column = column_idx;
					}
				}
				return LogicalType::UBIGINT;
			} else {
				throw InvalidInputException("");
			}
		}
    }

private:
    GraphComponentType type;
    csv::CSVReader *reader;
    csv::CSVReader::iterator csv_it;
    csv::CSVFormat csv_format;
    vector<string> key_names;
	string src_key_name;
	string dst_key_name;
    vector<LogicalType> key_types;
	int64_t key_column = -1;
	int64_t src_column = -1;
	int64_t dst_column = -1;

	unordered_map<string, LogicalType> m {
		{"STRING", LogicalType(LogicalTypeId::VARCHAR)},
		{"STRING[]", LogicalType(LogicalTypeId::VARCHAR)},
		{"INT"   , LogicalType(LogicalTypeId::INTEGER)},
		{"LONG"  , LogicalType(LogicalTypeId::BIGINT)},
	};
};

} // namespace duckdb
