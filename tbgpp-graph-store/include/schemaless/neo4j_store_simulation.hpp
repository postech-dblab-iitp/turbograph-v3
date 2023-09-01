#pragma once

#include <limits.h>
#include <vector>
#include <random>

#include "common/common.hpp"
#include "common/types/chunk_collection.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"

#include <boost/functional/hash.hpp>
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>

// simdcsv
#include "third_party/csv-parser/csv.hpp"
#include "graph_simdcsv_parser.hpp"
#include "schemaless/schema_hash_table.hpp"

// #define USE_NONNULL_EXPRESSION // wrong result..

namespace duckdb {

#define NEO4J_NODE_RECORD_SIZE 15
#define NEO4J_PROPERTY_RECORD_SIZE 41
#define HASH_TABLE_SIZE 1000
// #define USE_HASH_TABLE

enum class Neo4JTypeId : uint8_t {
    BOOL = 1,
    BYTE = 2,
    SHORT = 3,
    CHAR = 4,
    INT = 5,
    LONG = 6,
    FLOAT = 7,
    DOUBLE = 8,
    STRING_REFERENCE = 9,
    ARRAY_REFERENCE = 10,
    SHORT_STRING = 11,
    SHORT_ARRAY = 12,
    GEOMETRY = 13
};

inline Neo4JTypeId ConvertLogicalTypeToNeo4JType(LogicalTypeId type_id) {
    switch (type_id) {
        case LogicalTypeId::BOOLEAN:
            return Neo4JTypeId::BOOL;
        case LogicalTypeId::SMALLINT:
            return Neo4JTypeId::SHORT;
        case LogicalTypeId::INTEGER:
            return Neo4JTypeId::INT;
        case LogicalTypeId::BIGINT:
            return Neo4JTypeId::LONG;
        case LogicalTypeId::ID:
        case LogicalTypeId::UBIGINT:
            return Neo4JTypeId::LONG;
        case LogicalTypeId::FLOAT:
            return Neo4JTypeId::FLOAT;
        case LogicalTypeId::DOUBLE:
            return Neo4JTypeId::DOUBLE;
        case LogicalTypeId::VARCHAR:
            return Neo4JTypeId::SHORT_STRING;
        case LogicalTypeId::POINTER:
            return Neo4JTypeId::STRING_REFERENCE;
        default:
            break;
    }
    return Neo4JTypeId::LONG;
}

// [    ,   x] in use bit
// [    ,xxx ] higher bits for rel id
// [xxxx,    ] higher bits for prop id
struct Neo4JNode {
    uint8_t in_use;
    uint32_t nextRelId;
    uint32_t nextPropId;
    uint64_t labels; // : 40;
    uint8_t extra;

    Neo4JNode(uint8_t in_use_, uint32_t nextRelId_, uint32_t nextPropId_, uint64_t labels_, uint8_t extra_) {
        in_use = in_use_;
        nextRelId = nextRelId_;
        nextPropId = nextPropId_;
        labels = labels_;
        extra = extra_;
    }
}; // 1 + 4 + 4 + 5 (or 8?) + 1 = 15 (or 18) -> 24byte because of padding

    // private static int typeIdentifier(long propBlock) {
    //     return (int) ((propBlock & 0x000000000F000000L) >> 24);
    // }

struct Neo4JProperty {
    uint8_t high_bits;
    uint32_t next; // doubly linked list
    uint32_t prev; // doubly linked list
    uint64_t b1;
    uint64_t b2;
    uint64_t b3;
    uint64_t b4;

    Neo4JProperty() {
        b1 = 0;
        b2 = 0;
        b3 = 0;
        b4 = 0;
    }
}; // mostly use 2 >= blocks..

int maxStrLength(int payloadSize) {
    // key-type-encoding-length
    return ((payloadSize << 3) - 24 - 4 - 4 - 6) / 8;
}

inline void OffsetMemCpy(uint8_t* pDest, const uint8_t* pSrc, const uint8_t srcBitOffset, const size_t size) {
    if (srcBitOffset == 0) {
        for (size_t i = 0; i < size; ++i) { pDest[i] = pSrc[i]; }
    } else if (size > 0) {
        uint8_t v0 = 0, v1 = 0;
        for (size_t i = 0; i < size; i++) {
            v0 = pSrc[i];
            pDest[i] = (v0 << srcBitOffset) | (v1 >> (CHAR_BIT - srcBitOffset));
            v1 = v0;
        }
        pDest[size] = (v1 >> (CHAR_BIT - srcBitOffset));
    }
}

inline void SetNeo4JPropertyFromCSV(LogicalType type, int64_t key_id, vector<Neo4JProperty> &property_store, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset, vector<int64_t> &num_data_per_type) {
	// auto data_ptr = output.data[i].GetData();
    Neo4JProperty new_property;
    // TODO set key and type
    size_t string_size = end_offset - start_offset;
    Neo4JTypeId n_tid = ConvertLogicalTypeToNeo4JType(type.id());
    num_data_per_type[(uint8_t)n_tid]++;
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
            D_ASSERT(false);
            break;
            // std::from_chars(p[start_offset], string_size, ((bool *)data_ptr)[current_index]); break;
			//((bool *)data_ptr)[current_index] = val.get<bool>(); break;
		case LogicalTypeId::TINYINT:
            D_ASSERT(false);
            // std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::SMALLINT:
            D_ASSERT(false);
			// std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::INTEGER:
            D_ASSERT(false);
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
            // std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::BIGINT: {
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
            int64_t value;
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, value);
            std::memcpy((void *)&new_property.b2, (void *)(&value), sizeof(int64_t)); break;
        }
		case LogicalTypeId::UTINYINT:
            D_ASSERT(false);
			// std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::USMALLINT:
            D_ASSERT(false);
			// std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UINTEGER:
            D_ASSERT(false);
			// std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UBIGINT:
        case LogicalTypeId::ID:
        case LogicalTypeId::ADJLISTCOLUMN: {
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
			// std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint64_t *)data_ptr)[current_index]); break;
            n_tid = ConvertLogicalTypeToNeo4JType(LogicalTypeId::UBIGINT);
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, new_property.b2); break;
        }
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt"); break;
		case LogicalTypeId::DECIMAL:
            D_ASSERT(false);
			// uint8_t width, scale;
            // type.GetDecimalProperties(width, scale);
            // switch(type.InternalType()) {
            //     case PhysicalType::INT16: {
            //         int16_t val_before_decimal_point, val_after_decimal_point;
            //         std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
            //         std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
            //         ((int16_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
            //         break;
            //     }
            //     case PhysicalType::INT32: {
            //         int32_t val_before_decimal_point, val_after_decimal_point;
            //         std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
            //         std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
            //         ((int32_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
            //         break;
            //     }
            //     case PhysicalType::INT64: {
            //         int64_t val_before_decimal_point, val_after_decimal_point;
            //         std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
            //         std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
            //         ((int64_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
            //         break;
            //     }
            //     case PhysicalType::INT128:
            //         hugeint_t val;
            //         throw NotImplementedException("Hugeint type for Decimal");
            //     default:
            //         throw InvalidInputException("Unsupported type for Decimal");
            // }
            // break;
		case LogicalTypeId::FLOAT:
            D_ASSERT(false);
            // std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((float *)data_ptr)[current_index]); break;
		case LogicalTypeId::DOUBLE: {
            double value;
            std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, value);
            std::memcpy((void *)(&new_property.b2), (void *)(&value), sizeof(double)); break;
        }
		case LogicalTypeId::VARCHAR:
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
            if (string_size > maxStrLength(32)) {
                // n_tid = Neo4JTypeId::STRING_REFERENCE;

                // TODO currently save only prefix
                n_tid = Neo4JTypeId::SHORT_STRING;
                string_size = 27; // max string size

                uint64_t str_len = string_size << 33;
                uint8_t *dst_ptr = ((uint8_t *)&new_property.b1) + 5;
                OffsetMemCpy(dst_ptr, (const uint8_t*)p.data() + start_offset, 0, string_size);
                new_property.b1 = new_property.b1 | str_len;
            } else {
                n_tid = Neo4JTypeId::SHORT_STRING;
                // fprintf(stdout, "Short string size = %ld, %d\n", string_size, maxStrLength(32));
                uint64_t str_len = string_size << 33;
                // uint8_t *dst_ptr = ((uint8_t *)&new_property.b1) + 4;
                // OffsetMemCpy(dst_ptr, (const uint8_t*)p.data() + start_offset, 7, string_size);
                uint8_t *dst_ptr = ((uint8_t *)&new_property.b1) + 5;
                OffsetMemCpy(dst_ptr, (const uint8_t*)p.data() + start_offset, 0, string_size);
                new_property.b1 = new_property.b1 | str_len;
            }
			// ((string_t *)data_ptr)[current_index] = StringVector::AddStringOrBlob(output.data[i], (const char*)p.data() + start_offset, string_size); break;
            break;
        case LogicalTypeId::DATE:
            D_ASSERT(false);
            // ((date_t *)data_ptr)[current_index] = Date::FromCString((const char*)p.data() + start_offset, end_offset - start_offset); break;
		default:
			throw NotImplementedException("SetValueFromCSV - Unsupported type");
	}
    uint64_t n_tid_64bit = ((uint64_t) n_tid) << 24;
    key_id = key_id & 0xFFFFFF;
    new_property.b1 = new_property.b1 | n_tid_64bit | key_id;
    property_store.push_back(new_property);
}

inline void SetFullColumnValueFromCSV(LogicalType type, unique_ptr<DataChunk> &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output->data[i].GetData();
    size_t string_size = end_offset - start_offset;
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
            D_ASSERT(false);
            break;
            // std::from_chars(p[start_offset], string_size, ((bool *)data_ptr)[current_index]); break;
			//((bool *)data_ptr)[current_index] = val.get<bool>(); break;
		case LogicalTypeId::TINYINT:
            std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::SMALLINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::INTEGER:
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
            std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::BIGINT:
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int64_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UTINYINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::USMALLINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UINTEGER:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UBIGINT:
        case LogicalTypeId::ID:
        case LogicalTypeId::ADJLISTCOLUMN:
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint64_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt"); break;
		case LogicalTypeId::DECIMAL:
			uint8_t width, scale;
            type.GetDecimalProperties(width, scale);
            switch(type.InternalType()) {
            case PhysicalType::INT16: {
                int16_t val_before_decimal_point, val_after_decimal_point;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
                std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
                ((int16_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
                break;
            }
            case PhysicalType::INT32: {
                int32_t val_before_decimal_point, val_after_decimal_point;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
                std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
                ((int32_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
                break;
            }
            case PhysicalType::INT64: {
                int64_t val_before_decimal_point, val_after_decimal_point;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
                std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
                ((int64_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
                break;
            }
            case PhysicalType::INT128:
                hugeint_t val;
                throw NotImplementedException("Hugeint type for Decimal");
            default:
                throw InvalidInputException("Unsupported type for Decimal");
        }
        break;
		case LogicalTypeId::FLOAT:
            std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((float *)data_ptr)[current_index]); break;
		case LogicalTypeId::DOUBLE:
            std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((double *)data_ptr)[current_index]); break;
		case LogicalTypeId::VARCHAR:
            // for (size_t j = start_offset; j < end_offset; j++) {
            //     std::cout << p[j];
            // }
            // std::cout << std::endl;
            if (string_size > 27) string_size = 27; // max string size in neo4j
			((string_t *)data_ptr)[current_index] = StringVector::AddStringOrBlob(output->data[i], (const char*)p.data() + start_offset, string_size); break;
        case LogicalTypeId::DATE:
            ((date_t *)data_ptr)[current_index] = Date::FromCString((const char*)p.data() + start_offset, end_offset - start_offset); break;
		default:
			throw NotImplementedException("SetValueFromCSV - Unsupported type");
	}
}

class FullColumnarFormatStore {
public:
    FullColumnarFormatStore() {}
    ~FullColumnarFormatStore() {}

    FullColumnarFormatStore(uint64_t num_tuples, const char *csv_file_path) : num_tuples(num_tuples) {
        std::cout << "Load FullColumnarFormatStore Start!!" << std::endl;
        LoadCSVFile(csv_file_path);
        std::cout << "Load FullColumnarFormatStore Done!!" << std::endl;
    }

    void LoadCSVFile(const char *csv_file_path) {
        boost::timer::cpu_timer csv_load_timer;
        boost::timer::cpu_timer storage_load_timer;
        double csv_load_time, storage_load_time;

        csv_load_timer.start();
        // Initialize CSV Reader & iterator
        try {
            p = get_corpus(csv_file_path, CSV_PADDING);
        } catch (const std::exception &e) { // caught by reference to base
            throw InvalidInputException("Could not load the file");
        }

        // Read only header.. TODO how to read only few lines or just a line?
        csv::CSVFormat csv_form;
        csv_form.delimiter('|')
                .header_row(0);
        csv::CSVReader *reader = new csv::CSVReader(csv_file_path, csv_form);

        // Parse CSV File
        // std::cout << "File: " << csv_file_path << ", string size: " << p.size() << std::endl;
        pcsv.indexes = new (std::nothrow) uint32_t[p.size()]; // can't have more indexes than we have data
        if(pcsv.indexes == nullptr) {
            throw InvalidInputException("You are running out of memory.");
        }
        find_indexes(p.data(), p.size(), pcsv);

        // Parse header
        vector<string> col_names = move(reader->get_col_names());
        num_columns = col_names.size();
        num_rows = pcsv.n_indexes / num_columns;
        fprintf(stdout, "n_indexes = %ld, num_columns = %ld, num_rows = %ld\n", pcsv.n_indexes, num_columns, num_rows);
        D_ASSERT((pcsv.n_indexes % num_columns == 0) || (pcsv.n_indexes % num_columns == num_columns - 1)); // no newline at the end of file
        for (size_t i = 0; i < col_names.size(); i++) {
            // Assume each element in the header column is of format 'key:type'
            std::string key_and_type = col_names[i]; 
            std::cout << "\t" << key_and_type << std::endl;
            size_t delim_pos = key_and_type.find(':');
            if (delim_pos == std::string::npos) throw InvalidInputException("D");
            std::string key = key_and_type.substr(0, delim_pos);
            if (key == "") {
                // special case
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                if (type_name.find("START_ID") != std::string::npos) {
                    // key_names.push_back(src_key_name + "_src_" + std::to_string(src_columns.size()));
                    key_names.push_back("_sid");
                } else {
                    // key_names.push_back(dst_key_name + "_dst_" + std::to_string(dst_columns.size()));
                    key_names.push_back("_tid");
                }
                key_types.push_back(move(type));
            } else {
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                // auto key_it = key_map.find(key);
                // if (key_it == key_map.end()) {
                //     int64_t new_key_id = GetNewKeyVer();
                //     key_map.insert(std::make_pair(key, new_key_id));
                // }
                key_names.push_back(move(key));
                key_types.push_back(move(type));
            }
        }

        invalid_count_per_column.resize(num_columns, 0);

        // Initialize Cursor
        row_cursor = 1; // After the header
        index_cursor = num_columns;
        delete reader;

        csv_load_time = csv_load_timer.elapsed().wall / 1000000.0;

        storage_load_timer.start();
        // Load
        idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
        unique_ptr<DataChunk> newChunk = std::make_unique<DataChunk>();
        newChunk->Initialize(key_types);

        for (; row_cursor < num_rows; row_cursor++) {
            if ((((row_cursor - 1) % STANDARD_VECTOR_SIZE) == 0) && (row_cursor > 1)) {
                newChunk->SetCardinality(STANDARD_VECTOR_SIZE);
                node_store.Append(move(newChunk));
                newChunk = std::make_unique<DataChunk>();
                newChunk->Initialize(key_types);
                current_index = 0;
            }

			for (size_t i = 0; i < num_columns; i++) {
                idx_t target_index = index_cursor + i;//required_key_column_idxs[i];
                idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
                idx_t end_offset = pcsv.indexes[target_index];
                if (start_offset == end_offset) {  // null data case
                    newChunk->data[i].SetValue(current_index, Value(key_types[i])); // Set null Value
                    invalid_count_per_column[i]++;
                    continue;
                }
                if ((i == num_columns - 1) && (end_offset - start_offset == 1)) { // newline, null case
                    newChunk->data[i].SetValue(current_index, Value(key_types[i])); // Set null Value
                    invalid_count_per_column[i]++;
                    continue;
                }
                SetFullColumnValueFromCSV(key_types[i], newChunk, i, current_index, p, start_offset, end_offset);
            }

            current_index++;
            index_cursor += num_columns;
		}
        int64_t last_chunk_cardinality = current_index;
        newChunk->SetCardinality(last_chunk_cardinality);
        node_store.Append(move(newChunk));

        storage_load_time = storage_load_timer.elapsed().wall / 1000000.0;

        std::cout << "CSV Load Elapsed Time: " << csv_load_time << " ms, Storage Load Elapsed Time: " << storage_load_time << " ms" << std::endl;
    }

    LogicalType StringToLogicalType(std::string &type_name, size_t column_idx) {
        const auto end = m.end();
        auto it = m.find(type_name);
        if (it != end) {
            LogicalType return_type = it->second;
            return return_type;
        } else {
            if (type_name.find("ID") != std::string::npos) {
                // ID Column
                if (true) {
                    auto last_pos = type_name.find_first_of('(');
                    string id_name = type_name.substr(0, last_pos - 1);
                    auto delimiter_pos = id_name.find("_");
                    if (delimiter_pos != std::string::npos) {
                        // Multi key
                        auto key_order = std::stoi(type_name.substr(delimiter_pos + 1, last_pos - delimiter_pos - 1));
                        key_columns_order.push_back(key_order);
                        key_columns.push_back(column_idx);
                    } else {
                        // Single key
                        key_columns.push_back(column_idx);
                    }
                    return LogicalType::UBIGINT;
                } else { // type == GraphComponentType::EDGE
                    // D_ASSERT((src_column == -1) || (dst_column == -1));
                    auto first_pos = type_name.find_first_of('(');
                    auto last_pos = type_name.find_last_of(')');
                    string label_name = type_name.substr(first_pos + 1, last_pos - first_pos - 1);
                    std::cout << type_name << std::endl;
                    if (type_name.find("START_ID") != std::string::npos) {
                        src_key_name = move(label_name);
                        src_columns.push_back(column_idx);
                    } else { // "END_ID"
                        dst_key_name = move(label_name);
                        dst_columns.push_back(column_idx);
                    }
                    // return LogicalType::ID;
                    return LogicalType::UBIGINT;
                }
            } else if (type_name.find("DECIMAL") != std::string::npos) {
                auto first_pos = type_name.find_first_of('(');
                auto comma_pos = type_name.find_first_of(',');
                auto last_pos = type_name.find_last_of(')');
                int width = std::stoi(type_name.substr(first_pos + 1, comma_pos - first_pos - 1));
                int scale = std::stoi(type_name.substr(comma_pos + 1, last_pos - comma_pos - 1));
                return LogicalType::DECIMAL(width, scale);
            } else {
                fprintf(stdout, "%s\n", type_name.c_str());
                throw InvalidInputException("Unsupported Type");
            }
        }
    }

// full columnar storage
    void FullScanQuery() {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        // fprintf(stdout, "ChunkCount = %ld, ColumnCount = %ld\n", node_store.ChunkCount(), node_store.ColumnCount());
        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            for (int64_t colidx = 0; colidx < node_store.ColumnCount(); colidx++) {
                // fprintf(stdout, "colidx %ld, invalid count = %ld\n", colidx, invalid_count_per_column[colidx]);
                if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
                const Vector &vec = cur_chunk.data[colidx];
                if (key_types[colidx] == LogicalType::BIGINT) {
                    int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_int += cur_chunk_vec_ptr[i];
                        }
                    } else {
#ifdef USE_NONNULL_EXPRESSION
#else
                        auto &val_mask = FlatVector::Validity(vec);
                        if (val_mask.AllValid()) {
                            // we don't need nullity check
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        } else if (val_mask.CheckAllInValid()) {
                            // skip this chunk
                            continue;
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_int += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
#endif
                    }
                } else if (key_types[colidx] == LogicalType::UBIGINT) {
                    uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    } else {
#ifdef USE_NONNULL_EXPRESSION
#else
                        auto &val_mask = FlatVector::Validity(vec);
                        if (val_mask.AllValid()) {
                            // we don't need nullity check
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_uint += cur_chunk_vec_ptr[i];
                            }
                        } else if (val_mask.CheckAllInValid()) {
                            // skip this chunk
                            continue;
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_uint += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
#endif
                    }
                } else if (key_types[colidx] == LogicalType::DOUBLE) {
                    double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_double += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        auto &val_mask = FlatVector::Validity(vec);
                        if (val_mask.AllValid()) {
                            // we don't need nullity check
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_double += cur_chunk_vec_ptr[i];
                            }
                        } else if (val_mask.CheckAllInValid()) {
                            // skip this chunk
                            continue;
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_double += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::VARCHAR) {
                    data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            auto str = ((string_t *)vec_data)[i];
                            idx_t str_size = str.GetSize();
                            const char *str_data = str.GetDataUnsafe();
                            for (int64_t j = 0; j < str_size; j++) {
                                sum_str += str_data[j];
                            }
                        }
                    } else {
                        auto &val_mask = FlatVector::Validity(vec);
                        if (val_mask.AllValid()) {
                            // we don't need nullity check
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                auto str = ((string_t *)vec_data)[i];
                                idx_t str_size = str.GetSize();
                                const char *str_data = str.GetDataUnsafe();
                                for (int64_t j = 0; j < str_size; j++) {
                                    sum_str += str_data[j];
                                }
                            }
                        } else if (val_mask.CheckAllInValid()) {
                            // skip this chunk
                            continue;
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    auto str = ((string_t *)vec_data)[i];
                                    idx_t str_size = str.GetSize();
                                    const char *str_data = str.GetDataUnsafe();
                                    for (int64_t j = 0; j < str_size; j++) {
                                        sum_str += str_data[j];
                                    }
                                }
                            }
                        }
                    }
                } else {
                    D_ASSERT(false);
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void SIMDFullScanQuery(DataChunk &output) {
        output.Initialize(key_types);
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        // fprintf(stdout, "ChunkCount = %ld, ColumnCount = %ld\n", node_store.ChunkCount(), node_store.ColumnCount());
        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            for (int64_t colidx = 0; colidx < node_store.ColumnCount(); colidx++) {
                // fprintf(stdout, "colidx %ld, invalid count = %ld\n", colidx, invalid_count_per_column[colidx]);
                if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
                const Vector &vec = cur_chunk.data[colidx];
                if (key_types[colidx] == LogicalType::BIGINT) {
                    int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                    __m256i vsum = _mm256_set1_epi64x(0);
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < (cur_chunk.size() / 4) * 4; i = i + 4) {
                            __m256i v = _mm256_load_epi64(&cur_chunk_vec_ptr[i]);
                            vsum = _mm256_add_epi64(vsum, v);
                        }
                        int64_t result_arr[4];
                        _mm256_store_epi64(&result_arr[0], vsum);
                        sum_int += (result_arr[0] + result_arr[1] + result_arr[2] + result_arr[3]);
                        for (int64_t i = (cur_chunk.size() / 4) * 4; i < cur_chunk.size(); i++) {
                            sum_int += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::INTEGER) {
                    int32_t *cur_chunk_vec_ptr = (int32_t *)cur_chunk.data[colidx].GetData();
                    __m256i vsum = _mm256_set1_epi32(0);
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < (cur_chunk.size() / 8) * 8; i = i + 8) {
                            __m256i v = _mm256_load_epi32(&cur_chunk_vec_ptr[i]);
                            vsum = _mm256_add_epi32(vsum, v);
                        }
                        int32_t result_arr[8];
                        _mm256_store_epi32(&result_arr[0], vsum);
                        sum_int += (result_arr[0] + result_arr[1] + result_arr[2] + result_arr[3] +
                                    result_arr[4] + result_arr[5] + result_arr[6] + result_arr[7]);
                        for (int64_t i = (cur_chunk.size() / 8) * 8; i < cur_chunk.size(); i++) {
                            sum_int += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::UBIGINT) {
                    uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                    __m256i vsum = _mm256_set1_epi64x(0);
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < (cur_chunk.size() / 4) * 4; i = i + 4) {
                            __m256i v = _mm256_load_epi64(&cur_chunk_vec_ptr[i]);
                            vsum = _mm256_add_epi64(vsum, v);
                        }
                        uint64_t result_arr[4];
                        _mm256_store_epi64(&result_arr[0], vsum);
                        sum_uint += (result_arr[0] + result_arr[1] + result_arr[2] + result_arr[3]);
                        for (int64_t i = (cur_chunk.size() / 4) * 4; i < cur_chunk.size(); i++) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::UINTEGER) {
                    uint32_t *cur_chunk_vec_ptr = (uint32_t *)cur_chunk.data[colidx].GetData();
                    __m256i vsum = _mm256_set1_epi32(0);
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < (cur_chunk.size() / 8) * 8; i = i + 8) {
                            __m256i v = _mm256_load_epi32(&cur_chunk_vec_ptr[i]);
                            vsum = _mm256_add_epi32(vsum, v);
                        }
                        uint32_t result_arr[8];
                        _mm256_store_epi32(&result_arr[0], vsum);
                        sum_uint += (result_arr[0] + result_arr[1] + result_arr[2] + result_arr[3] +
                                     result_arr[4] + result_arr[5] + result_arr[6] + result_arr[7]);
                        for (int64_t i = (cur_chunk.size() / 8) * 8; i < cur_chunk.size(); i++) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::DOUBLE) {
                    double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_double += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_double += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
                } else if (key_types[colidx] == LogicalType::VARCHAR) {
                    data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[colidx] == 0) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            auto str = ((string_t *)vec_data)[i];
                            idx_t str_size = str.GetSize();
                            const char *str_data = str.GetDataUnsafe();
                            for (int64_t j = 0; j < str_size; j++) {
                                sum_str += str_data[j];
                            }
                        }
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                auto str = ((string_t *)vec_data)[i];
                                idx_t str_size = str.GetSize();
                                const char *str_data = str.GetDataUnsafe();
                                for (int64_t j = 0; j < str_size; j++) {
                                    sum_str += str_data[j];
                                }
                            }
                        }
                    }
                } else {
                    D_ASSERT(false);
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void TargetColScanQuery(int target_col) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;
        // int a, b, c, d;
        // a = b = c = d = 0;

        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            auto colidx = target_col;
            if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
            const Vector &vec = cur_chunk.data[colidx];
            if (key_types[colidx] == LogicalType::BIGINT) {
                int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    // a++;
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_int += cur_chunk_vec_ptr[i];
                    }
                } else {
#ifdef USE_NONNULL_EXPRESSION
                    idx_t result_count = nonnull_expr_executor.SelectExpression(cur_chunk, sel_vec);
                    if (result_count == cur_chunk.size()) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_int += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < result_count; i++) {
                            sum_int += cur_chunk_vec_ptr[sel_vec.get_index(i)];
                        }
                    }
#else
                    auto &val_mask = FlatVector::Validity(vec);
                    if (val_mask.AllValid()) {
                        // we don't need nullity check
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_int += cur_chunk_vec_ptr[i];
                        }
                    } else if (val_mask.CheckAllInValid()) {
                        // skip this chunk
                        continue;
                    } else {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            if (!FlatVector::IsNull(vec, i)) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        }
                    }
#endif
                }
            } else if (key_types[colidx] == LogicalType::UBIGINT) {
                uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_uint += cur_chunk_vec_ptr[i];
                    }
                } else {
#ifdef USE_NONNULL_EXPRESSION
                    idx_t result_count = nonnull_expr_executor.SelectExpression(cur_chunk, sel_vec);
                    if (result_count == cur_chunk.size()) {
                        for (int64_t i = 0; i < cur_chunk.size(); i++) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    } else {
                        for (int64_t i = 0; i < result_count; i++) {
                            sum_uint += cur_chunk_vec_ptr[sel_vec.get_index(i)];
                        }
                    }
#else
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    }
#endif
                }
            } else if (key_types[colidx] == LogicalType::DOUBLE) {
                double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_double += cur_chunk_vec_ptr[i];
                    }
                } else {
#ifdef USE_NONNULL_EXPRESSION
#else
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            sum_double += cur_chunk_vec_ptr[i];
                        }
                    }
#endif
                }
            } else if (key_types[colidx] == LogicalType::VARCHAR) {
                data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        auto str = ((string_t *)vec_data)[i];
                        idx_t str_size = str.GetSize();
                        const char *str_data = str.GetDataUnsafe();
                        for (int64_t j = 0; j < str_size; j++) {
                            sum_str += str_data[j];
                        }
                    }
                } else {
#ifdef USE_NONNULL_EXPRESSION
#else
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            auto str = ((string_t *)vec_data)[i];
                            idx_t str_size = str.GetSize();
                            const char *str_data = str.GetDataUnsafe();
                            for (int64_t j = 0; j < str_size; j++) {
                                sum_str += str_data[j];
                            }
                        }
                    }
#endif
                }
            } else {
                D_ASSERT(false);
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
        // fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d, a = %d, b = %d, c = %d, d = %d\n",
        //     total, sum_int, sum_uint, sum_double, sum_str, a, b, c, d);
    }

    void TargetColRangeQuery(int target_col, int begin, int end) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            int64_t colidx = target_col;
            if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
            const Vector &vec = cur_chunk.data[colidx];
            if (key_types[colidx] == LogicalType::BIGINT) {
                int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                idx_t result_count = expr_executor.SelectExpression(cur_chunk, sel_vec);
                if (result_count == cur_chunk.size()) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_int += cur_chunk_vec_ptr[i];
                    }
                } else {
                    for (int64_t i = 0; i < result_count; i++) {
                        sum_int += cur_chunk_vec_ptr[sel_vec.get_index(i)];
                    }
                }
            } else if (key_types[colidx] == LogicalType::UBIGINT) {
                uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_uint += cur_chunk_vec_ptr[i];
                    }
                } else {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            sum_uint += cur_chunk_vec_ptr[i];
                        }
                    }
                }
            } else if (key_types[colidx] == LogicalType::DOUBLE) {
                double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_double += cur_chunk_vec_ptr[i];
                    }
                } else {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            sum_double += cur_chunk_vec_ptr[i];
                        }
                    }
                }
            } else if (key_types[colidx] == LogicalType::VARCHAR) {
                data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        auto str = ((string_t *)vec_data)[i];
                        idx_t str_size = str.GetSize();
                        const char *str_data = str.GetDataUnsafe();
                        for (int64_t j = 0; j < str_size; j++) {
                            sum_str += str_data[j];
                        }
                    }
                } else {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        if (!FlatVector::IsNull(vec, i)) {
                            auto str = ((string_t *)vec_data)[i];
                            idx_t str_size = str.GetSize();
                            const char *str_data = str.GetDataUnsafe();
                            for (int64_t j = 0; j < str_size; j++) {
                                sum_str += str_data[j];
                            }
                        }
                    }
                }
            } else {
                D_ASSERT(false);
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d, begin = %d, end = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str, begin, end);
    }

    void generateExpressionExecutor(int target_col, int begin, int end) {
        vector<unique_ptr<Expression>> filter_exprs;
        {
            unique_ptr<Expression> filter_expr1;
            filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::BIGINT, target_col),
                                make_unique<BoundConstantExpression>(Value::BIGINT( begin ))
                            );
            unique_ptr<Expression> filter_expr2;
            filter_expr2 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::BIGINT, target_col),
                                make_unique<BoundConstantExpression>(Value::BIGINT( end ))
                            );
            filter_exprs.push_back(move(filter_expr1));
            filter_exprs.push_back(move(filter_expr2));
        }

        auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
        for (auto &expr : filter_exprs) {
            conjunction->children.push_back(move(expr));
        }
        expression = move(conjunction);
        expr_executor.expressions.clear();
        expr_executor.AddExpression(*expression);
        if (sel_vec.data() == nullptr)
            sel_vec.Initialize();
    }

    void generateNonNullExpressionExecutor(int target_col) {
        unique_ptr<Expression> filter_expr1;
        filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, 
                            make_unique<BoundReferenceExpression>(LogicalType::BIGINT, target_col),
                            make_unique<BoundConstantExpression>(Value(LogicalType::BIGINT))
                        );
        
        nonnull_expression = move(filter_expr1);
        nonnull_expr_executor.expressions.clear();
        nonnull_expr_executor.AddExpression(*nonnull_expression);
        if (sel_vec.data() == nullptr)
            sel_vec.Initialize();
    }

    void test4() {
        std::random_device rd;
        std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device

        constexpr int kSize = 1024 * 1024; // 1M

        // Create a reusable random number generator that generates uniform numbers between 1 and 1048576
        std::uniform_int_distribution<> ran_gen(0, kSize);

        ChunkCollection data;

        bool is_point_query = true;

        for (auto i = 0; i < kSize; i+= STANDARD_VECTOR_SIZE) {
            int32_t chunk_size = std::min<int32_t>(STANDARD_VECTOR_SIZE, kSize - i);
            unique_ptr<DataChunk> data_chunk = std::make_unique<DataChunk>();
            data_chunk->Initialize({LogicalType::INTEGER});
            data_chunk->SetCardinality(STANDARD_VECTOR_SIZE);
            data.Append(move(data_chunk));
        }

        // auto *data_vec = (int32_t *)data.data[0].GetData();
        for (auto i = 0; i < kSize; i+= 2) {
            data.SetValue(0, i, Value::INTEGER(i / 2));
            data.SetValue(0, i + 1, Value::INTEGER(kSize - i));
        }

        for (auto i = 0; i < 1024; i++) {
            if (is_point_query) {
                int val = ran_gen(mersenne);

                test4_point_query(val, data, kSize);
            } else {
                int begin = ran_gen(mersenne);
                int end = ran_gen(mersenne);
                if (begin > end) std::swap(begin, end);

                test4_internal2(begin, end, data, kSize);
            }
        }
    }

    void test4_internal(const int begin, const int end) {
        boost::timer::cpu_timer test4_timer;
        double test4_time;

        constexpr int kSize = 1024 * 1024; // 1M

        DataChunk data;
        data.Initialize({LogicalType::INTEGER}, kSize);

        auto *data_vec = (int32_t *)data.data[0].GetData();
        for (auto i = 0; i < kSize; i+= 2) {
            data_vec[i] = i / 2;
            data_vec[i + 1] = kSize - i;
        }

        test4_timer.start();
        vector<int32_t> results;
        for (auto rowIndex = 0; rowIndex < kSize; rowIndex++) {
            if (data_vec[rowIndex] >= begin && data_vec[rowIndex] <= end) {
                results.push_back(data_vec[rowIndex]);
            }
        }
        test4_time = test4_timer.elapsed().wall / 1000000.0;

        int32_t passedCount = 0;
        for (auto i = 0; i < results.size(); i++) {
            if (results[i] >= begin && results[i] <= end) {
                ++passedCount;
            }
        }

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[FullColumnar] Test4 kStep = " << 1 << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    void test4_internal2(const int begin, const int end, ChunkCollection &data, int32_t kSize) {
        icecream::ic.enable();
        boost::timer::cpu_timer test4_timer;
        double test4_time;

        ChunkCollection outputs;

        vector<unique_ptr<Expression>> filter_exprs;
        {
            unique_ptr<Expression> filter_expr1;
            filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 0),
                                make_unique<BoundConstantExpression>(Value::INTEGER( begin ))
                            );
            unique_ptr<Expression> filter_expr2;
            filter_expr2 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 0),
                                make_unique<BoundConstantExpression>(Value::INTEGER( end ))
                            );
            filter_exprs.push_back(move(filter_expr1));
            filter_exprs.push_back(move(filter_expr2));
        }

        auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
        for (auto &expr : filter_exprs) {
            conjunction->children.push_back(move(expr));
        }
        unique_ptr<Expression> expression = move(conjunction);
        ExpressionExecutor expr_executor(*expression);
        SelectionVector sel_vec(STANDARD_VECTOR_SIZE);

        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            unique_ptr<DataChunk> output = std::make_unique<DataChunk>();
            output->Initialize({LogicalType::INTEGER});
            outputs.Append(move(output));
        }
        test4_timer.start();
        vector<int32_t> results;
        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            auto &input = data.GetChunk(chunkIndex);
            auto &output = outputs.GetChunk(chunkIndex);

            idx_t result_count = expr_executor.SelectExpression(input, sel_vec);
            if (result_count == input.size()) {
                // nothing was filtered: skip adding any selection vectors
                output.Reference(input);
            } else {
                output.Slice(input, sel_vec, result_count);
            }
        }

        // for (auto rowIndex = 0; rowIndex < kSize; rowIndex++) {
        //     auto row_val = data.GetValue(0, rowIndex);
        //     if (data_vec[rowIndex] >= begin && data_vec[rowIndex] <= end) {
        //         results.push_back(data_vec[rowIndex]);
        //     }
        // }
        test4_time = test4_timer.elapsed().wall / 1000000.0;

        int32_t passedCount = 0;
        for (auto chunkIndex = 0; chunkIndex < outputs.ChunkCount(); chunkIndex++) {
            auto &output = outputs.GetChunk(chunkIndex);
            passedCount += output.size();
        }
        // for (auto i = 0; i < results.size(); i++) {
        //    if (results[i] >= begin && results[i] <= end) {
        //        ++passedCount;
        //    }
        // }

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[FullColumnar] Test4 kStep = " << 1 << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
        icecream::ic.disable();
    }

    void test4_point_query(const int val, ChunkCollection &data, int32_t kSize) {
        icecream::ic.enable();
        boost::timer::cpu_timer test4_timer;
        double test4_time;

        ChunkCollection outputs;

        unique_ptr<Expression> filter_expr1;
        filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, 
                            make_unique<BoundReferenceExpression>(LogicalType::INTEGER, 0),
                            make_unique<BoundConstantExpression>(Value::INTEGER( val ))
                        );

        ExpressionExecutor expr_executor(*filter_expr1);
        SelectionVector sel_vec(STANDARD_VECTOR_SIZE);

        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            unique_ptr<DataChunk> output = std::make_unique<DataChunk>();
            output->Initialize({LogicalType::INTEGER});
            outputs.Append(move(output));
        }
        test4_timer.start();
        vector<int32_t> results;
        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            auto &input = data.GetChunk(chunkIndex);
            auto &output = outputs.GetChunk(chunkIndex);

            idx_t result_count = expr_executor.SelectExpression(input, sel_vec);
            if (result_count == input.size()) {
                // nothing was filtered: skip adding any selection vectors
                output.Reference(input);
            } else {
                output.Slice(input, sel_vec, result_count);
            }
        }

        test4_time = test4_timer.elapsed().wall / 1000000.0;

        int32_t passedCount = 0;
        for (auto chunkIndex = 0; chunkIndex < outputs.ChunkCount(); chunkIndex++) {
            auto &output = outputs.GetChunk(chunkIndex);
            passedCount += output.size();
        }
        // for (auto i = 0; i < results.size(); i++) {
        //    if (results[i] >= begin && results[i] <= end) {
        //        ++passedCount;
        //    }
        // }

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[FullColumnar] Test4 kStep = " << 1 << ", Exec elapsed: " << test4_time << " ms, val: " << val
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
        icecream::ic.disable();
    }

    void test5(int Size) {
        std::random_device rd;
        std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device

        int kSize = Size * 1024 * 1024;

        // Create a reusable random number generator that generates uniform numbers between 1 and 1048576
        std::uniform_int_distribution<> ran_gen(0, kSize);

        ChunkCollection data;

        bool is_point_query = false;

        for (auto i = 0; i < kSize; i+= STANDARD_VECTOR_SIZE) {
            int32_t chunk_size = std::min<int32_t>(STANDARD_VECTOR_SIZE, kSize - i);
            unique_ptr<DataChunk> data_chunk = std::make_unique<DataChunk>();
            data_chunk->Initialize({LogicalType::BIGINT});
            data_chunk->SetCardinality(STANDARD_VECTOR_SIZE);
            data.Append(move(data_chunk));
        }

        // auto *data_vec = (int32_t *)data.data[0].GetData();
        for (auto i = 0; i < kSize; i+= 2) {
            data.SetValue(0, i, Value::BIGINT(i / 2));
            data.SetValue(0, i + 1, Value::BIGINT(kSize - i));
        }

        for (auto i = 0; i < 1024; i++) {
            if (is_point_query) {
                int val = ran_gen(mersenne);

                test4_point_query(val, data, kSize);
            } else {
                int begin = ran_gen(mersenne);
                int end = ran_gen(mersenne);
                if (begin > end) std::swap(begin, end);

                test5_internal2(begin, end, data, kSize);
            }
        }
    }

    void test5_internal2(const int begin, const int end, ChunkCollection &data, int32_t kSize) {
        icecream::ic.enable();
        boost::timer::cpu_timer test4_timer;
        double test4_time;

        ChunkCollection outputs;

        vector<unique_ptr<Expression>> filter_exprs;
        {
            unique_ptr<Expression> filter_expr1;
            filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 0),
                                make_unique<BoundConstantExpression>(Value::BIGINT( begin ))
                            );
            unique_ptr<Expression> filter_expr2;
            filter_expr2 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, 
                                make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 0),
                                make_unique<BoundConstantExpression>(Value::BIGINT( end ))
                            );
            filter_exprs.push_back(move(filter_expr1));
            filter_exprs.push_back(move(filter_expr2));
        }

        auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
        for (auto &expr : filter_exprs) {
            conjunction->children.push_back(move(expr));
        }
        unique_ptr<Expression> expression = move(conjunction);
        ExpressionExecutor expr_executor(*expression);
        SelectionVector sel_vec(STANDARD_VECTOR_SIZE);

        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            unique_ptr<DataChunk> output = std::make_unique<DataChunk>();
            output->Initialize({LogicalType::BIGINT});
            outputs.Append(move(output));
        }
        test4_timer.start();
        vector<int32_t> results;
        for (auto chunkIndex = 0; chunkIndex < data.ChunkCount(); chunkIndex++) {
            auto &input = data.GetChunk(chunkIndex);
            auto &output = outputs.GetChunk(chunkIndex);

            idx_t result_count = expr_executor.SelectExpression(input, sel_vec);
            if (result_count == input.size()) {
                // nothing was filtered: skip adding any selection vectors
                output.Reference(input);
            } else {
                output.Slice(input, sel_vec, result_count);
            }
        }

        // for (auto rowIndex = 0; rowIndex < kSize; rowIndex++) {
        //     auto row_val = data.GetValue(0, rowIndex);
        //     if (data_vec[rowIndex] >= begin && data_vec[rowIndex] <= end) {
        //         results.push_back(data_vec[rowIndex]);
        //     }
        // }
        test4_time = test4_timer.elapsed().wall / 1000000.0;

        int32_t passedCount = 0;
        for (auto chunkIndex = 0; chunkIndex < outputs.ChunkCount(); chunkIndex++) {
            auto &output = outputs.GetChunk(chunkIndex);
            passedCount += output.size();
        }
        // for (auto i = 0; i < results.size(); i++) {
        //    if (results[i] >= begin && results[i] <= end) {
        //        ++passedCount;
        //    }
        // }

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[FullColumnar] Test4 kStep = " << 1 << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
        icecream::ic.disable();
    }

    vector<string> key_names;
    string src_key_name;
    string dst_key_name;
    vector<LogicalType> key_types;
    vector<int64_t> key_columns;
    vector<int64_t> key_columns_order;
    vector<int64_t> src_columns;
    vector<int64_t> dst_columns;
    int64_t num_columns;
    int64_t num_rows;
    int64_t num_tuples;
    idx_t row_cursor;
    idx_t index_cursor;
    idx_t index_size;
    std::basic_string_view<uint8_t> p;
    ParsedCSV pcsv;
    vector<int64_t> invalid_count_per_column;

    unordered_map<string, int64_t> key_map;
    ChunkCollection node_store;

    // ExpressionExecutor executor;
    unique_ptr<Expression> expression;
    ExpressionExecutor expr_executor;
    unique_ptr<Expression> nonnull_expression;
    ExpressionExecutor nonnull_expr_executor;
    SelectionVector sel_vec;

    unordered_map<string, LogicalType> m {
        {"STRING", LogicalType(LogicalTypeId::VARCHAR)},
        {"STRING[]", LogicalType(LogicalTypeId::VARCHAR)},
        {"INT"   , LogicalType(LogicalTypeId::INTEGER)},
        {"INTEGER"   , LogicalType(LogicalTypeId::INTEGER)},
        {"LONG"  , LogicalType(LogicalTypeId::BIGINT)},
        {"ULONG"  , LogicalType(LogicalTypeId::UBIGINT)},
        {"DATE"  , LogicalType(LogicalTypeId::DATE)},
        {"DECIMAL"  , LogicalType(LogicalTypeId::DECIMAL)},
        {"DOUBLE"   , LogicalType(LogicalTypeId::DOUBLE)}
    };

};

class PartialColumnarFormatStore {

typedef std::pair<string, uint8_t> key_type_pair;

public:
    PartialColumnarFormatStore() {}
    ~PartialColumnarFormatStore() {}

    PartialColumnarFormatStore(uint64_t num_tuples, const char *csv_file_path, int64_t max_allow_edit_distance_, int64_t max_merge_count_) :
        max_allow_edit_distance(max_allow_edit_distance_), max_merge_count(max_merge_count_), schema_hash_table(HASH_TABLE_SIZE) {
        property_key_id_ver = 0;

        // node_store = new uint8_t[num_tuples * NEO4J_NODE_RECORD_SIZE];
        std::cout << "Load PartialColumnarFormatStore Start!!" << std::endl;
        LoadCSVFile(csv_file_path);
        std::cout << "Load PartialColumnarFormatStore Done!!" << std::endl;
    }

    void LoadCSVFile(const char *csv_file_path) {
        boost::timer::cpu_timer csv_load_timer;
        boost::timer::cpu_timer storage_load_timer;
        double csv_load_time, storage_load_time;

        csv_load_timer.start();
        // Initialize CSV Reader & iterator
        try {
            p = get_corpus(csv_file_path, CSV_PADDING);
        } catch (const std::exception &e) { // caught by reference to base
            throw InvalidInputException("Could not load the file");
        }

        // Read only header.. TODO how to read only few lines or just a line?
        csv::CSVFormat csv_form;
        csv_form.delimiter('|')
                .header_row(0);
        csv::CSVReader *reader = new csv::CSVReader(csv_file_path, csv_form);

        // Parse CSV File
        // std::cout << "File: " << csv_file_path << ", string size: " << p.size() << std::endl;
        pcsv.indexes = new (std::nothrow) uint32_t[p.size()]; // can't have more indexes than we have data
        if(pcsv.indexes == nullptr) {
            throw InvalidInputException("You are running out of memory.");
        }
        find_indexes(p.data(), p.size(), pcsv);

        // Parse header
        vector<string> col_names = move(reader->get_col_names());
        num_columns = col_names.size();
        num_rows = pcsv.n_indexes / num_columns;
        fprintf(stdout, "n_indexes = %ld, num_columns = %ld, num_rows = %ld\n", pcsv.n_indexes, num_columns, num_rows);
        D_ASSERT((pcsv.n_indexes % num_columns == 0) || (pcsv.n_indexes % num_columns == num_columns - 1)); // no newline at the end of file
        for (size_t i = 0; i < col_names.size(); i++) {
            // Assume each element in the header column is of format 'key:type'
            std::string key_and_type = col_names[i]; 
            std::cout << "\t" << key_and_type << std::endl;
            size_t delim_pos = key_and_type.find(':');
            if (delim_pos == std::string::npos) throw InvalidInputException("D");
            std::string key = key_and_type.substr(0, delim_pos);
            if (key == "") {
                // special case
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                if (type_name.find("START_ID") != std::string::npos) {
                    // key_names.push_back(src_key_name + "_src_" + std::to_string(src_columns.size()));
                    key_names.push_back("_sid");
                } else {
                    // key_names.push_back(dst_key_name + "_dst_" + std::to_string(dst_columns.size()));
                    key_names.push_back("_tid");
                }
                key_types.push_back(move(type));
            } else {
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                auto key_it = key_map.find(std::make_pair(key, (uint8_t)type.id()));
                if (key_it == key_map.end()) {
                    int64_t new_key_id = GetNewKeyVer();
                    key_map.insert(std::make_pair(std::make_pair(key, (uint8_t)type.id()), new_key_id));
                }
                key_names.push_back(move(key));
                key_types.push_back(move(type));
            }
        }

        // Initialize Cursor
        row_cursor = 1; // After the header
        index_cursor = num_columns;
        delete reader;

        csv_load_time = csv_load_timer.elapsed().wall / 1000000.0;

        storage_load_timer.start();
        // Schema analyze
        for (; row_cursor < num_rows; row_cursor++) {
            // fprintf(stdout, "row_cursor = %ld\n", row_cursor);
            vector<int64_t> cur_tuple_schema;
            for (size_t i = 0; i < num_columns; i++) {
                idx_t target_index = index_cursor + i;//required_key_column_idxs[i];
                idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
                idx_t end_offset = pcsv.indexes[target_index];
                if (start_offset == end_offset) continue; // null data case
                if ((i == num_columns - 1) && (end_offset - start_offset == 1)) continue; // newline case

                auto key_it = key_map.find(std::make_pair(key_names[i], (uint8_t)key_types[i].id()));
                D_ASSERT(key_it != key_map.end());
                int64_t key_id = key_it->second;
                cur_tuple_schema.push_back(key_id);
            }

            // for (size_t i = 0; i < cur_tuple_schema.size(); i++) {
            //     fprintf(stdout, "%ld, ", cur_tuple_schema[i]);
            // }
            // fprintf(stdout, "\n");

            if (final_schema_key_ids.size() == 0) { // first schema
                // fprintf(stdout, "case A\n");
                final_schema_key_ids.push_back(move(cur_tuple_schema));
                merge_count.push_back(0);
                vector<int64_t> tuple_id_list {0};
                tuple_id_list_per_schema.push_back(tuple_id_list);

                #ifdef USE_HASH_TABLE
                    schema_hash_table.insert(final_schema_key_ids[0], 0);
                #endif

            } else {
                vector<int64_t> similar_schema_list;
                vector<int> scores;
                int64_t lowest_score_index;
                GetSimilarSchema(cur_tuple_schema, similar_schema_list, scores, lowest_score_index);

                #ifdef USE_HASH_TABLE
                    int64_t same_schema_index;
                    GetSameSchema(cur_tuple_schema, same_schema_index);
                    if (!isSchemaFound(same_schema_index)) { 
                        final_schema_key_ids.push_back(move(cur_tuple_schema));
                        schema_hash_table.insert(final_schema_key_ids[final_schema_key_ids.size() - 1], final_schema_key_ids.size() - 1); 
                    }
                #endif

                if (similar_schema_list.size() == 0) {
                    // fprintf(stdout, "case A, new schema at %ld\n", final_schema_key_ids.size());
                    // generate new schema
                    final_schema_key_ids.push_back(move(cur_tuple_schema));
                    merge_count.push_back(0);
                    vector<int64_t> tuple_id_list;
                    tuple_id_list.push_back((int64_t)row_cursor - 1);
                    tuple_id_list_per_schema.push_back(tuple_id_list);
                } else {
                    if (scores[lowest_score_index] == 0) {
                        // fprintf(stdout, "case B\n");
                        // same schema
                        D_ASSERT(tuple_id_list_per_schema.size() > similar_schema_list[lowest_score_index]);
                        tuple_id_list_per_schema[similar_schema_list[lowest_score_index]].push_back((int64_t)row_cursor - 1);
                    } else {
                        // similar, but different schema
                        int64_t merge_target = similar_schema_list[lowest_score_index];
                        // fprintf(stdout, "case C, merge_target = %ld\n", merge_target);
                        std::vector<int64_t> merged_schema;
                        std::set_union(cur_tuple_schema.begin(), cur_tuple_schema.end(),
                                       final_schema_key_ids[merge_target].begin(), final_schema_key_ids[merge_target].end(),
                                       std::back_inserter(merged_schema));
                        merged_schema.erase(std::unique(merged_schema.begin(), merged_schema.end()), merged_schema.end());
                        final_schema_key_ids.push_back(move(merged_schema));
                        merge_count.push_back(merge_count[merge_target] + 1);
                        merge_count[merge_target] = -1; // invalid
                        tuple_id_list_per_schema.push_back(move(tuple_id_list_per_schema[merge_target]));
                        tuple_id_list_per_schema.back().push_back((int64_t)row_cursor - 1);
                    }
                }
            }
            index_cursor += num_columns;
        }

        // invalid_count_per_column.resize(final_schema_key_ids.size());

        row_cursor = 1; // rewind

        fprintf(stdout, "Final_schema_key_ids.size() = %ld\n", final_schema_key_ids.size());
        
        // Load
        idx_t current_index = 0;
        for (int64_t i = 0; i < final_schema_key_ids.size(); i++) {
            current_index = 0;
            if (merge_count[i] == -1) {
                D_ASSERT(tuple_id_list_per_schema[i].size() == 0);
                continue;
            }

            invalid_count_per_column.push_back(std::vector<int64_t>(0));
            invalid_count_per_column.back().resize(final_schema_key_ids[i].size(), 0);
            schema_mapping_per_collection.push_back(i);

            vector<LogicalType> cur_types;
            int64_t cur_schema_group_id = node_store.size();
            for (int64_t k = 0; k < final_schema_key_ids[i].size(); k++) {
                cur_types.push_back(key_types[final_schema_key_ids[i][k]]);
                auto it = property_key_to_schema_idx.find(final_schema_key_ids[i][k]);
                if (it == property_key_to_schema_idx.end()) {
                    std::vector<std::pair<int64_t, int64_t>> tmp_vec;
                    tmp_vec.push_back({cur_schema_group_id, k});
                    property_key_to_schema_idx.insert({final_schema_key_ids[i][k], tmp_vec});
                } else {
                    auto &vec = it->second;
                    vec.push_back({cur_schema_group_id, k});
                }
                // fprintf(stdout, "%d, ", (uint8_t)key_types[final_schema_key_ids[i][k]].id());
            }
            // fprintf(stdout, "\n");

            node_store.push_back(ChunkCollection());
            unique_ptr<DataChunk> newChunk = std::make_unique<DataChunk>();
            newChunk->Initialize(cur_types);

            for (int64_t j = 0; j < tuple_id_list_per_schema[i].size(); j++) {
                int64_t rowid = tuple_id_list_per_schema[i][j];
                index_cursor = num_columns * (rowid + 1);
                for (int64_t k = 0; k < final_schema_key_ids[i].size(); k++) {
                    int64_t colid = final_schema_key_ids[i][k];
                    idx_t target_index = index_cursor + colid;
                    idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
                    idx_t end_offset = pcsv.indexes[target_index];
                    if (start_offset == end_offset) { // null data case
                        newChunk->data[k].SetValue(current_index, Value(key_types[colid])); // Set null Value
                        invalid_count_per_column.back()[k]++;
                        continue;
                    }
                    if ((colid == num_columns - 1) && (end_offset - start_offset == 1)) { // newline, null case
                        newChunk->data[k].SetValue(current_index, Value(key_types[colid])); // Set null Value
                        invalid_count_per_column.back()[k]++;
                        continue;
                    }
                    SetFullColumnValueFromCSV(key_types[colid], newChunk, k, current_index, p, start_offset, end_offset);
                }
                current_index++;

                if (current_index % STANDARD_VECTOR_SIZE == 0) {
                    newChunk->SetCardinality(STANDARD_VECTOR_SIZE);
                    node_store.back().Append(move(newChunk));
                    newChunk = std::make_unique<DataChunk>();
                    newChunk->Initialize(cur_types);
                    current_index = 0;
                }
            }
            newChunk->SetCardinality(current_index);
            node_store.back().Append(move(newChunk));
        }
        storage_load_time = storage_load_timer.elapsed().wall / 1000000.0;

        std::cout << "CSV Load Elapsed Time: " << csv_load_time << " ms, Storage Load Elapsed Time: " << storage_load_time << " ms" << std::endl;
    }

    LogicalType StringToLogicalType(std::string &type_name, size_t column_idx) {
        const auto end = m.end();
        auto it = m.find(type_name);
        if (it != end) {
            LogicalType return_type = it->second;
            return return_type;
        } else {
            if (type_name.find("ID") != std::string::npos) {
                // ID Column
                if (true) {
                    auto last_pos = type_name.find_first_of('(');
                    string id_name = type_name.substr(0, last_pos - 1);
                    auto delimiter_pos = id_name.find("_");
                    if (delimiter_pos != std::string::npos) {
                        // Multi key
                        auto key_order = std::stoi(type_name.substr(delimiter_pos + 1, last_pos - delimiter_pos - 1));
                        key_columns_order.push_back(key_order);
                        key_columns.push_back(column_idx);
                    } else {
                        // Single key
                        key_columns.push_back(column_idx);
                    }
                    return LogicalType::UBIGINT;
                } else { // type == GraphComponentType::EDGE
                    // D_ASSERT((src_column == -1) || (dst_column == -1));
                    auto first_pos = type_name.find_first_of('(');
                    auto last_pos = type_name.find_last_of(')');
                    string label_name = type_name.substr(first_pos + 1, last_pos - first_pos - 1);
                    std::cout << type_name << std::endl;
                    if (type_name.find("START_ID") != std::string::npos) {
                        src_key_name = move(label_name);
                        src_columns.push_back(column_idx);
                    } else { // "END_ID"
                        dst_key_name = move(label_name);
                        dst_columns.push_back(column_idx);
                    }
                    // return LogicalType::ID;
                    return LogicalType::UBIGINT;
                }
            } else if (type_name.find("DECIMAL") != std::string::npos) {
                auto first_pos = type_name.find_first_of('(');
                auto comma_pos = type_name.find_first_of(',');
                auto last_pos = type_name.find_last_of(')');
                int width = std::stoi(type_name.substr(first_pos + 1, comma_pos - first_pos - 1));
                int scale = std::stoi(type_name.substr(comma_pos + 1, last_pos - comma_pos - 1));
                return LogicalType::DECIMAL(width, scale);
            } else {
                fprintf(stdout, "%s\n", type_name.c_str());
                throw InvalidInputException("Unsupported Type");
            }
        }
    }

    int64_t GetNewKeyVer() {
        return property_key_id_ver++;
    }

    // Returns index if found, -1 otherwise.
    // For comfortability, we use -1 as invalid index (INVALID_TUPLE_GROUP_ID)
    // After finding, please call if (same_schema_index == INVALID_TUPLE_GROUP_ID) { schema_hash_table.insert(cur_tuple_schema); }
    void GetSameSchema(vector<int64_t> &cur_tuple_schema, int64_t &same_schema_index) {
        schema_hash_table.find(cur_tuple_schema, same_schema_index);
    }

    void GetSimilarSchema(vector<int64_t> &cur_tuple_schema, vector<int64_t> &similar_schema_list, vector<int> &scores, int64_t &lowest_score_index) {
        int64_t lowest_score = max_allow_edit_distance + 1;
        D_ASSERT(final_schema_key_ids.size() == merge_count.size());
        for (int64_t i = 0; i < final_schema_key_ids.size(); i++) {
            if (merge_count[i] == -1) continue; // invalid schema
            if (std::abs((int64_t)final_schema_key_ids[i].size() - (int64_t)cur_tuple_schema.size()) 
                > max_allow_edit_distance) continue;
            
            int score = 0;
            // for (size_t k = 0; k < cur_tuple_schema.size(); k++) {
            //     fprintf(stdout, "%ld, ", cur_tuple_schema[k]);
            // }
            // fprintf(stdout, " vs ");
            // for (size_t k = 0; k < final_schema_key_ids[i].size(); k++) {
            //     fprintf(stdout, "%ld, ", final_schema_key_ids[i][k]);
            // }
            // fprintf(stdout, "\n");
            vector<int64_t> diff_v1_m_v2;
            vector<int64_t> diff_v2_m_v1;
            std::set_difference(cur_tuple_schema.begin(), cur_tuple_schema.end(),
                                final_schema_key_ids[i].begin(), final_schema_key_ids[i].end(),
                                std::inserter(diff_v1_m_v2, diff_v1_m_v2.begin()));
            std::set_difference(final_schema_key_ids[i].begin(), final_schema_key_ids[i].end(),
                                cur_tuple_schema.begin(), cur_tuple_schema.end(),
                                std::inserter(diff_v2_m_v1, diff_v2_m_v1.begin()));
            score = diff_v1_m_v2.size() + diff_v2_m_v1.size();
            if (diff_v1_m_v2.size() == 0) {
                score = 0; // containment
            }
            if (score > 0 && (merge_count[i] >= max_merge_count)) continue;
            if (max_allow_edit_distance == 0 && score == 0) {
                if (diff_v1_m_v2.size() == 0 && diff_v2_m_v1.size() == 0) { // exact match
                    similar_schema_list.clear();
                    scores.clear();
                    similar_schema_list.push_back(i);
                    scores.push_back(score);
                    lowest_score_index = 0;
                    return;
                }
            }
            if (score <= max_allow_edit_distance) {   
                similar_schema_list.push_back(i);
                scores.push_back(score);
                if (score < lowest_score) {
                    lowest_score = score;
                    lowest_score_index = similar_schema_list.size() - 1;
                }
            }
        }
    }

// partial columnar storage
    void FullScanQuery() {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        for (int64_t schemaidx = 0; schemaidx < node_store.size(); schemaidx++) {
            ChunkCollection &cur_chunkcollection = node_store[schemaidx];
            D_ASSERT(cur_chunkcollection.ColumnCount() == invalid_count_per_column[schemaidx].size());
            for (int64_t chunkidx = 0; chunkidx < cur_chunkcollection.ChunkCount(); chunkidx++) {
                DataChunk &cur_chunk = cur_chunkcollection.GetChunk(chunkidx);
                for (int64_t colidx = 0; colidx < cur_chunkcollection.ColumnCount(); colidx++) {
                    if (invalid_count_per_column[schemaidx][colidx] == cur_chunkcollection.Count()) continue; // full null column
                    const Vector &vec = cur_chunk.data[colidx];
                    if (cur_chunkcollection.Types()[colidx] == LogicalType::BIGINT) {
                        int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                        if (invalid_count_per_column[schemaidx][colidx] == 0) {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_int += cur_chunk_vec_ptr[i];
                            }
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_int += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
                    } else if (cur_chunkcollection.Types()[colidx] == LogicalType::UBIGINT) {
                        uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                        if (invalid_count_per_column[schemaidx][colidx] == 0) {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_uint += cur_chunk_vec_ptr[i];
                            }
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_uint += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
                    } else if (cur_chunkcollection.Types()[colidx] == LogicalType::DOUBLE) {
                        double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                        if (invalid_count_per_column[schemaidx][colidx] == 0) {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                sum_double += cur_chunk_vec_ptr[i];
                            }
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    sum_double += cur_chunk_vec_ptr[i];
                                }
                            }
                        }
                    } else if (cur_chunkcollection.Types()[colidx] == LogicalType::VARCHAR) {
                        data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                        if (invalid_count_per_column[schemaidx][colidx] == 0) {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                auto str = ((string_t *)vec_data)[i];
                                idx_t str_size = str.GetSize();
                                const char *str_data = str.GetDataUnsafe();
                                for (int64_t j = 0; j < str_size; j++) {
                                    sum_str += str_data[j];
                                }
                            }
                        } else {
                            for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                if (!FlatVector::IsNull(vec, i)) {
                                    auto str = ((string_t *)vec_data)[i];
                                    idx_t str_size = str.GetSize();
                                    const char *str_data = str.GetDataUnsafe();
                                    for (int64_t j = 0; j < str_size; j++) {
                                        sum_str += str_data[j];
                                    }
                                }
                            }
                        }
                    } else {
                        D_ASSERT(false);
                    }
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void SIMDFullScanQuery(DataChunk &output) {
        D_ASSERT(false); // TODO
    }

    void TargetColScanQuery(int target_col) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        auto it = property_key_to_schema_idx.find(target_col);
        if (it == property_key_to_schema_idx.end()) {
            // do nothing
        } else {
            auto &vec = it->second;
            printf("vec.size = %ld\n", vec.size());
            for (int64_t i = 0; i < vec.size(); i++) {
                int64_t schemaidx = vec[i].first;
                int64_t target_col_idx = vec[i].second;

                ChunkCollection &cur_chunkcollection = node_store[schemaidx];

                for (int64_t chunkidx = 0; chunkidx < cur_chunkcollection.ChunkCount(); chunkidx++) {
                    DataChunk &cur_chunk = cur_chunkcollection.GetChunk(chunkidx);
                    for (int64_t colidx = 0; colidx < cur_chunkcollection.ColumnCount(); colidx++) {
                        if (colidx != target_col_idx) continue;
                        const Vector &vec = cur_chunk.data[colidx];
                        if (key_types[colidx] == LogicalType::BIGINT) {
                            int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                            if (invalid_count_per_column[schemaidx][colidx] == 0) {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    sum_int += cur_chunk_vec_ptr[i];
                                }
                            } else {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    if (!FlatVector::IsNull(vec, i)) {
                                        sum_int += cur_chunk_vec_ptr[i];
                                    }
                                }
                            }
                        } else if (key_types[colidx] == LogicalType::UBIGINT) {
                            uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
                            if (invalid_count_per_column[schemaidx][colidx] == 0) {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    sum_uint += cur_chunk_vec_ptr[i];
                                }
                            } else {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    if (!FlatVector::IsNull(vec, i)) {
                                        sum_uint += cur_chunk_vec_ptr[i];
                                    }
                                }
                            }
                        } else if (key_types[colidx] == LogicalType::DOUBLE) {
                            double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
                            if (invalid_count_per_column[schemaidx][colidx] == 0) {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    sum_double += cur_chunk_vec_ptr[i];
                                }
                            } else {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    if (!FlatVector::IsNull(vec, i)) {
                                        sum_double += cur_chunk_vec_ptr[i];
                                    }
                                }
                            }
                        } else if (key_types[colidx] == LogicalType::VARCHAR) {
                            data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
                            if (invalid_count_per_column[schemaidx][colidx] == 0) {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    auto str = ((string_t *)vec_data)[i];
                                    idx_t str_size = str.GetSize();
                                    const char *str_data = str.GetDataUnsafe();
                                    for (int64_t j = 0; j < str_size; j++) {
                                        sum_str += str_data[j];
                                    }
                                }
                            } else {
                                for (int64_t i = 0; i < cur_chunk.size(); i++) {
                                    if (!FlatVector::IsNull(vec, i)) {
                                        auto str = ((string_t *)vec_data)[i];
                                        idx_t str_size = str.GetSize();
                                        const char *str_data = str.GetDataUnsafe();
                                        for (int64_t j = 0; j < str_size; j++) {
                                            sum_str += str_data[j];
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // for (int64_t schemaidx = 0; schemaidx < node_store.size(); schemaidx++) {
        //     ChunkCollection &cur_chunkcollection = node_store[schemaidx];
        //     std::vector<int64_t> &schema_info = final_schema_key_ids[schema_mapping_per_collection[schemaidx]];
        //     D_ASSERT(cur_chunkcollection.ColumnCount() == invalid_count_per_column[schemaidx].size());

        //     int target_col_idx;
        //     auto it = std::find(schema_info.begin(), schema_info.end(), target_col);
        //     if (it == schema_info.end()) continue;
        //     else target_col_idx = it - schema_info.begin();
        //     for (int64_t chunkidx = 0; chunkidx < cur_chunkcollection.ChunkCount(); chunkidx++) {
        //         DataChunk &cur_chunk = cur_chunkcollection.GetChunk(chunkidx);
        //         for (int64_t colidx = 0; colidx < cur_chunkcollection.ColumnCount(); colidx++) {
        //             if (colidx != target_col_idx) continue;
        //             const Vector &vec = cur_chunk.data[colidx];
        //             if (key_types[colidx] == LogicalType::BIGINT) {
        //                 int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
        //                 if (invalid_count_per_column[schemaidx][colidx] == 0) {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         sum_int += cur_chunk_vec_ptr[i];
        //                     }
        //                 } else {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         if (!FlatVector::IsNull(vec, i)) {
        //                             sum_int += cur_chunk_vec_ptr[i];
        //                         }
        //                     }
        //                 }
        //             } else if (key_types[colidx] == LogicalType::UBIGINT) {
        //                 uint64_t *cur_chunk_vec_ptr = (uint64_t *)cur_chunk.data[colidx].GetData();
        //                 if (invalid_count_per_column[schemaidx][colidx] == 0) {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         sum_uint += cur_chunk_vec_ptr[i];
        //                     }
        //                 } else {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         if (!FlatVector::IsNull(vec, i)) {
        //                             sum_uint += cur_chunk_vec_ptr[i];
        //                         }
        //                     }
        //                 }
        //             } else if (key_types[colidx] == LogicalType::DOUBLE) {
        //                 double *cur_chunk_vec_ptr = (double *)cur_chunk.data[colidx].GetData();
        //                 if (invalid_count_per_column[schemaidx][colidx] == 0) {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         sum_double += cur_chunk_vec_ptr[i];
        //                     }
        //                 } else {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         if (!FlatVector::IsNull(vec, i)) {
        //                             sum_double += cur_chunk_vec_ptr[i];
        //                         }
        //                     }
        //                 }
        //             } else if (key_types[colidx] == LogicalType::VARCHAR) {
        //                 data_ptr_t vec_data = cur_chunk.data[colidx].GetData();
        //                 if (invalid_count_per_column[schemaidx][colidx] == 0) {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         auto str = ((string_t *)vec_data)[i];
        //                         idx_t str_size = str.GetSize();
        //                         const char *str_data = str.GetDataUnsafe();
        //                         for (int64_t j = 0; j < str_size; j++) {
        //                             sum_str += str_data[j];
        //                         }
        //                     }
        //                 } else {
        //                     for (int64_t i = 0; i < cur_chunk.size(); i++) {
        //                         if (!FlatVector::IsNull(vec, i)) {
        //                             auto str = ((string_t *)vec_data)[i];
        //                             idx_t str_size = str.GetSize();
        //                             const char *str_data = str.GetDataUnsafe();
        //                             for (int64_t j = 0; j < str_size; j++) {
        //                                 sum_str += str_data[j];
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    vector<string> key_names;
    string src_key_name;
    string dst_key_name;
    vector<LogicalType> key_types;
    vector<int64_t> key_columns;
    vector<int64_t> key_columns_order;
    vector<int64_t> src_columns;
    vector<int64_t> dst_columns;
    int64_t num_columns;
    int64_t num_rows;
    idx_t row_cursor;
    idx_t index_cursor;
    idx_t index_size;
    std::basic_string_view<uint8_t> p;
    ParsedCSV pcsv;

    int64_t property_key_id_ver;
    unordered_map<key_type_pair, int64_t, boost::hash<key_type_pair>> key_map;
    unordered_map<int64_t, vector<std::pair<int64_t, int64_t>>> property_key_to_schema_idx;

    vector<int64_t> merge_count;
    vector<int64_t> schema_mapping_per_collection;
    vector<vector<int64_t>> tuple_id_list_per_schema;
    vector<vector<int64_t>> final_schema_key_ids;
    vector<vector<LogicalType>> final_schema_types;
    vector<ChunkCollection> node_store;

    vector<vector<int64_t>> invalid_count_per_column;

    SchemaHashTable schema_hash_table;

    // parameter
    int64_t max_allow_edit_distance = 0;
    int64_t max_merge_count = 0;

    unordered_map<string, LogicalType> m {
        {"STRING", LogicalType(LogicalTypeId::VARCHAR)},
        {"STRING[]", LogicalType(LogicalTypeId::VARCHAR)},
        {"INT"   , LogicalType(LogicalTypeId::INTEGER)},
        {"INTEGER"   , LogicalType(LogicalTypeId::INTEGER)},
        {"LONG"  , LogicalType(LogicalTypeId::BIGINT)},
        {"ULONG"  , LogicalType(LogicalTypeId::UBIGINT)},
        {"DATE"  , LogicalType(LogicalTypeId::DATE)},
        {"DECIMAL"  , LogicalType(LogicalTypeId::DECIMAL)},
        {"DOUBLE"   , LogicalType(LogicalTypeId::DOUBLE)}
    };
};

class Neo4JPropertyStore {
public:
    Neo4JPropertyStore() {}
    ~Neo4JPropertyStore() {}

    Neo4JPropertyStore(uint64_t num_tuples, const char *csv_file_path) {
        fprintf(stdout, "sizeof Neo4JNode = %ld\n", sizeof(Neo4JNode));
        property_key_id_ver = 0;
        num_data_per_type.resize(13, 0);

        // node_store = new uint8_t[num_tuples * NEO4J_NODE_RECORD_SIZE];

        std::cout << "Load Neo4JPropertyStore Start!!" << std::endl;
        LoadCSVFile(csv_file_path);
        std::cout << "Load Neo4JPropertyStore Done!!" << std::endl;
    }

    void LoadCSVFile(const char *csv_file_path) {
        boost::timer::cpu_timer csv_load_timer;
        boost::timer::cpu_timer storage_load_timer;
        double csv_load_time, storage_load_time;

        csv_load_timer.start();
        // Initialize CSV Reader & iterator
        try {
            p = get_corpus(csv_file_path, CSV_PADDING);
        } catch (const std::exception &e) { // caught by reference to base
            throw InvalidInputException("Could not load the file");
        }

        // Read only header.. TODO how to read only few lines or just a line?
        csv::CSVFormat csv_form;
        csv_form.delimiter('|')
                .header_row(0);
        csv::CSVReader *reader = new csv::CSVReader(csv_file_path, csv_form);

        // Parse CSV File
        // std::cout << "File: " << csv_file_path << ", string size: " << p.size() << std::endl;
        pcsv.indexes = new (std::nothrow) uint32_t[p.size()]; // can't have more indexes than we have data
        if(pcsv.indexes == nullptr) {
            throw InvalidInputException("You are running out of memory.");
        }
        find_indexes(p.data(), p.size(), pcsv);

        // Parse header
        vector<string> col_names = move(reader->get_col_names());
        num_columns = col_names.size();
        num_rows = pcsv.n_indexes / num_columns;
        fprintf(stdout, "n_indexes = %ld, num_columns = %ld, num_rows = %ld\n", pcsv.n_indexes, num_columns, num_rows);
        D_ASSERT((pcsv.n_indexes % num_columns == 0) || (pcsv.n_indexes % num_columns == num_columns - 1)); // no newline at the end of file
        for (size_t i = 0; i < col_names.size(); i++) {
            // Assume each element in the header column is of format 'key:type'
            std::string key_and_type = col_names[i]; 
            std::cout << "\t" << key_and_type << std::endl;
            size_t delim_pos = key_and_type.find(':');
            if (delim_pos == std::string::npos) throw InvalidInputException("D");
            std::string key = key_and_type.substr(0, delim_pos);
            if (key == "") {
                // special case
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                if (type_name.find("START_ID") != std::string::npos) {
                    // key_names.push_back(src_key_name + "_src_" + std::to_string(src_columns.size()));
                    key_names.push_back("_sid");
                } else {
                    // key_names.push_back(dst_key_name + "_dst_" + std::to_string(dst_columns.size()));
                    key_names.push_back("_tid");
                }
                key_types.push_back(move(type));
            } else {
                std::string type_name = key_and_type.substr(delim_pos + 1);
                LogicalType type = move(StringToLogicalType(type_name, i));
                auto key_it = key_map.find(key);
                if (key_it == key_map.end()) {
                    int64_t new_key_id = GetNewKeyVer();
                    key_map.insert(std::make_pair(key, new_key_id));
                    id_to_key_map.insert(std::make_pair(new_key_id, key));
                }
                key_names.push_back(move(key));
                key_types.push_back(move(type));
            }
        }

        // Initialize Cursor
        row_cursor = 1; // After the header
        index_cursor = num_columns;
        delete reader;

        csv_load_time = csv_load_timer.elapsed().wall / 1000000.0;

        storage_load_timer.start();
        // Load
        idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
        bool has_prev_prop;
        uint64_t prev_position;
        for (; row_cursor < num_rows; row_cursor++) {
        // for (; row_cursor < 10; row_cursor++) {
            bool find_first_property = false;
            has_prev_prop = false;
            uint32_t first_property_id;
			for (size_t i = 0; i < num_columns; i++) {
                idx_t target_index = index_cursor + i;//required_key_column_idxs[i];
                idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
                idx_t end_offset = pcsv.indexes[target_index];
                if (start_offset == end_offset) continue; // null data case
                if ((i == num_columns - 1) && (end_offset - start_offset == 1)) continue; // newline case
                // std::cout << "start_offset: " << start_offset << ", end_offset: " << end_offset << std::endl;
                if (!find_first_property) {
                    D_ASSERT(property_store.size() < std::numeric_limits<uint32_t>::max());
                    first_property_id = (uint32_t)property_store.size();
                    find_first_property = true;
                }
                auto key_it = key_map.find(key_names[i]);
                D_ASSERT(key_it != key_map.end());
                int64_t key_id = key_it->second;
                SetNeo4JPropertyFromCSV(key_types[i], key_id, property_store, i, current_index, p, start_offset, end_offset, num_data_per_type);
                if (!has_prev_prop) {
                    has_prev_prop = true;
                    prev_position = property_store.size() - 1;
                } else {
                    property_store[prev_position].next = property_store.size() - 1;
                    property_store.back().prev = prev_position;
                    property_store.back().next = std::numeric_limits<uint32_t>::max();
                    prev_position = property_store.size() - 1;
                }
                // fprintf(stdout, "New property at %ld col %ld, [%016lX] [%016lX] [%016lX] [%016lX]\n", property_store.size() - 1, i, property_store.back().b1, property_store.back().b2, property_store.back().b3, property_store.back().b4);
            }
            current_index++;
            index_cursor += num_columns;

            // std::cout << "node id: " << node_store.size() << ", first_prop_id: " << first_property_id << std::endl;
            node_store.push_back(Neo4JNode(1, 0, first_property_id, 0, 0));
		}
        storage_load_time = storage_load_timer.elapsed().wall / 1000000.0;

        for (int i = 0; i < 13; i++) {
            fprintf(stdout, "Type %d: %ld, ", i, num_data_per_type[i]);
        }
        fprintf(stdout, "\n");

        std::cout << "CSV Load Elapsed Time: " << csv_load_time << " ms, Storage Load Elapsed Time: " << storage_load_time << " ms" << std::endl;
    }

    LogicalType StringToLogicalType(std::string &type_name, size_t column_idx) {
        const auto end = m.end();
        auto it = m.find(type_name);
        if (it != end) {
            LogicalType return_type = it->second;
            return return_type;
        } else {
            if (type_name.find("ID") != std::string::npos) {
                // ID Column
                if (true) {
                    auto last_pos = type_name.find_first_of('(');
                    string id_name = type_name.substr(0, last_pos - 1);
                    auto delimiter_pos = id_name.find("_");
                    if (delimiter_pos != std::string::npos) {
                        // Multi key
                        auto key_order = std::stoi(type_name.substr(delimiter_pos + 1, last_pos - delimiter_pos - 1));
                        key_columns_order.push_back(key_order);
                        key_columns.push_back(column_idx);
                    } else {
                        // Single key
                        key_columns.push_back(column_idx);
                    }
                    return LogicalType::UBIGINT;
                } else { // type == GraphComponentType::EDGE
                    // D_ASSERT((src_column == -1) || (dst_column == -1));
                    auto first_pos = type_name.find_first_of('(');
                    auto last_pos = type_name.find_last_of(')');
                    string label_name = type_name.substr(first_pos + 1, last_pos - first_pos - 1);
                    std::cout << type_name << std::endl;
                    if (type_name.find("START_ID") != std::string::npos) {
                        src_key_name = move(label_name);
                        src_columns.push_back(column_idx);
                    } else { // "END_ID"
                        dst_key_name = move(label_name);
                        dst_columns.push_back(column_idx);
                    }
                    // return LogicalType::ID;
                    return LogicalType::UBIGINT;
                }
            } else if (type_name.find("DECIMAL") != std::string::npos) {
                auto first_pos = type_name.find_first_of('(');
                auto comma_pos = type_name.find_first_of(',');
                auto last_pos = type_name.find_last_of(')');
                int width = std::stoi(type_name.substr(first_pos + 1, comma_pos - first_pos - 1));
                int scale = std::stoi(type_name.substr(comma_pos + 1, last_pos - comma_pos - 1));
                return LogicalType::DECIMAL(width, scale);
            } else {
                fprintf(stdout, "%s\n", type_name.c_str());
                throw InvalidInputException("Unsupported Type");
            }
        }
    }

    int64_t GetNewKeyVer() {
        return property_key_id_ver++;
    }

// Neo4J
    void FullScanQuery() {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0;
        uint8_t sum_str = 0;
        uint8_t sum_prop_key = 0;

        for (int64_t nodeidx = 0; nodeidx <= node_store.size(); nodeidx++) {
            uint32_t nextPropId = node_store[nodeidx].nextPropId;

            Neo4JProperty prop = property_store[nextPropId];
            int key = prop.b1 & 0xFFFFFF;
            auto key_it = id_to_key_map.find(key);
            assert(key_it != id_to_key_map.end());
            string &key_str = key_it->second;
            for (int i = 0; i < key_str.length(); i++) {
                sum_prop_key += key_str.c_str()[i];
            }

            uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
            Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

            if (n_tid == Neo4JTypeId::LONG) {
                sum_int += (int64_t) prop.b2;
            } else if (n_tid == Neo4JTypeId::DOUBLE) {
                sum_double += (double) prop.b2;
            } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                int str_len = (prop.b1 & 0x7E00000000) >> 33;
                uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                for (int i = 0; i < str_len; i++) {
                    sum_str += str_ptr[i];
                }
            }
            while (prop.next != std::numeric_limits<uint32_t>::max()) {
                prop = property_store[prop.next];
                key = prop.b1 & 0xFFFFFF;
                auto key_it = id_to_key_map.find(key);
                assert(key_it != id_to_key_map.end());
                string &key_str = key_it->second;
                for (int i = 0; i < key_str.length(); i++) {
                    sum_prop_key += key_str.c_str()[i];
                }
                uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
                Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

                if (n_tid == Neo4JTypeId::LONG) {
                    sum_int += (int64_t) prop.b2;
                } else if (n_tid == Neo4JTypeId::DOUBLE) {
                    sum_double += (double) prop.b2;
                } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                    int str_len = (prop.b1 & 0x7E00000000) >> 33;
                    uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                    for (int i = 0; i < str_len; i++) {
                        sum_str += str_ptr[i];
                    }
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d, sum_prop_key = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str, sum_prop_key);
    }

    void TargetColScanQuery(int target_col) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0;
        uint8_t sum_str = 0;

        for (int64_t nodeidx = 0; nodeidx <= node_store.size(); nodeidx++) {
            uint32_t nextPropId = node_store[nodeidx].nextPropId;

            Neo4JProperty prop = property_store[nextPropId];
            int key = prop.b1 & 0xFFFFFF;
            if (key == target_col) {
                uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
                Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

                if (n_tid == Neo4JTypeId::LONG) {
                    sum_int += (int64_t) prop.b2;
                } else if (n_tid == Neo4JTypeId::DOUBLE) {
                    sum_double += (double) prop.b2;
                } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                    int str_len = (prop.b1 & 0x7E00000000) >> 33;
                    uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                    for (int i = 0; i < str_len; i++) {
                        sum_str += str_ptr[i];
                    }
                }
                continue;
            }
            while (prop.next != std::numeric_limits<uint32_t>::max()) {
                prop = property_store[prop.next];
                key = prop.b1 & 0xFFFFFF;
                if (key == target_col) {
                    uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
                    Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

                    if (n_tid == Neo4JTypeId::LONG) {
                        sum_int += (int64_t) prop.b2;
                    } else if (n_tid == Neo4JTypeId::DOUBLE) {
                        sum_double += (double) prop.b2;
                    } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                        int str_len = (prop.b1 & 0x7E00000000) >> 33;
                        uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                        for (int i = 0; i < str_len; i++) {
                            sum_str += str_ptr[i];
                        }
                    }
                    break;
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void TargetColRangeQuery(int target_col, int begin, int end) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0;
        uint8_t sum_str = 0;

        for (int64_t nodeidx = 0; nodeidx <= node_store.size(); nodeidx++) {
            uint32_t nextPropId = node_store[nodeidx].nextPropId;

            Neo4JProperty prop = property_store[nextPropId];
            int key = prop.b1 & 0xFFFFFF;
            if (key == target_col) {
                uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
                Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

                if (n_tid == Neo4JTypeId::LONG) {
                    auto target_val = (int64_t) prop.b2;
                    if (target_val >= begin && target_val <= end) {
                        sum_int += target_val;
                    }
                } else if (n_tid == Neo4JTypeId::DOUBLE) {
                    auto target_val = (double) prop.b2;
                    if (target_val >= begin && target_val <= end) {
                        sum_double += target_val;
                    }
                } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                    int str_len = (prop.b1 & 0x7E00000000) >> 33;
                    uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                    for (int i = 0; i < str_len; i++) {
                        sum_str += str_ptr[i];
                    }
                }
                continue;
            }
            while (prop.next != std::numeric_limits<uint32_t>::max()) {
                prop = property_store[prop.next];
                key = prop.b1 & 0xFFFFFF;
                if (key == target_col) {
                    uint64_t n_tid_64bit = (prop.b1 & 0xF000000) >> 24;
                    Neo4JTypeId n_tid = (Neo4JTypeId) n_tid_64bit;

                    if (n_tid == Neo4JTypeId::LONG) {
                        auto target_val = (int64_t) prop.b2;
                        if (target_val >= begin && target_val <= end) {
                            sum_int += target_val;
                        }
                    } else if (n_tid == Neo4JTypeId::DOUBLE) {
                        sum_double += (double) prop.b2;
                    } else if (n_tid == Neo4JTypeId::SHORT_STRING) {
                        int str_len = (prop.b1 & 0x7E00000000) >> 33;
                        uint8_t *str_ptr = ((uint8_t *)&prop.b1) + 5;
                        for (int i = 0; i < str_len; i++) {
                            sum_str += str_ptr[i];
                        }
                    }
                    break;
                }
            }
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

private:
    vector<Neo4JNode> node_store;
    vector<Neo4JProperty> property_store;
    // uint8_t *node_store;
    // uint8_t *property_store;

    vector<string> key_names;
    string src_key_name;
    string dst_key_name;
    vector<LogicalType> key_types;
    vector<int64_t> key_columns;
    vector<int64_t> key_columns_order;
    vector<int64_t> src_columns;
    vector<int64_t> dst_columns;
    int64_t num_columns;
    int64_t num_rows;
    idx_t row_cursor;
    idx_t index_cursor;
    idx_t index_size;
    std::basic_string_view<uint8_t> p;
    ParsedCSV pcsv;
    int64_t property_key_id_ver;
    unordered_map<string, int64_t> key_map;
    unordered_map<int64_t, string> id_to_key_map;
    vector<int64_t> num_data_per_type;

    unordered_map<string, LogicalType> m {
        {"STRING", LogicalType(LogicalTypeId::VARCHAR)},
        {"STRING[]", LogicalType(LogicalTypeId::VARCHAR)},
        {"INT"   , LogicalType(LogicalTypeId::INTEGER)},
        {"INTEGER"   , LogicalType(LogicalTypeId::INTEGER)},
        {"LONG"  , LogicalType(LogicalTypeId::BIGINT)},
        {"ULONG"  , LogicalType(LogicalTypeId::UBIGINT)},
        {"DATE"  , LogicalType(LogicalTypeId::DATE)},
        {"DECIMAL"  , LogicalType(LogicalTypeId::DECIMAL)},
        {"DOUBLE"   , LogicalType(LogicalTypeId::DOUBLE)}
    };
};

} //namespace duckdb
