#pragma once

#include <random>

#include <folly/init/Init.h>
// #include "velox/connectors/tpch/TpchConnector.h"
// #include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
// #include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/SwitchExpr.h"

#include "velox/dwio/common/DecoderUtil.h"
#include <folly/Random.h>
#include "velox/common/base/Nulls.h"
#include "velox/type/Filter.h"

#include "common/common.hpp"
#include "common/types/chunk_collection.hpp"
// simdcsv
#include "third_party/csv-parser/csv.hpp"
#include "graph_simdcsv_parser.hpp"

#include "common/types/validity_mask.hpp"

using namespace facebook::velox;
// using namespace facebook::velox::test;
// using namespace facebook::velox::exec::test;

namespace facebook::velox::dwio::common {
// Excerpt from LazyVector.h.
struct NoHook {
  void addValue(
      vector_size_t /*row*/,
      const void* FOLLY_NULLABLE /*value*/) {}
  void addValues(
      const int32_t* /*rows*/,
      const void* /*values*/,
      int32_t /*size*/,
      uint8_t /*valueWidth*/) {}
};

} // namespace facebook::velox::dwio::common

namespace duckdb {

template <typename T>
struct FilterFunctionTslee {
  FOLLY_ALWAYS_INLINE bool call(int32_t& out, const int32_t& a) {
    if (a >= 40 && a <= 1000) {
      out = a;
      return true;
    } else {
      return false;
    }
  }
};

class VeloxPartialColumnarFormatStore : public facebook::velox::test::VectorTestBase {

typedef std::pair<string, uint8_t> key_type_pair;

public:
    VeloxPartialColumnarFormatStore() {
        // Register Presto scalar functions.
        functions::prestosql::registerAllScalarFunctions();

        // Register Presto aggregate functions.
        aggregate::prestosql::registerAllAggregateFunctions();

        // Setup random generator
        rng_.seed(1);
    }
    ~VeloxPartialColumnarFormatStore() {}

    VeloxPartialColumnarFormatStore(uint64_t num_tuples, const char *csv_file_path, int64_t max_allow_edit_distance_, int64_t max_merge_count_) :
        num_tuples(num_tuples), max_allow_edit_distance(max_allow_edit_distance_), max_merge_count(max_merge_count_), schema_hash_table(HASH_TABLE_SIZE) {
        // Register Presto scalar functions.
        functions::prestosql::registerAllScalarFunctions();

        // Register Presto aggregate functions.
        aggregate::prestosql::registerAllAggregateFunctions();

        // Setup random generator
        rng_.seed(1);

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
            for (int64_t k = 0; k < final_schema_key_ids[i].size(); k++) {
                cur_types.push_back(key_types[final_schema_key_ids[i][k]]);
                fprintf(stdout, "%d, ", (uint8_t)key_types[final_schema_key_ids[i][k]].id());
            }
            fprintf(stdout, "\n");

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

        for (int64_t schemaidx = 0; schemaidx < node_store.size(); schemaidx++) {
            ChunkCollection &cur_chunkcollection = node_store[schemaidx];
            std::vector<int64_t> &schema_info = final_schema_key_ids[schema_mapping_per_collection[schemaidx]];
            D_ASSERT(cur_chunkcollection.ColumnCount() == invalid_count_per_column[schemaidx].size());

            int target_col_idx;
            auto it = std::find(schema_info.begin(), schema_info.end(), target_col);
            if (it == schema_info.end()) continue;
            else target_col_idx = it - schema_info.begin();
            for (int64_t chunkidx = 0; chunkidx < cur_chunkcollection.ChunkCount(); chunkidx++) {
                DataChunk &cur_chunk = cur_chunkcollection.GetChunk(chunkidx);
                int64_t colidx = target_col_idx;
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
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void TargetColRangeQuery(int target_col, int32_t begin, int32_t end) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(STANDARD_VECTOR_SIZE);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(STANDARD_VECTOR_SIZE);
        std::iota(rows.begin(), rows.end(), 0);
        // inner & outer
        raw_vector<int32_t> innerVector_;
        raw_vector<int32_t> outerVector_;

        bool hasFilter = true;
        constexpr bool filterOnly = true;
        bool hasHook = false;
        auto numRows = num_tuples;
        auto numNonNull = numRows;
        const int32_t* rows_ = rows.data();
        auto rowsAsRange = folly::Range<const int32_t*>(rows_, STANDARD_VECTOR_SIZE);
        constexpr bool is_dense = true;

        raw_vector<int32_t>* innerVector = nullptr;
        auto outerVector = &outerVector_;
        int32_t numValues = 0;
        facebook::velox::dwio::common::NoHook noHook;

        for (int64_t schemaidx = 0; schemaidx < node_store.size(); schemaidx++) {
            ChunkCollection &cur_chunkcollection = node_store[schemaidx];
            std::vector<int64_t> &schema_info = final_schema_key_ids[schema_mapping_per_collection[schemaidx]];
            D_ASSERT(cur_chunkcollection.ColumnCount() == invalid_count_per_column[schemaidx].size());

            int target_col_idx;
            auto it = std::find(schema_info.begin(), schema_info.end(), target_col);
            if (it == schema_info.end()) continue;
            else target_col_idx = it - schema_info.begin();
            for (int64_t chunkidx = 0; chunkidx < cur_chunkcollection.ChunkCount(); chunkidx++) {
                DataChunk &cur_chunk = cur_chunkcollection.GetChunk(chunkidx);
                auto colidx = target_col_idx;
                const Vector &vec = cur_chunk.data[colidx];
                uint64_t *nulls = (uint64_t *)FlatVector::Validity(cur_chunk.data[colidx]).GetData();
                numValues = 0;
                if (key_types[colidx] == LogicalType::BIGINT) {
                    int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                    if (invalid_count_per_column[schemaidx][colidx] == 0 || !nulls) {
                        idx_t num_tuples_in_curchunk = cur_chunk.size();
                        if (num_tuples_in_curchunk == 0) continue;
                        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
                        dwio::common::SeekableArrayInputStream is((const char *)cur_chunk_vec_ptr, num_tuples_in_curchunk * sizeof(int64_t));
                        const char *bufferStart = (const char *)cur_chunk_vec_ptr;
                        const char *bufferEnd = (const char *)(cur_chunk_vec_ptr + num_tuples_in_curchunk);
                        dwio::common::fixedWidthScan<int64_t, filterOnly, false>(
                            rowsAsRange,
                            nullptr, // scatterRows
                            nullptr, // data,
                            hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                            numValues,
                            is,
                            bufferStart,
                            bufferEnd,
                            *filter,
                            noHook);

                        for (int64_t i = 0; i < numValues; i++) {
                            sum_int += cur_chunk_vec_ptr[hits[i]];
                        }
                        // for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        //     sum_int += cur_chunk_vec_ptr[i];
                        // }
                    } else {
                        idx_t num_tuples_in_curchunk = cur_chunk.size();
                        if (num_tuples_in_curchunk == 0) continue;
                        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
                        dwio::common::nonNullRowsFromDense(nulls, num_tuples_in_curchunk, *outerVector);
                        dwio::common::SeekableArrayInputStream is((const char *)cur_chunk_vec_ptr, num_tuples_in_curchunk * sizeof(int64_t));
                        const char *bufferStart = (const char *)cur_chunk_vec_ptr;
                        const char *bufferEnd = (const char *)(cur_chunk_vec_ptr + num_tuples_in_curchunk);
                        auto dataRows = folly::Range<const int32_t*>(outerVector->data(), outerVector->size());
                        dwio::common::fixedWidthScan<int64_t, filterOnly, false>(
                            dataRows,
                            nullptr, // scatterRows
                            nullptr, // data,
                            hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                            numValues,
                            is,
                            bufferStart,
                            bufferEnd,
                            *filter,
                            noHook);
                        // std::cout << "num_tuples_in_curchunk: " << num_tuples_in_curchunk << ", numValues: " << numValues << std::endl;

                        for (int64_t i = 0; i < numValues; i++) {
                            sum_int += cur_chunk_vec_ptr[hits[i]];
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
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    folly::Random::DefaultGenerator rng_;

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

    int64_t property_key_id_ver;
    unordered_map<key_type_pair, int64_t, boost::hash<key_type_pair>> key_map;

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

class VeloxPropertyStore : public facebook::velox::test::VectorTestBase {
public:
    VeloxPropertyStore() {
        // Register Presto scalar functions.
        functions::prestosql::registerAllScalarFunctions();

        // Register Presto aggregate functions.
        aggregate::prestosql::registerAllAggregateFunctions();

        // Setup random generator
        rng_.seed(1);
    }

    VeloxPropertyStore(uint64_t num_tuples, const char *csv_file_path) : num_tuples(num_tuples) {
        // Register Presto scalar functions.
        functions::prestosql::registerAllScalarFunctions();

        // Register Presto aggregate functions.
        aggregate::prestosql::registerAllAggregateFunctions();

        // Setup random generator
        rng_.seed(1);

        std::cout << "Load FullColumnarFormatStore(Velox) Start!!" << std::endl;
        LoadCSVFile(csv_file_path);
        std::cout << "Load FullColumnarFormatStore(Velox) Done!!" << std::endl;
    }

    ~VeloxPropertyStore() {}

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
                // std::cout << "row_cursor: " << row_cursor << ", start: " << start_offset << ", end: " << end_offset << std::endl;
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
    void FullScanQuery(DataChunk &output) {
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
                    if (invalid_count_per_column[colidx] == 0) {
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
        }
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void TargetColScanQuery(int target_col) {
        DataChunk output;
        output.Initialize(key_types);
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        // inner & outer
        raw_vector<int32_t> innerVector_;
        raw_vector<int32_t> outerVector_;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(STANDARD_VECTOR_SIZE);
        raw_vector<int32_t>* innerVector = nullptr;
        auto outerVector = &outerVector_;
        int32_t numValues = 0;
        facebook::velox::dwio::common::NoHook noHook;

        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            auto colidx = target_col;
            if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
            const Vector &vec = cur_chunk.data[colidx];
            uint64_t *nulls = (uint64_t *)FlatVector::Validity(cur_chunk.data[colidx]).GetData();
            numValues = 0;
            if (key_types[colidx] == LogicalType::BIGINT) {
                int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0 || !nulls) {
                    for (int64_t i = 0; i < cur_chunk.size(); i++) {
                        sum_int += cur_chunk_vec_ptr[i];
                    }
                } else {
                    // idx_t num_tuples_in_curchunk = cur_chunk.size();
                    // if (num_tuples_in_curchunk == 0) continue;
                    // auto filter = std::make_unique<common::AlwaysTrue>();
                    // dwio::common::nonNullRowsFromDense(nulls, num_tuples_in_curchunk, *outerVector);
                    // dwio::common::SeekableArrayInputStream is((const char *)cur_chunk_vec_ptr, num_tuples_in_curchunk * sizeof(int64_t));
                    // const char *bufferStart = (const char *)cur_chunk_vec_ptr;
                    // const char *bufferEnd = (const char *)(cur_chunk_vec_ptr + num_tuples_in_curchunk);
                    // auto dataRows = folly::Range<const int32_t*>(outerVector->data(), outerVector->size());
                    // dwio::common::fixedWidthScan<int64_t, filterOnly, false>(
                    //     dataRows,
                    //     nullptr, // scatterRows
                    //     nullptr, // data,
                    //     hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                    //     numValues,
                    //     is,
                    //     bufferStart,
                    //     bufferEnd,
                    //     *filter,
                    //     noHook);

                    // for (int64_t i = 0; i < numValues; i++) {
                    //     sum_int += cur_chunk_vec_ptr[hits[i]];
                    // }
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
        fprintf(stdout, "Total = %d, sum_int = %ld, sum_uint = %ld, sum_double = %.3f, sum_str = %d\n",
            total, sum_int, sum_uint, sum_double, sum_str);
    }

    void TargetColRangeQuery(int target_col, int32_t begin, int32_t end) {
        int total = 0;
        int64_t sum_int = 0;
        uint64_t sum_uint = 0;
        double sum_double = 0.0;
        uint8_t sum_str = 0;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(STANDARD_VECTOR_SIZE);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(STANDARD_VECTOR_SIZE);
        std::iota(rows.begin(), rows.end(), 0);
        // inner & outer
        raw_vector<int32_t> innerVector_;
        raw_vector<int32_t> outerVector_;

        bool hasFilter = true;
        constexpr bool filterOnly = true;
        bool hasHook = false;
        auto numRows = num_tuples;
        auto numNonNull = numRows;
        const int32_t* rows_ = rows.data();
        auto rowsAsRange = folly::Range<const int32_t*>(rows_, STANDARD_VECTOR_SIZE);
        constexpr bool is_dense = true;

        raw_vector<int32_t>* innerVector = nullptr;
        auto outerVector = &outerVector_;
        int32_t numValues = 0;
        facebook::velox::dwio::common::NoHook noHook;

        for (int64_t chunkidx = 0; chunkidx < node_store.ChunkCount(); chunkidx++) {
            DataChunk &cur_chunk = node_store.GetChunk(chunkidx);
            int64_t colidx = target_col;
            if (invalid_count_per_column[colidx] == num_tuples) continue; // full null column
            const Vector &vec = cur_chunk.data[colidx];
            uint64_t *nulls = (uint64_t *)FlatVector::Validity(cur_chunk.data[colidx]).GetData();
            numValues = 0;
            if (key_types[colidx] == LogicalType::BIGINT) {
                int64_t *cur_chunk_vec_ptr = (int64_t *)cur_chunk.data[colidx].GetData();
                if (invalid_count_per_column[colidx] == 0 || !nulls) {
                    idx_t num_tuples_in_curchunk = cur_chunk.size();
                    if (num_tuples_in_curchunk == 0) continue;
                    auto filter = std::make_unique<common::BigintRange>(begin, end, false);
                    dwio::common::SeekableArrayInputStream is((const char *)cur_chunk_vec_ptr, num_tuples_in_curchunk * sizeof(int64_t));
                    const char *bufferStart = (const char *)cur_chunk_vec_ptr;
                    const char *bufferEnd = (const char *)(cur_chunk_vec_ptr + num_tuples_in_curchunk);
                    dwio::common::fixedWidthScan<int64_t, filterOnly, false>(
                        rowsAsRange,
                        nullptr, // scatterRows
                        nullptr, // data,
                        hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                        numValues,
                        is,
                        bufferStart,
                        bufferEnd,
                        *filter,
                        noHook);
                    // std::cout << "num_tuples_in_curchunk: " << num_tuples_in_curchunk << ", numValues: " << numValues << std::endl;

                    for (int64_t i = 0; i < numValues; i++) {
                        sum_int += cur_chunk_vec_ptr[hits[i]];
                    }
                } else {
                    idx_t num_tuples_in_curchunk = cur_chunk.size();
                    if (num_tuples_in_curchunk == 0) continue;
                    auto filter = std::make_unique<common::BigintRange>(begin, end, false);
                    dwio::common::nonNullRowsFromDense(nulls, num_tuples_in_curchunk, *outerVector);
                    dwio::common::SeekableArrayInputStream is((const char *)cur_chunk_vec_ptr, num_tuples_in_curchunk * sizeof(int64_t));
                    const char *bufferStart = (const char *)cur_chunk_vec_ptr;
                    const char *bufferEnd = (const char *)(cur_chunk_vec_ptr + num_tuples_in_curchunk);
                    auto dataRows = folly::Range<const int32_t*>(outerVector->data(), outerVector->size());
                    dwio::common::fixedWidthScan<int64_t, filterOnly, false>(
                        dataRows,
                        nullptr, // scatterRows
                        nullptr, // data,
                        hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                        numValues,
                        is,
                        bufferStart,
                        bufferEnd,
                        *filter,
                        noHook);
                    // std::cout << "num_tuples_in_curchunk: " << num_tuples_in_curchunk << ", numValues: " << numValues << std::endl;

                    for (int64_t i = 0; i < numValues; i++) {
                        sum_int += cur_chunk_vec_ptr[hits[i]];
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

    void randomBits(std::vector<uint64_t>& bits, int32_t onesPer1000) {
        for (auto i = 0; i < bits.size() * 64; ++i) {
            if (folly::Random::rand32(rng_) % 1000 < onesPer1000) {
                bits::setBit(bits.data(), i);
            }
        }
    }

    void randomBits(uint64_t *bits, uint64_t size_in_bits, int32_t onesPer1000) {
        for (auto i = 0; i < size_in_bits; ++i) {
            if (folly::Random::rand32(rng_) % 1000 < onesPer1000) {
                bits::setBit(bits, i);
            } else {
                bits::clearBit(bits, i);
            }
        }
    }

    void randomRows(
      int32_t numRows,
      int32_t rowsPer1000,
      raw_vector<int32_t>& result) {
        for (auto i = 0; i < numRows; ++i) {
            if (folly::Random::rand32(rng_) % 1000 < rowsPer1000) {
                result.push_back(i);
            }
        }
    }

    template <bool isFilter, bool outputNulls>
    bool nonNullRowsFromSparseReference(
        const uint64_t* nulls,
        RowSet rows,
        raw_vector<int32_t>& innerRows,
        raw_vector<int32_t>& outerRows,
        uint64_t* resultNulls,
        int32_t& tailSkip) {
        bool anyNull = false;
        auto numIn = rows.size();
        innerRows.resize(numIn);
        outerRows.resize(numIn);
        int32_t lastRow = -1;
        int32_t numNulls = 0;
        int32_t numInner = 0;
        int32_t lastNonNull = -1;
        for (auto i = 0; i < numIn; ++i) {
            auto row = rows[i];
            if (row > lastRow + 1) {
                numNulls += bits::countNulls(nulls, lastRow + 1, row);
            }
            if (bits::isBitNull(nulls, row)) {
                ++numNulls;
                lastRow = row;
                if (!isFilter && outputNulls) {
                    bits::setNull(resultNulls, i);
                    anyNull = true;
                }
            } else {
                innerRows[numInner] = row - numNulls;
                outerRows[numInner++] = isFilter ? row : i;
                lastNonNull = row;
                lastRow = row;
            }
        }
        innerRows.resize(numInner);
        outerRows.resize(numInner);
        tailSkip = bits::countBits(nulls, lastNonNull + 1, lastRow);
        return anyNull;
    }

    // Maps 'rows' where the row falls on a non-null in 'nulls' to an
    // index in non-null rows. This uses both a reference implementation
    // and the SIMDized fast path and checks consistent results.
    template <bool isFilter, bool outputNulls>
    void testNonNullFromSparse(uint64_t* nulls, RowSet rows) {
        raw_vector<int32_t> referenceInner;
        raw_vector<int32_t> referenceOuter;
        std::vector<uint64_t> referenceNulls(bits::nwords(rows.size()), ~0ULL);
        int32_t referenceSkip;
        auto referenceAnyNull =
            nonNullRowsFromSparseReference<isFilter, outputNulls>(
                nulls,
                rows,
                referenceInner,
                referenceOuter,
                referenceNulls.data(),
                referenceSkip);
        // raw_vector<int32_t> testInner;
        // raw_vector<int32_t> testOuter;
        // std::vector<uint64_t> testNulls(bits::nwords(rows.size()), ~0ULL);
        // int32_t testSkip;
        // auto testAnyNull = nonNullRowsFromSparse<isFilter, outputNulls>(
        //     nulls, rows, testInner, testOuter, testNulls.data(), testSkip);

        // EXPECT_EQ(testAnyNull, referenceAnyNull);
        // EXPECT_EQ(testSkip, referenceSkip);
        // for (auto i = 0; i < testInner.size() && i < testOuter.size(); ++i) {
        // EXPECT_EQ(testInner[i], referenceInner[i]);
        // EXPECT_EQ(testOuter[i], referenceOuter[i]);
        // }
        // EXPECT_EQ(testInner.size(), referenceInner.size());
        // EXPECT_EQ(testOuter.size(), referenceOuter.size());

        // if (outputNulls) {
        //     for (auto i = 0; i < rows.size(); ++i) {
        //         EXPECT_EQ(
        //             bits::isBitSet(testNulls.data(), i),
        //             bits::isBitSet(referenceNulls.data(), i));
        //     }
        // }
    }

    void testNonNullFromSparseCases(uint64_t* nulls, RowSet rows) {
        testNonNullFromSparse<false, true>(nulls, rows);
        testNonNullFromSparse<true, false>(nulls, rows);
    }

    // /// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
    // core::TypedExprPtr parseExpression(
    //     const std::string& text,
    //     const RowTypePtr& rowType) {
    //     parse::ParseOptions options;
    //     auto untyped = parse::parseExpr(text, options);
    //     return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
    // }

    // /// Compile typed expression tree into an executable ExprSet.
    // std::unique_ptr<exec::ExprSet> compileExpression(
    //     const std::string& expr,
    //     const RowTypePtr& rowType) {
    //     std::vector<core::TypedExprPtr> expressions = {
    //         parseExpression(expr, rowType)};
    //     return std::make_unique<exec::ExprSet>(
    //         std::move(expressions), execCtx_.get());
    // }

    // /// Evaluate an expression on one batch of data.
    // VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    //     exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    //     SelectivityVector rows(input->size());
    //     std::vector<VectorPtr> result(1);
    //     exprSet.eval(rows, context, result);
    //     return result[0];
    // }

    void test() {
        // Let’s create two vectors of 64-bit integers and one vector of strings.
        auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        auto dow = makeFlatVector<std::string>(
            {"monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday"});

        auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

        // We can also filter rows that have even values of 'a'.
        // facebook::velox::core::PlanNodePtr plan = 
        //     facebook::velox::exec::test::PlanBuilder().values({data}).filter("a % 2 == 0").planNode();
        
        // auto evenA = facebook::velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());

        // std::cout << std::endl
        //     << "> rows with even values of 'a': " << evenA->toString()
        //     << std::endl;
        // std::cout << evenA->toString(0, evenA->size()) << std::endl;
    }

    void test2() {
        boost::timer::cpu_timer test2_timer;
        double test2_time;

        facebook::velox::registerFunction<FilterFunctionTslee, int32_t, int32_t>({"filter_tslee"});

        constexpr int kSize = 1024 * 1024; // 1M
        std::vector<int32_t> input_data;
        input_data.reserve(kSize);
        for (auto i = 0; i < kSize; i += 2) {
            input_data.push_back(i / 2);
            input_data.push_back(kSize - i);
        }

        auto a = makeFlatVector<int32_t>(input_data);
        // auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        // auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        // auto dow = makeFlatVector<std::string>(
        //     {"monday",
        //     "tuesday",
        //     "wednesday",
        //     "thursday",
        //     "friday",
        //     "saturday",
        //     "sunday"});

        // auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});
        auto data = makeRowVector({"a"}, {a});

        auto queryCtx = std::make_shared<core::QueryCtx>();

        auto pool = memory::addDefaultLeafMemoryPool();
        core::ExecCtx execCtx{pool.get(), queryCtx.get()};

        // auto inputRowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"dow", VARCHAR()}});
        auto inputRowType = ROW({{"a", INTEGER()}});

        auto fieldAccessExprNode =
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "a");
        // auto fieldAccessExprPtr =
        //     std::vector<facebook::velox::core::TypedExprPtr>{fieldAccessExprNode};

        auto exprTree = std::make_shared<core::CallTypedExpr>(
            INTEGER(),
            std::vector<core::TypedExprPtr>{fieldAccessExprNode},
            "filter_tslee");

        exec::ExprSet exprSet({exprTree}, &execCtx);

        auto rowVector = std::make_shared<RowVector>(
            execCtx.pool(), // pool where allocations will be made.
            inputRowType, // input row type (defined above).
            BufferPtr(nullptr), // no nulls for this example.
            kSize, // length of the vectors.
            std::vector<VectorPtr>{a}); // the input vector data.

        std::vector<VectorPtr> result{nullptr};

        SelectivityVector rows{kSize};

        exec::EvalCtx evalCtx(&execCtx, &exprSet, rowVector.get());

        test2_timer.start();
        exprSet.eval(rows, evalCtx, result);
        test2_time = test2_timer.elapsed().wall / 1000000.0;

        // Print the output vector, just for fun:
        const auto& outputVector = result.front();
        int32_t passedCount = 0;
        for (vector_size_t i = 0; i < outputVector->size(); ++i) {
            if (outputVector->toString(i) != "null") passedCount++;
            // std::cout << outputVector->toString(i);
        }
        std::cout << "passedCount: " << passedCount << std::endl;

        // auto exprSet = compileExpression("a == 0", asRowType(data->type()));

        // auto c = evaluate(*exprSet, data);

        // auto filterExpr = facebook::velox::exec::constructSpecialForm(
        //     "if",
        //     BIGINT(),
        //     {std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<bool>({false})),
        //     std::make_shared<facebook::velox::exec::FieldReference>(
        //         BIGINT(),
        //         fieldAccessExprPtr,
        //         "a"
        //     ),
        //     std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<int64_t>({1}))},
        //     false);
        std::cout << "[Velox] Test2 Exec elapsed: " << test2_time << " ms" << std::endl;
    }

    void test2_2(bool warmup) {
        std::random_device rd;
        std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device

        // Create a reusable random number generator that generates uniform numbers between 1 and 1048576
        std::uniform_int_distribution<> ran_gen(0, 1048576);

        constexpr int kSize = 1024 * 1024; // 1M
        // constexpr int kSize = 100; // 1M
        std::vector<int32_t> input_data;
        input_data.reserve(kSize);
        for (auto i = 0; i < kSize; i += 2) {
            input_data.push_back(i / 2);
            input_data.push_back(kSize - i);
        }

        auto a = makeFlatVector<int32_t>(input_data);
        // auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        // auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        // auto dow = makeFlatVector<std::string>(
        //     {"monday",
        //     "tuesday",
        //     "wednesday",
        //     "thursday",
        //     "friday",
        //     "saturday",
        //     "sunday"});

        // auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});
        // auto data = makeRowVector({"a"}, {a});

        if (warmup) {
            // test2_2_internal(40, 1000);
        } else {
            for (auto i = 0; i < 1024; i++) {
                int begin = ran_gen(mersenne);
                int end = ran_gen(mersenne);
                if (begin > end) std::swap(begin, end);

                test2_2_internal(begin, end, a, kSize);
            }
        }
    }

    void test2_2_internal(const int begin, const int end, VectorPtr a, int kSize) {
        boost::timer::cpu_timer test2_timer;
        double test2_time;

        // constexpr int kSize = 1024 * 1024 * 1024; // 1M
        // // constexpr int kSize = 100; // 1M
        // std::vector<int32_t> input_data;
        // input_data.reserve(kSize);
        // for (auto i = 0; i < kSize; i += 2) {
        //     input_data.push_back(i / 2);
        //     input_data.push_back(kSize - i);
        // }

        // auto a = makeFlatVector<int32_t>(input_data);
        // // auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        // // auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        // // auto dow = makeFlatVector<std::string>(
        // //     {"monday",
        // //     "tuesday",
        // //     "wednesday",
        // //     "thursday",
        // //     "friday",
        // //     "saturday",
        // //     "sunday"});

        // // auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});
        // auto data = makeRowVector({"a"}, {a});

        auto queryCtx = std::make_shared<core::QueryCtx>();

        auto pool = memory::addDefaultLeafMemoryPool();
        core::ExecCtx execCtx{pool.get(), queryCtx.get()};

        // auto inputRowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"dow", VARCHAR()}});
        auto inputRowType = ROW({{"a", INTEGER()}});

        auto trueExprNode =
            std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true);
        auto fieldAccessExprNode =
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "a");
        auto beginExprNode =
            std::make_shared<core::ConstantTypedExpr>(INTEGER(), begin);
        auto endExprNode =
            std::make_shared<core::ConstantTypedExpr>(INTEGER(), end);
        auto gteExprNode =
            std::make_shared<core::CallTypedExpr>(
                BOOLEAN(),
                std::vector<core::TypedExprPtr>{
                    fieldAccessExprNode,
                    beginExprNode
                },
                "gte");
        auto lteExprNode =
            std::make_shared<core::CallTypedExpr>(
                BOOLEAN(),
                std::vector<core::TypedExprPtr>{
                    fieldAccessExprNode,
                    endExprNode
                },
                "lte");
        auto conjuncExprNode =
            std::make_shared<core::CallTypedExpr>(
                BOOLEAN(),
                std::vector<core::TypedExprPtr>{
                    gteExprNode,
                    lteExprNode
                },
                "and");

        auto exprTree = std::make_shared<core::CallTypedExpr>(
            INTEGER(),
            std::vector<core::TypedExprPtr>{
                conjuncExprNode,
                fieldAccessExprNode},
            "if");

        exec::ExprSet exprSet({exprTree}, &execCtx);

        auto rowVector = std::make_shared<RowVector>(
            execCtx.pool(), // pool where allocations will be made.
            inputRowType, // input row type (defined above).
            BufferPtr(nullptr), // no nulls for this example.
            kSize, // length of the vectors.
            std::vector<VectorPtr>{a}); // the input vector data.

        std::vector<VectorPtr> result{nullptr};

        SelectivityVector rows{kSize};

        exec::EvalCtx evalCtx(&execCtx, &exprSet, rowVector.get());

        test2_timer.start();
        exprSet.eval(rows, evalCtx, result);
        test2_time = test2_timer.elapsed().wall / 1000000.0;

        // Print the output vector, just for fun:
        const auto& outputVector = result.front();
        int32_t passedCount = 0;
        for (vector_size_t i = 0; i < outputVector->size(); ++i) {
            if (outputVector->toString(i) != "null") passedCount++;
            // std::cout << outputVector->toString(i) << " ";
        }
        // std::cout << std::endl;
        // std::cout << "passedCount: " << passedCount << std::endl;

        // auto filterExpr = facebook::velox::exec::constructSpecialForm(
        //     "if",
        //     BIGINT(),
        //     {std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<bool>({false})),
        //     std::make_shared<facebook::velox::exec::FieldReference>(
        //         BIGINT(),
        //         fieldAccessExprPtr,
        //         "a"
        //     ),
        //     std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<int64_t>({1}))},
        //     false);
        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[Velox] Test2 Exec elapsed: " << test2_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    // Copy from DecoderUtilTest, nonNullsFromSparse
    void test3() {
        // We cover cases with different null frequencies and different density of
        // access.
        constexpr int32_t kSize = 2000;
        for (auto nullsIn1000 = 1; nullsIn1000 < 1011; nullsIn1000 += 10) {
            for (auto rowsIn1000 = 1; rowsIn1000 < 1011; rowsIn1000 += 10) {
                raw_vector<int32_t> rows;
                // Have an extra word at the end to allow 64 bit access.
                std::vector<uint64_t> nulls(bits::nwords(kSize) + 1);
                randomBits(nulls, 1000 - nullsIn1000);
                randomRows(kSize, rowsIn1000, rows);
                if (rows.empty()) {
                    // The operation is not defined for 0 rows.
                    rows.push_back(1234);
                }
                testNonNullFromSparseCases(nulls.data(), rows);
            }
        }
    }

    void test4(bool warmup, int32_t kStep_ = 256) {
        std::random_device rd;
        std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device

        // Create a reusable random number generator that generates uniform numbers between 1 and 1048576

        constexpr int kSize = 16 * 1024 * 1024; // 1M
        // constexpr int kSize = 100; // 1M
        raw_vector<int32_t> data;
        raw_vector<int32_t> scatter;
        data.reserve(kSize);
        scatter.reserve(kSize);
        // Data is 0, 100,  2, 98 ... 98, 2.
        // scatter is 0, 2, 4,6 ... 196, 198.
        for (auto i = 0; i < kSize; i += 2) {
            data.push_back(i / 2);
            data.push_back(kSize - i);
            scatter.push_back(i * 2);
            scatter.push_back((i + 1) * 2);
        }

        std::uniform_int_distribution<> ran_gen(0, kSize);

        bool is_point_query = false;
        int begin, end;

        if (warmup) {
            // test4_internal(40, 1000, kStep_);
        } else {
            for (auto i = 0; i < 1024; i++) {
                if (is_point_query) {
                    begin = ran_gen(mersenne);
                    end = begin;
                } else {
                    begin = ran_gen(mersenne);
                    end = ran_gen(mersenne);
                    if (begin > end) std::swap(begin, end);
                }

                // test4_internal2(begin, end, data, scatter, kSize, kStep_);
                test4_internal3(begin, end, data, scatter, kSize, kStep_);
            }
        }
    }

    // Copy from DecoderUtilTest, processFixedWithRun
    void test4_internal(const int begin, const int end, raw_vector<int32_t> &data, raw_vector<int32_t> &scatter, int32_t kSize, int32_t kStep_ = 256) {
        boost::timer::cpu_timer test4_timer;
        double test4_time;
        // Tests processing consecutive batches of integers with processFixedWidthRun.
        int32_t kStep = kStep_;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(kSize);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(kSize);
        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
        std::iota(rows.begin(), rows.end(), 0);
        // The passing values are gathered here. Before each call to
        // processFixedWidthRun, the candidate values are appended here and
        // processFixedWidthRun overwrites them with the passing values and sets
        // numValues to be the first unused index after the passing values.
        raw_vector<int32_t> results;
        int32_t numValues = 0;
        test4_timer.start();
        for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
            int32_t numInput = std::min<int32_t>(kStep, kSize - rowIndex);
            results.resize(numValues + numInput);
            std::memcpy(
                results.data() + numValues,
                data.data() + rowIndex,
                numInput * sizeof(results[0]));
            fprintf(stdout, "rowIndex = %d, numInput = %d, numValues = %d, numValues + numInput = %d\n", rowIndex, numInput, numValues, numValues + numInput);

            facebook::velox::dwio::common::NoHook noHook;
            // facebook::velox::dwio::common::processFixedWidthRun<int32_t, false, true, false>(
            // facebook::velox::dwio::common::processFixedWidthRun<int32_t, false, false, false>(
            facebook::velox::dwio::common::processFixedWidthRun<int32_t, false, false, false>(
                rows,
                rowIndex,
                numInput,
                scatter.data(),
                results.data(),
                hits.data(),
                numValues,
                *filter,
                noHook);
        }
        test4_time = test4_timer.elapsed().wall / 1000000.0;
        // Check that each value that passes the filter is in 'results' and that   its
        // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
        // each row to 2x the row number.
        int32_t passedCount = 0;
        for (auto i = 0; i < kSize; ++i) {
            if (data[i] >= begin && data[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount;
            }
        }

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[Velox] Test4 kStep = " << kStep << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    // Copy from DecoderUtilTest, processFixedWithRun
    void test4_internal2(const int begin, const int end, raw_vector<int32_t> &data, raw_vector<int32_t> &scatter, int32_t kSize, int32_t kStep_ = 256) {
        boost::timer::cpu_timer test4_timer;
        double test4_time = 0.0;
        // Tests processing consecutive batches of integers with processFixedWidthRun.
        // constexpr int kSize = 1024 * 1024; // 1M
        // constexpr int32_t kStep = 16;
        int32_t kStep = kStep_;

        // the row numbers that pass the filter come here, translated via scatter.
        // raw_vector<int32_t> hits(kSize);
        std::vector<raw_vector<int32_t>> hits((kSize / kStep));
        // Each valid index in 'data'
        // raw_vector<int32_t> rows(kSize);
        std::vector<raw_vector<int32_t>> rows((kSize / kStep));
        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
        // std::iota(rows.begin(), rows.end(), 0);
        // The passing values are gathered here. Before each call to
        // processFixedWidthRun, the candidate values are appended here and
        // processFixedWidthRun overwrites them with the passing values and sets
        // numValues to be the first unused index after the passing values.
        std::vector<int32_t> numValues(kSize / kStep);
        int32_t idx = 0;
        // raw_vector<int32_t> results;
        assert(kSize % kStep == 0);
        std::vector<raw_vector<int32_t>> results;
        results.resize((kSize / kStep));
        for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
            hits[idx].resize(kStep);
            rows[idx].resize(kStep);
            results[idx].resize(kStep);
            numValues[idx] = 0;
            std::iota(rows[idx].begin(), rows[idx].end(), rowIndex);
            idx++;
        }
        // idx = 0;
        // for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
        //     for (auto i = 0; i < kStep; ++i) {
        //         assert(results[idx][i] == data[rowIndex + i]);
        //         assert(rows[idx][i] == rowIndex + i);
        //     }
        //     idx++;
        // }
        idx = 0;
        for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
            int32_t numInput = std::min<int32_t>(kStep, kSize - rowIndex);
            std::memcpy(
                    results[idx].data(),
                    data.data() + rowIndex,
                    kStep * sizeof(results[idx][0]));

            facebook::velox::dwio::common::NoHook noHook;
            test4_timer.start();
            facebook::velox::dwio::common::processFixedWidthRun<int32_t, false, false, false>(
                rows[idx],
                0,
                numInput,
                scatter.data(),
                results[idx].data(),
                hits[idx].data(),
                numValues[idx],
                *filter,
                noHook);
            test4_timer.stop();
            auto tmp_test4_time = test4_timer.elapsed().wall / 1000000.0;
            test4_time += tmp_test4_time;
            idx++;
        }
        
        // Check that each value that passes the filter is in 'results' and that   its
        // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
        // each row to 2x the row number.
        int32_t passedCount = 0;
        for (auto i = 0; i < kSize; ++i) {
            if (data[i] >= begin && data[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount;
            }
        }

        int32_t passedCount2 = 0;
        int32_t numValuesTotal = 0;
        for (auto i = 0; i < results.size(); ++i) {
            numValuesTotal += numValues[i];
            for (auto j = 0; j < numValues[i]; ++j) {
                if (results[i][j] >= begin && results[i][j] <= end) {
                    ++passedCount2;
                }
            }
        }
        // for (auto i = 0; i < passedCount; ++i) {
        //     if (results[i] >= begin && results[i] <= end) {
        //         // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
        //         // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
        //         // EXPECT_EQ(data[i], results[passedCount]);
        //         // EXPECT_EQ(i * 2, hits[passedCount]);
        //         ++passedCount2;
        //     }
        // }
        std::cout << "results size = " << results.size() << ", passedCount2 = " << passedCount2 << ", numValues = " << numValuesTotal << std::endl;

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[Velox] Test4 kStep = " << kStep << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    // Copy from DecoderUtilTest, processFixedWithRun // with null case
    void test4_internal3(const int begin, const int end, raw_vector<int32_t> &data, raw_vector<int32_t> &scatter, int32_t kSize, int32_t kStep_ = 256) {
        boost::timer::cpu_timer test4_timer;
        double test4_time;
        // Tests processing consecutive batches of integers with processFixedWidthRun.
        // constexpr int kSize = 1024 * 1024; // 1M
        // constexpr int32_t kStep = 16;
        int32_t kStep = kStep_;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(kSize);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(kSize);
        const int32_t* rows_ = rows.data();
        // inner & outer
        raw_vector<int32_t> innerVector_;
        raw_vector<int32_t> outerVector_;

        int64_t n_bytes_for_nulls = bits::nbytes(kSize);
        uint64_t *nulls = new uint64_t[((n_bytes_for_nulls / 8) + 1)];
        randomBits(nulls, kSize, 600); // 40% null
        // for (int64_t i = 0; i < kSize; i++) {
        //     if (i % 2 == 0) bits::clearBit(nulls, i); // null value --> clearBit
        //     else bits::setBit(nulls, i);
        // }

        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
        std::iota(rows.begin(), rows.end(), 0);
        // The passing values are gathered here. Before each call to
        // processFixedWidthRun, the candidate values are appended here and
        // processFixedWidthRun overwrites them with the passing values and sets
        // numValues to be the first unused index after the passing values.
        raw_vector<int32_t> results;
        results.resize(kSize);
        // std::memcpy(
        //         results.data(),
        //         data.data(),
        //         kSize * sizeof(results[0]));

        int32_t numValues = 0;
        facebook::velox::dwio::common::NoHook noHook;
        bool hasFilter = true;
        constexpr bool filterOnly = false;
        bool hasHook = false;
        auto numRows = kSize;
        auto numNonNull = numRows;
        auto rowsAsRange = folly::Range<const int32_t*>(rows_, numRows);
        constexpr bool is_dense = true;
        test4_timer.start();
        if (true) { // hasNulls
            int32_t tailSkip = 0;
            raw_vector<int32_t>* innerVector = nullptr;
            auto outerVector = &outerVector_;
            // In non-DWRF formats, it can be the visitor is not dense but
            // this run of rows is dense.
            if (is_dense || rowsAsRange.back() == rowsAsRange.size() - 1) {
                dwio::common::nonNullRowsFromDense(nulls, numRows, *outerVector);
                numNonNull = outerVector->size();
                if (!numNonNull) {
                    // visitor.setAllNull(hasFilter ? 0 : numRows);
                    return;
                }
            } else {
                // innerVector = &innerVector_;
                // auto anyNulls = dwio::common::nonNullRowsFromSparse < hasFilter,
                //     !hasFilter &&
                //     !hasHook >
                //         (nulls,
                //         rowsAsRange,
                //         *innerVector,
                //         *outerVector,
                //         (hasFilter || hasHook) ? nullptr : visitor.rawNulls(numRows),
                //         tailSkip);
                // if (anyNulls) {
                //     visitor.setHasNulls();
                // }
                // if (innerVector->empty()) {
                //     skip<false>(tailSkip, 0, nullptr);
                //     visitor.setAllNull(hasFilter ? 0 : numRows);
                //     return;
                // }
            }
            if (false) { // super::useVInts
                // if (Visitor::dense) {
                //     super::bulkRead(numNonNull, data);
                // } else {
                //     super::bulkReadRows(*innerVector, data);
                // }
                // skip<false>(tailSkip, 0, nullptr);
                std::cout << "outerVector->size(): " << outerVector->size() << std::endl;
                auto dataRows = innerVector
                    ? folly::Range<const int*>(innerVector->data(), innerVector->size())
                    : folly::Range<const int32_t*>(rows, outerVector->size());
                facebook::velox::dwio::common::processFixedWidthRun<int32_t, filterOnly, false, is_dense>(
                    dataRows,
                    0,
                    dataRows.size(),
                    outerVector->data(),
                    results.data(),
                    hasFilter ? hits.data() : nullptr,
                    numValues,
                    *filter,
                    noHook);
            } else {
                std::cout << "outerVector->size(): " << outerVector->size() << std::endl;
                dwio::common::SeekableArrayInputStream is((const char *)data.data(), kSize * sizeof(int32_t));
                const char *bufferStart = (const char *)data.data();
                const char *bufferEnd = (const char *)(data.data() + kSize);
                auto dataRows = folly::Range<const int32_t*>(outerVector->data(), outerVector->size());
                dwio::common::fixedWidthScan<int32_t, filterOnly, false>(
                    dataRows,
                    // rowsAsRange,
                    // innerVector
                    //     ? folly::Range<const int32_t*>(*innerVector)
                    //     : folly::Range<const int32_t*>(rows_, outerVector->size()),
                    nullptr, //outerVector->data(),
                    results.data(), //data,
                    hasFilter ? hits.data() : nullptr, //hasFilter ? visitor.outputRows(numRows) : nullptr,
                    numValues,
                    is,
                    bufferStart,
                    bufferEnd,
                    *filter,
                    noHook);
                // skip<false>(tailSkip, 0, nullptr);
            }
        } else {
            // if (super::useVInts) {
            //     if (Visitor::dense) {
            //     super::bulkRead(numRows, visitor.rawValues(numRows));
            //     } else {
            //     super::bulkReadRows(
            //         folly::Range<const int32_t*>(rows, numRows),
            //         visitor.rawValues(numRows));
            //     }
            //     dwio::common::
            //         processFixedWidthRun<T, filterOnly, false, Visitor::dense>(
            //             rowsAsRange,
            //             0,
            //             rowsAsRange.size(),
            //             hasHook ? velox::iota(numRows, visitor.innerNonNullRows())
            //                     : nullptr,
            //             visitor.rawValues(numRows),
            //             hasFilter ? visitor.outputRows(numRows) : nullptr,
            //             numValues,
            //             visitor.filter(),
            //             visitor.hook());
            // } else {
            //     dwio::common::fixedWidthScan<T, filterOnly, false>(
            //         rowsAsRange,
            //         hasHook ? velox::iota(numRows, visitor.innerNonNullRows())
            //                 : nullptr,
            //         visitor.rawValues(numRows),
            //         hasFilter ? visitor.outputRows(numRows) : nullptr,
            //         numValues,
            //         *super::inputStream,
            //         super::bufferStart,
            //         super::bufferEnd,
            //         visitor.filter(),
            //         visitor.hook());
            // }
            // }
            // visitor.setNumValues(hasFilter ? numValues : numRows);
        }
        test4_time = test4_timer.elapsed().wall / 1000000.0;
        // Check that each value that passes the filter is in 'results' and that   its
        // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
        // each row to 2x the row number.
        int32_t passedCount = 0;
        for (auto i = 0; i < kSize; ++i) {
            if (bits::isBitNull(nulls, i)) continue;
            if (data[i] >= begin && data[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount;
            }
        }

        int32_t passedCount2 = 0;
        for (auto i = 0; i < passedCount; ++i) {
            if (results[i] >= begin && results[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount2;
            }
        }
        if (passedCount != passedCount2 || passedCount2 != numValues) { exit(-1); }
        std::cout << "results size = " << results.size() << ", passedCount2 = " << passedCount2 << ", numValues = " << numValues << std::endl;
        std::cout << "hits size = " << hits.size() << std::endl;

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[Velox] Test4 kStep = " << kStep << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    void test5(bool warmup, int Size, int32_t kStep_ = 256) {
        std::random_device rd;
        std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device

        // Create a reusable random number generator that generates uniform numbers between 1 and 1048576

        int kSize = Size * 1024 * 1024; // 1M
        // constexpr int kSize = 100; // 1M
        raw_vector<int64_t> data;
        raw_vector<int32_t> scatter;
        data.reserve(kSize);
        scatter.reserve(kSize);
        // Data is 0, 100,  2, 98 ... 98, 2.
        // scatter is 0, 2, 4,6 ... 196, 198.
        for (auto i = 0; i < kSize; i += 2) {
            data.push_back(i / 2);
            data.push_back(kSize - i);
            scatter.push_back(i * 2);
            scatter.push_back((i + 1) * 2);
        }

        std::uniform_int_distribution<> ran_gen(0, kSize);

        bool is_point_query = false;
        int begin, end;

        if (warmup) {
            // test4_internal(40, 1000, kStep_);
        } else {
            for (auto i = 0; i < 1024; i++) {
                if (is_point_query) {
                    begin = ran_gen(mersenne);
                    end = begin;
                } else {
                    begin = ran_gen(mersenne);
                    end = ran_gen(mersenne);
                    if (begin > end) std::swap(begin, end);
                }

                test5_internal2(begin, end, data, scatter, kSize, kStep_);
            }
        }
    }

    void test5_internal2(const int begin, const int end, raw_vector<int64_t> &data, raw_vector<int32_t> &scatter, int32_t kSize, int32_t kStep_ = 256) {
        boost::timer::cpu_timer test4_timer;
        double test4_time;
        // Tests processing consecutive batches of integers with processFixedWidthRun.
        // constexpr int kSize = 1024 * 1024; // 1M
        // constexpr int32_t kStep = 16;
        int32_t kStep = kStep_;

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(kSize);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(kSize);
        auto filter = std::make_unique<common::BigintRange>(begin, end, false);
        std::iota(rows.begin(), rows.end(), 0);
        // The passing values are gathered here. Before each call to
        // processFixedWidthRun, the candidate values are appended here and
        // processFixedWidthRun overwrites them with the passing values and sets
        // numValues to be the first unused index after the passing values.
        raw_vector<int64_t> results;
        results.resize(kSize);
        std::memcpy(
                results.data(),
                data.data(),
                kSize * sizeof(results[0]));

        int32_t numValues = 0;
        facebook::velox::dwio::common::NoHook noHook;
        test4_timer.start();
        facebook::velox::dwio::common::processFixedWidthRun<int64_t, false, false, false>(
            rows,
            0,
            kSize,
            scatter.data(),
            results.data(),
            hits.data(),
            numValues,
            *filter,
            noHook);
        test4_time = test4_timer.elapsed().wall / 1000000.0;
        // Check that each value that passes the filter is in 'results' and that   its
        // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
        // each row to 2x the row number.
        int32_t passedCount = 0;
        for (auto i = 0; i < kSize; ++i) {
            if (data[i] >= begin && data[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount;
            }
        }

        int32_t passedCount2 = 0;
        for (auto i = 0; i < passedCount; ++i) {
            if (results[i] >= begin && results[i] <= end) {
                // fprintf(stdout, "[%d] EXPECT_EQ1 %d == %d\n", i, data[i], results[passedCount]);
                // fprintf(stdout, "[%d] EXPECT_EQ2 %d == %d\n", i, i * 2, hits[passedCount]);
                // EXPECT_EQ(data[i], results[passedCount]);
                // EXPECT_EQ(i * 2, hits[passedCount]);
                ++passedCount2;
            }
        }
        std::cout << "results size = " << results.size() << ", passedCount2 = " << passedCount2 << ", numValues = " << numValues << std::endl;

        double selectivity = 100 * ((double) passedCount / kSize);
        std::cout << "[Velox] Test4 kStep = " << kStep << ", Exec elapsed: " << test4_time << " ms, begin: " << begin << ", end: " << end
                  << ", passedCount = " << passedCount << ", sel: " << selectivity << std::endl;
    }

    folly::Random::DefaultGenerator rng_;
    std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
    std::shared_ptr<core::QueryCtx> queryCtx_{
        std::make_shared<core::QueryCtx>(executor_.get())};
    std::unique_ptr<core::ExecCtx> execCtx_{
        std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};

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

} // namespace duckdb