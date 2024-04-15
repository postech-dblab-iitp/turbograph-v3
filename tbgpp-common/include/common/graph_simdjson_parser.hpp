#pragma once

#include <iostream>
#include <chrono>
#include <algorithm>

#include "common/vector.hpp"
#include "common/enums/json_file_type.hpp"
#include "schemaless/schema_hash_table.hpp"
#include "schemaless/ssj/allpairs_cluster.h"
#include "simdjson.h"
#include "fptree.hpp"
#include "icecream.hpp"

using namespace simdjson;

#define TILE_SIZE 1024 // or 4096
#define FREQUENCY_THRESHOLD 0.95
// #define SET_SIM_THRESHOLD 0.7
#define SET_SIM_THRESHOLD 0.99
#define NEO4J_VERTEX_ID_NAME "id"

// static variable
std::chrono::duration<double> fpgrowth_duration;

namespace duckdb {

#ifndef LIDPAIR
#define LIDPAIR
typedef std::pair<idx_t, idx_t> LidPair;
#endif

class GraphSIMDJSONFileParser {

public:
    GraphSIMDJSONFileParser() {}
    ~GraphSIMDJSONFileParser() {}

    GraphSIMDJSONFileParser(std::shared_ptr<ClientContext> client_, ExtentManager *ext_mng_, Catalog *cat_instance_) {
        client = client_;
        ext_mng = ext_mng_;
        cat_instance = cat_instance_;

        sch_HT.resize(1000); // TODO appropriate size

        cluster_algo = new AllPairsCluster<Jaccard>(SET_SIM_THRESHOLD);
    }

    void SetLidToPidMap (vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> *lid_to_pid_map_) {
        load_edge = true;
        lid_to_pid_map = lid_to_pid_map_;
    }

    uint64_t GetNewPropertyID() {
        return propertyIDver++;
    }

    void StoreLidToPidInfo(DataChunk &data, vector<idx_t> &key_column_idxs, ExtentID eid) {
        D_ASSERT(key_column_idxs.size() == 1); // TODO handle general case
        idx_t *key_column = (idx_t *)data.data[key_column_idxs[0]].GetData();

        idx_t pid_base = (idx_t) eid;
		pid_base = pid_base << 32;

        LidPair lid_key{0, 0};
        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
            lid_key.first = key_column[seqno];
            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
        }
    }

    size_t InitJsonFile(const char *json_file_path, JsonFileType jftype) {
        std::string input_path = std::string(json_file_path);

        if (jftype == JsonFileType::JSON) {
            // Load & Parse JSON File
            json = padded_string::load(input_path);
            // docs = parser.iterate_many(json);
            doc = parser.iterate(json);
            transactions.resize(TILE_SIZE);
        } else if (jftype == JsonFileType::JSONL) {
            json = padded_string::load(input_path);
            docs = parser.iterate_many(json);
            transactions.resize(TILE_SIZE);
        }
        return 0;
    }

    void LoadJson(string &label_name, vector<string> &label_set, const char *json_key, DataChunk &data, JsonFileType jftype, GraphCatalogEntry *graph_cat, PartitionCatalogEntry *partition_cat, GraphComponentType gctype = GraphComponentType::INVALID) {
        if (jftype == JsonFileType::JSON) {
            // _IterateJson(label_name, json_key, data, graph_cat, partition_cat);
        } else if (jftype == JsonFileType::JSONL) {
            // Extract Schema & Preprocessing
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering();

            // Clustering
            _ClusterSchema();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            // _IterateJsonL(data, gctype);
        }
    }

    void IterateJson(const char *label_name, const char *json_key, DataChunk &data, JsonFileType jftype, GraphCatalogEntry *graph_cat, PartitionCatalogEntry *partition_cat, GraphComponentType gctype = GraphComponentType::INVALID) {
        if (jftype == JsonFileType::JSON) {
            _IterateJson(label_name, json_key, data, graph_cat, partition_cat);
        } else if (jftype == JsonFileType::JSONL) {
            _IterateJsonL(data, gctype);
        }
    }

    void _IterateJson(const char *label_name, const char *json_key, DataChunk &data, GraphCatalogEntry *graph_cat, PartitionCatalogEntry *partition_cat) {
        // Run FPGrowth for the sample
        // for (auto doc_ : docs) {
        recursive_collect_key_paths(doc[json_key], "", false, transactions, 0);
        // }

        // Get Schema
        most_common_schema.clear();
        most_common_key_paths.clear();
        key_column_idxs.clear();
        for (std::set<Item>::iterator item_it = most_frequency_pattern.first.begin(); item_it != most_frequency_pattern.first.end(); item_it++) {
            std::string key_path_and_type = std::string(*item_it);
            size_t delimiter_pos = key_path_and_type.find_last_of("_");
            if (delimiter_pos == std::string::npos) {
                continue;
            }
            std::string key_path = key_path_and_type.substr(0, delimiter_pos);
            std::string type = key_path_and_type.substr(delimiter_pos + 1);
            most_common_key_paths.push_back(key_path);
            std::cout << key_path << ": ";
            if (key_path == "id") {
                key_column_idxs.push_back(std::distance(most_frequency_pattern.first.begin(), item_it));
            }
            if (type == "str") {
                std::cout << "str / ";
                most_common_schema.push_back(LogicalType::VARCHAR);
            } else if (type == "int64") {
                std::cout << "int64 / ";
                most_common_schema.push_back(LogicalType::BIGINT);
            } else if (type == "uint64 / ") {
                most_common_schema.push_back(LogicalType::UBIGINT);
            } else if (type == "bool") {
                most_common_schema.push_back(LogicalType::BOOLEAN);
            } else if (type == "double") {
                std::cout << "double / ";
                most_common_schema.push_back(LogicalType::DOUBLE);
            } else if (type == "arr") {
                std::cout << "arr / ";
                // most_common_schema.push_back(LogicalType::LIST(LogicalType::BIGINT));
                // most_common_schema.push_back(LogicalType::LIST(LogicalType::VARCHAR));
                most_common_schema.push_back(LogicalType::LIST(LogicalType::DOUBLE));
            }
        }
        std::cout << std::endl;

        // Initialize Property Schema Catalog Entry using Schema of the vertex
		// vector<int64_t> key_column_idxs_ = reader.GetKeyColumnIndexFromHeader();
		// vector<idx_t> key_column_idxs;
		// for (size_t i = 0; i < key_column_idxs_.size(); i++) key_column_idxs.push_back((idx_t) key_column_idxs_[i]);
		
        PartitionID new_pid = partition_cat->GetPartitionID();
		string property_schema_name = "vps_" + std::string(label_name);
		fprintf(stdout, "prop_schema_name = %s\n", property_schema_name.c_str());
		CreatePropertySchemaInfo propertyschema_info("main", property_schema_name.c_str(), new_pid, partition_cat->GetOid());
		property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance->CreatePropertySchema(*client.get(), &propertyschema_info);
		
		vector<PropertyKeyID> property_key_ids;
		graph_cat->GetPropertyKeyIDs(*client.get(), most_common_key_paths, most_common_schema, property_key_ids);
		partition_cat->AddPropertySchema(*client.get(), property_schema_cat->GetOid(), property_key_ids);
        property_schema_cat->SetSchema(*client.get(), most_common_key_paths, most_common_schema, property_key_ids);
		property_schema_cat->SetKeyColumnIdxs(key_column_idxs);

        // Initialize LID_TO_PID_MAP
		if (load_edge) {
			lid_to_pid_map->emplace_back(label_name, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
			lid_to_pid_map_instance = &lid_to_pid_map->back().second;
			// lid_to_pid_map_instance->reserve(approximated_num_rows * 2);
		}

        // Rewind
        doc.rewind();

        // Iterate & Create DataChunk
        data.Initialize(most_common_schema, STORAGE_STANDARD_VECTOR_SIZE);

        // for (auto doc_ : docs) {
        recursive_iterate_json(doc[json_key], "", false, 0, 0, data);
        // }
    }

    void _IterateJsonL(DataChunk &data, GraphComponentType gctype) {
        // deprecate
        // if (gctype == GraphComponentType::INVALID) {
        //     for (auto doc_ : docs) {
        //         std::string_view type = doc_["type"].get_string();
        //         if (type == "node") {
        //             // fprintf(stderr, "Node\n");
        //             ondemand::value labels = doc_["labels"];
        //             D_ASSERT(labels.type() == ondemand::json_type::array);
        //             int num_labels = 0;
        //             if (labels.count_elements() >= 2) {
        //                 for (auto child : labels.get_array()) {
        //                     std::cout << "\"" << child.get_raw_json_string() << "\"";
        //                 }
        //             }
        //             // recursive_print_json(doc_["properties"], "", false);

        //             // Get Scheam from samples
                    
        //         } else if (type == "relationship") {
        //             fprintf(stderr, "Relationship\n");
        //             ondemand::value labels = doc_["labels"];
        //             D_ASSERT(labels.type() == ondemand::json_type::array);
        //             // recursive_print_json(doc_["properties"], "", false);

        //             // Get Scheam from samples
        //         }
        //     }
        // } else if (gctype == GraphComponentType::VERTEX) {
        //     int num_tuples = 0;
        //     for (auto doc_ : docs) {
        //         // recursive_collect_key_paths_jsonl(doc_["properties"], "", true, transactions, num_tuples);
        //         num_tuples++;

        //         if (num_tuples == TILE_SIZE) {
        //             // Run FPGrowth
        //             auto fpgrowth_start = std::chrono::high_resolution_clock::now();
        //             // Run FpGrowth
        //             std::cout << "\tRun FPGrowth for (" << num_tuples << ") tuples\n";
                    
        //             const uint64_t minimum_support_threshold = 2;
        //             const FPTree fptree{ transactions, minimum_support_threshold };
        //             const std::set<Pattern> patterns = fptree_growth( fptree );
        //             auto fpgrowth_end = std::chrono::high_resolution_clock::now();
        //             fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

        //             Pattern *current_mfp = nullptr;
        //             // std::cout << "Pattern Size = " << patterns.size() << std::endl;
        //             for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
        //                 // std::cout << "Set Size = " << it->first.size() << " ";
        //                 // std::cout << "({";
        //                 bool print_comma = false;
        //                 if (current_mfp == nullptr) {
        //                     current_mfp = new Pattern;
        //                     current_mfp->first = it->first;
        //                     current_mfp->second = it->second;
        //                 } else {
        //                     if (current_mfp->first.size() < it->first.size() && it->second >= (num_tuples * FREQUENCY_THRESHOLD)) {
        //                         current_mfp->first = it->first;
        //                         current_mfp->second = it->second;
        //                     }
        //                 }
        //                 // for (std::set<Item>::iterator item_it = it->first.begin(); item_it != it->first.end(); item_it++) {
        //                 //     if (print_comma) std::cout << ",";
        //                 //     std::cout << *item_it;
        //                 //     print_comma = true;
        //                 // }
        //                 // std::cout << "}, " << it->second << ")\n";
        //             }
        //             most_frequency_pattern.first = current_mfp->first;
        //             most_frequency_pattern.second = current_mfp->second;

        //             // Clear Transactions
        //             for (int i = 0; i < TILE_SIZE; i++) transactions[i].clear();
        //             num_tuples = 0;
        //             break;
        //         }
        //     }

        //     if (num_tuples > 0) {
        //         auto fpgrowth_start = std::chrono::high_resolution_clock::now();
        //         // Run FpGrowth for remaining
        //         std::cout << "\tRun FPGrowth for (" << num_tuples << ") tuples\n";

        //         const uint64_t minimum_support_threshold = 2;
        //         const FPTree fptree{ transactions, minimum_support_threshold };
        //         const std::set<Pattern> patterns = fptree_growth( fptree );
        //         auto fpgrowth_end = std::chrono::high_resolution_clock::now();
        //         fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

        //         Pattern *current_mfp = nullptr;
        //         //std::cout << "Pattern Size = " << patterns.size() << std::endl;
        //         for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
        //             //std::cout << "Set Size = " << it->first.size() << " ";
        //             //std::cout << "({";
        //             bool print_comma = false;
        //             if (current_mfp == nullptr) {
        //                 current_mfp = new Pattern;
        //                 current_mfp->first = it->first;
        //                 current_mfp->second = it->second;
        //             } else {
        //                 if (current_mfp->first.size() < it->first.size() && it->second >= (num_tuples * FREQUENCY_THRESHOLD)) {
        //                     current_mfp->first = it->first;
        //                     current_mfp->second = it->second;
        //                 }
        //             }
        //             //for (std::set<Item>::iterator item_it = it->first.begin(); item_it != it->first.end(); item_it++) {
        //                 // if (print_comma) std::cout << ",";
        //             //     std::cout << *item_it;
        //             //     print_comma = true;
        //             // }
        //             // std::cout << "}, " << it->second << ")\n";

        //             most_frequency_pattern.first = current_mfp->first;
        //             most_frequency_pattern.second = current_mfp->second;
        //         }

        //         // Clear Transactions
        //         for (int i = 0; i < TILE_SIZE; i++) transactions[i].clear();
        //     }
            
        //     num_tuples = 0;

        //     // Get Scheam from samples
        //     most_common_schema.clear();
        //     most_common_key_paths.clear();
        //     for (std::set<Item>::iterator item_it = most_frequency_pattern.first.begin(); item_it != most_frequency_pattern.first.end(); item_it++) {
        //         std::string key_path_and_type = std::string(*item_it);
        //         size_t delimiter_pos = key_path_and_type.find_last_of("_");
        //         if (delimiter_pos == std::string::npos) {
        //             continue;
        //         }
        //         std::string key_path = key_path_and_type.substr(0, delimiter_pos);
        //         std::string type = key_path_and_type.substr(delimiter_pos + 1);
        //         most_common_key_paths.push_back(key_path);
        //         std::cout << key_path << ": ";
        //         if (type == "str") {
        //             std::cout << "str / ";
        //             most_common_schema.push_back(LogicalType::VARCHAR);
        //         } else if (type == "int64") {
        //             std::cout << "int64 / ";
        //             most_common_schema.push_back(LogicalType::BIGINT);
        //         } else if (type == "uint64 / ") {
        //             most_common_schema.push_back(LogicalType::UBIGINT);
        //         } else if (type == "bool") {
        //             most_common_schema.push_back(LogicalType::BOOLEAN);
        //         } else if (type == "double") {
        //             std::cout << "double / ";
        //             most_common_schema.push_back(LogicalType::DOUBLE);
        //         } else if (type == "arr") {
        //             std::cout << "arr / ";
        //             most_common_schema.push_back(LogicalType::LIST(LogicalType::BIGINT));
        //             // most_common_schema.push_back(LogicalType::LIST(LogicalType::VARCHAR)); // TODO..
        //         }
        //     }
        //     std::cout << std::endl;

        //     data.Initialize(most_common_schema, STORAGE_STANDARD_VECTOR_SIZE);

        //     // Iterate from the beginning
        //     for (auto doc_ : docs) {
        //         // icecream::ic.enable(); IC(num_tuples); icecream::ic.disable();
        //         recursive_iterate_jsonl(doc_["properties"], "", true, num_tuples, 0, data);
        //         num_tuples++;
        //         if (num_tuples == STORAGE_STANDARD_VECTOR_SIZE) {
        //             // Do something
        //             fprintf(stderr, "CreateExtent\n");
        //             num_tuples = 0;
        //             data.Reset(STORAGE_STANDARD_VECTOR_SIZE);
        //         }
        //     }
        //     if (num_tuples > 0) {
        //         fprintf(stderr, "CreateExtent\n");
        //     }
        // } else if (gctype == GraphComponentType::EDGE) {
        //     icecream::ic.enable(); IC(); icecream::ic.disable();
        //     for (auto doc_ : docs) {
        //         std::string_view type = doc_["type"].get_string();

        //         ondemand::value labels = doc_["labels"];
        //         D_ASSERT(labels.type() == ondemand::json_type::array);
        //         // recursive_print_json(doc_["properties"], "", false);

        //         // Get Scheam from samples
        //     }
        // }
    }

    void _ExtractSchema(GraphComponentType gctype) {
        if (gctype == GraphComponentType::INVALID) {
            D_ASSERT(false); // deactivate temporarily
            // for (auto doc_ : docs) {
            //     std::string_view type = doc_["type"].get_string();
            //     if (type == "node") {
            //         // fprintf(stderr, "Node\n");
            //         ondemand::value labels = doc_["labels"];
            //         D_ASSERT(labels.type() == ondemand::json_type::array);
            //         int num_labels = 0;
            //         if (labels.count_elements() >= 2) {
            //             for (auto child : labels.get_array()) {
            //                 std::cout << "\"" << child.get_raw_json_string() << "\"";
            //             }
            //         }
            //         // recursive_print_json(doc_["properties"], "", false);

            //         // Get Scheam from samples
                    
            //     } else if (type == "relationship") {
            //         fprintf(stderr, "Relationship\n");
            //         ondemand::value labels = doc_["labels"];
            //         D_ASSERT(labels.type() == ondemand::json_type::array);
            //         // recursive_print_json(doc_["properties"], "", false);

            //         // Get Scheam from samples
            //     }
            // }
        } else if (gctype == GraphComponentType::VERTEX) {
            int num_tuples = 0;
            // TODO check; always same order?
            for (auto doc_ : docs) { // iterate each jsonl document; one for each vertex
                // properties object has vertex properties; assume Neo4J dump file format
                std::vector<uint64_t> tmp_vec;

                string current_prefix = "";
                recursive_collect_key_paths_jsonl(doc_["properties"], current_prefix, true, tmp_vec, num_tuples);

                int64_t schema_id;
                sch_HT.find(tmp_vec, schema_id);
                // for (auto i = 0; i < tmp_vec.size(); i++) {
                //     fprintf(stdout, "%ld ", tmp_vec[i]);
                // }
                // fprintf(stdout, ": %ld\n", schema_id);
                if (schema_id == INVALID_TUPLE_GROUP_ID) { // not found
                    schema_id = schema_groups.size();
                    sch_HT.insert(tmp_vec, schema_id);
                    schema_groups.push_back(tmp_vec);
                    corresponding_schemaID.push_back(schema_id);
                } else {
                    corresponding_schemaID.push_back(schema_id);
                }
                num_tuples++;
            }
            schema_property_freq_vec.resize(property_freq_vec.size(), 0);
            for (size_t i = 0; i < schema_groups.size(); i++) {
                for (size_t j = 0; j < schema_groups[i].size(); j++) {
                    schema_property_freq_vec[schema_groups[i][j]]++;
                }
            }
            printf("schema_groups.size = %ld\n", schema_groups.size());

            return;

            // data.Initialize(most_common_schema, STORAGE_STANDARD_VECTOR_SIZE);

            // // Iterate from the beginning
            // for (auto doc_ : docs) {
            //     // icecream::ic.enable(); IC(num_tuples); icecream::ic.disable();
            //     recursive_iterate_jsonl(doc_["properties"], "", true, num_tuples, 0, data);
            //     num_tuples++;
            //     if (num_tuples == STORAGE_STANDARD_VECTOR_SIZE) {
            //         // Do something
            //         fprintf(stderr, "CreateExtent\n");
            //         num_tuples = 0;
            //         data.Reset(STORAGE_STANDARD_VECTOR_SIZE);
            //     }
            // }
            // if (num_tuples > 0) {
            //     fprintf(stderr, "CreateExtent\n");
            // }
        } else if (gctype == GraphComponentType::EDGE) {
            for (auto doc_ : docs) {
                std::string_view type = doc_["type"].get_string();

                ondemand::value labels = doc_["labels"];
                D_ASSERT(labels.type() == ondemand::json_type::array);
                // recursive_print_json(doc_["properties"], "", false);

                // Get Scheam from samples
            }
        }
    }

    void _PreprocessSchemaForClustering() {
        // Sort tokens by frequency
        printf("Schema Freq vec\n");
        for (int i = 0; i < schema_property_freq_vec.size(); i++) {
            printf("%d-th (%s) freq: %ld, ", i, id_to_property_vec[i].c_str(), schema_property_freq_vec[i]);
        }
        printf("\n");
        order.resize(schema_property_freq_vec.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return schema_property_freq_vec[a] < schema_property_freq_vec[b];
        });
        vector<uint64_t> inv_order(order.size());
        for (size_t i = 0; i < order.size(); i++) {
            inv_order[order[i]] = i;
        }
        for (int i = 0; i < order.size(); i++) {
            printf("%ld - freq %ld, ", order[i], schema_property_freq_vec[order[i]]);
        }
        printf("\n");

        // TODO sort schema groups by their size
        // TODO? do not use addrecord to avoid copy?
        for (size_t i = 0; i < schema_groups.size(); i++) {
            IntRecord rec;
            // rec.recordid = i;
            for (size_t j = 0; j < schema_groups[i].size(); j++) {
                rec.tokens.push_back(inv_order[schema_groups[i][j]]);
            }
            std::sort(rec.tokens.begin(), rec.tokens.end(), [&](size_t a, size_t b) {
                return a < b;
            });
            // for (size_t x = 0; x < rec.tokens.size(); x++) {
            //     printf("%ld, ", rec.tokens[x]);
            // }
            // printf("\n");
            cluster_algo->addrecord(rec);
        }
    }

    void _ClusterSchema() {
        cluster_algo->doindex();
        cluster_algo->docluster();

        auto &cluster_to_rid_lists = cluster_algo->getctorlists();
        sg_to_cluster_vec.resize(schema_groups.size(), -1);
        for (size_t i = 0; i < cluster_to_rid_lists.size(); i++) {
            for (size_t j = 0; j < cluster_to_rid_lists[i].size(); j++) {
                D_ASSERT(sg_to_cluster_vec.size() > cluster_to_rid_lists[i][j]);
                D_ASSERT(sg_to_cluster_vec[cluster_to_rid_lists[i][j]] == -1);
                sg_to_cluster_vec[cluster_to_rid_lists[i][j]] = i;
            }
        }
        num_clusters = cluster_to_rid_lists.size();

        // TODO phase 2
    }

    void _CreateExtents(GraphComponentType gctype, GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
        if (gctype == GraphComponentType::VERTEX) {
            _CreateVertexExtents(graph_cat, label_name, label_set);
        } else if (gctype == GraphComponentType::EDGE) {
            _CreateEdgeExtents(graph_cat, label_name, label_set);
        }
    }

    void _CreateVertexExtents(GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
        vector<DataChunk> datas(num_clusters);
        property_to_id_map_per_cluster.resize(num_clusters);

        // Create partition catalog
        string partition_name = DEFAULT_VERTEX_PARTITION_PREFIX + label_name;
        PartitionID new_pid = graph_cat->GetNewPartitionID();
        CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str(), new_pid);
        PartitionCatalogEntry *partition_cat = 
            (PartitionCatalogEntry *)cat_instance->CreatePartition(*client.get(), &partition_info);
        graph_cat->AddVertexPartition(*client.get(), new_pid, partition_cat->GetOid(), label_set);
        partition_cat->SetPartitionID(new_pid);

        vector<string> key_names;
        vector<LogicalType> types;
        vector<PropertyKeyID> universal_property_key_ids;
        for (auto i = 0; i < order.size(); i++) {
            auto original_idx = order[i];
            get_key_and_type(id_to_property_vec[original_idx], key_names, types);
            // printf("%d - %s(%s), ", i, key_names.back().c_str(), types.back().ToString().c_str());
        }
        // printf("\n");
        graph_cat->GetPropertyKeyIDs(*client.get(), key_names, types, universal_property_key_ids);
        partition_cat->SetSchema(*client.get(), key_names, types, universal_property_key_ids);

        // Create property schema catalog for each cluster
        property_schema_cats.resize(num_clusters);
        vector<vector<idx_t>> per_cluster_key_column_idxs;
        per_cluster_key_column_idxs.resize(num_clusters);
        for (size_t i = 0; i < num_clusters; i++) {
            string property_schema_name = DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX + std::string(label_name) + "_" + std::to_string(i);
            CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
            property_schema_cats[i] = 
                (PropertySchemaCatalogEntry*) cat_instance->CreatePropertySchema(*client.get(), &propertyschema_info);
             
             // Create Physical ID Index Catalog & Add to PartitionCatalogEntry
            CreateIndexInfo idx_info(DEFAULT_SCHEMA, label_name + "_" + std::to_string(property_schema_cats[i]->GetOid()) + "_id", IndexType::PHYSICAL_ID, 
                partition_cat->GetOid(), property_schema_cats[i]->GetOid(), 0, {-1});
            IndexCatalogEntry *index_cat = (IndexCatalogEntry *)cat_instance->CreateIndex(*client.get(), &idx_info);
            partition_cat->SetPhysicalIDIndex(index_cat->GetOid());
            property_schema_cats[i]->SetPhysicalIDIndex(index_cat->GetOid());
            
            // Parse schema informations
            vector<PropertyKeyID> property_key_ids;
            vector<LogicalType> cur_cluster_schema_types;
            vector<string> cur_cluster_schema_names;

            vector<unsigned int> &tokens = cluster_algo->getclustertokens(i);
            for (size_t token_idx = 0; token_idx < tokens.size(); token_idx++) {
                auto original_idx = order[tokens[token_idx]];
                
                if (get_key_and_type(id_to_property_vec[original_idx], cur_cluster_schema_names, cur_cluster_schema_types)) {
                    per_cluster_key_column_idxs[i].push_back(token_idx);
                }
                property_to_id_map_per_cluster[i].insert({cur_cluster_schema_names.back(), token_idx});
            }

            // Set catalog informations
            graph_cat->GetPropertyKeyIDs(*client.get(), cur_cluster_schema_names, cur_cluster_schema_types, property_key_ids);
            partition_cat->AddPropertySchema(*client.get(), property_schema_cats[i]->GetOid(), property_key_ids);
            property_schema_cats[i]->SetSchema(*client.get(), cur_cluster_schema_names,cur_cluster_schema_types, property_key_ids);
            property_schema_cats[i]->SetKeyColumnIdxs(per_cluster_key_column_idxs[i]);

            datas[i].Initialize(cur_cluster_schema_types, STORAGE_STANDARD_VECTOR_SIZE);
        }

// #ifdef DYNAMIC_SCHEMA_INSTANTIATION
        // if (num_clusters >= 1) {
        //     // create univ ps cat entry for dynamic schema instantiation
        //     vector<PropertyKeyID> property_key_ids;
        //     string property_schema_name = DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX + std::string(label_name) + "_univ";
        //     CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
        //     PropertySchemaCatalogEntry *univ_property_schema_cat = 
        //         (PropertySchemaCatalogEntry*) cat_instance->CreatePropertySchema(*client.get(), &propertyschema_info);
        //     graph_cat->GetPropertyKeyIDs(*client.get(), key_names, types, property_key_ids);
        //     univ_property_schema_cat->SetSchema(*client.get(), key_names, types, property_key_ids);
        //     univ_property_schema_cat->SetFake();
        //     partition_cat->SetUnivPropertySchema(univ_property_schema_cat->GetOid());
        // }
// #endif

        // Initialize LID_TO_PID_MAP
		if (load_edge) {
			lid_to_pid_map->emplace_back(label_name, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
			lid_to_pid_map_instance = &lid_to_pid_map->back().second;
			// lid_to_pid_map_instance->reserve(approximated_num_rows * 2);
		}

        // Iterate JSON file again & create extents
        vector<int64_t> num_tuples_per_cluster;
        num_tuples_per_cluster.resize(num_clusters, 0);
        int64_t num_tuples = 0;

        docs = parser.iterate_many(json); // TODO w/o parse?
        for (auto doc_ : docs) {
            uint64_t cluster_id = sg_to_cluster_vec[corresponding_schemaID[num_tuples]];
            D_ASSERT(cluster_id < num_clusters);

            for (auto i = 0; i < datas[cluster_id].size(); i++) {
                FlatVector::Validity(datas[cluster_id].data[i]).SetAllInvalid(datas[cluster_id].size());
            }
            recursive_iterate_jsonl(doc_["properties"], "", true, num_tuples_per_cluster[cluster_id], 0, cluster_id, datas[cluster_id]);
            
            if (++num_tuples_per_cluster[cluster_id] == STORAGE_STANDARD_VECTOR_SIZE) {
                // create extent
                datas[cluster_id].SetCardinality(num_tuples_per_cluster[cluster_id]);
                ExtentID new_eid = ext_mng->CreateExtent(*client.get(), datas[cluster_id], *partition_cat, *property_schema_cats[cluster_id]);
                property_schema_cats[cluster_id]->AddExtent(new_eid, datas[cluster_id].size());
                if (load_edge) StoreLidToPidInfo(datas[cluster_id], per_cluster_key_column_idxs[cluster_id], new_eid);
                num_tuples_per_cluster[cluster_id] = 0;
                datas[cluster_id].Reset(STORAGE_STANDARD_VECTOR_SIZE);
            }
            num_tuples++;
        }

        // Create extents for remaining datas
        for (size_t i = 0; i < num_clusters; i++) {
            datas[i].SetCardinality(num_tuples_per_cluster[i]);
            ExtentID new_eid = ext_mng->CreateExtent(*client.get(), datas[i], *partition_cat, *property_schema_cats[i]);
            property_schema_cats[i]->AddExtent(new_eid, datas[i].size());
            if (load_edge) StoreLidToPidInfo(datas[i], per_cluster_key_column_idxs[i], new_eid);
        }

        printf("# of documents = %ld\n", num_tuples);
        for (int i = 0; i < num_clusters; i++) {
            printf("cluster %d num_tuples: %ld\n", i, num_tuples_per_cluster[i]);
        }
    }

    void _CreateEdgeExtents(GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
    }

private:
    bool get_key_and_type(string &key_path, vector<string> &keys, vector<LogicalType> &types) {
        auto pos = key_path.rfind("_");
        string type_info = key_path.substr(pos + 1);
        auto aux_type_begin_pos = type_info.find("(");
        LogicalTypeId type_id;
        LogicalTypeId child_type_id;
        if (aux_type_begin_pos == string::npos) {
            type_id = static_cast<LogicalTypeId>((uint8_t)std::stoi(key_path.substr(pos + 1)));
        } else {
            auto aux_type_end_pos = type_info.find(")");
            type_id = static_cast<LogicalTypeId>((uint8_t)std::stoi(type_info.substr(0, aux_type_begin_pos)));
            child_type_id = static_cast<LogicalTypeId>((uint8_t)std::stoi(type_info.substr(aux_type_begin_pos + 1, aux_type_end_pos - aux_type_begin_pos - 1)));
        }

        keys.push_back(key_path.substr(0, pos));
        if (type_id == LogicalTypeId::LIST) {
            types.push_back(LogicalType::LIST(child_type_id));
        } else {
            if (keys.back() == NEO4J_VERTEX_ID_NAME) {
                types.push_back(LogicalType(LogicalTypeId::UBIGINT));
            } else {
                types.push_back(LogicalType(type_id));
            }
        }

        if (keys.back() == NEO4J_VERTEX_ID_NAME) return true;
        else return false;
    }

    void recursive_collect_key_paths_jsonl(ondemand::value element, std::string &current_prefix, bool in_array, vector<uint64_t> &schema, int current_idx) {
        switch (element.type()) {
        case ondemand::json_type::array: {
            // for (auto child : element.get_array()) {
            //     // We need the call to value() to get
            //     // an ondemand::value type.
            //     recursive_collect_key_paths_jsonl(child.value(), current_prefix, in_array, schema, current_idx);
            // }
            break;
        }
        case ondemand::json_type::object: {
            for (auto field : element.get_object()) {
                // key() returns the key as it appears in the raw
                // JSON document, if we want the unescaped key,
                // we should do field.unescaped_key().
                std::string old_prefix = current_prefix;
                std::string key = std::string(std::string_view(field.unescaped_key()));
                if (current_prefix == "") {
                    current_prefix = key;
                } else {
                    current_prefix = current_prefix + std::string("_") + key;
                }

                // Get field type
                switch (field.value().type()) {
                case ondemand::json_type::array: {
                    // Get child type
                    LogicalTypeId child_type_id = LogicalTypeId::INVALID;
                    for (auto child : field.value().get_array()) {
                        // We need the call to value() to get
                        // an ondemand::value type.
                        switch(child.value().type()) {
                        case ondemand::json_type::array:
                        case ondemand::json_type::object:
                            break;
                        case ondemand::json_type::number: {
                            ondemand::number_type t = child.value().get_number_type();
                            switch(t) {
                            case ondemand::number_type::signed_integer:
                                child_type_id = LogicalTypeId::BIGINT;
                                break;
                            case ondemand::number_type::unsigned_integer:
                                child_type_id = LogicalTypeId::UBIGINT;
                                break;
                            case ondemand::number_type::floating_point_number:
                                child_type_id = LogicalTypeId::DOUBLE;
                                break;
                            default:
                                break;
                            }
                            break;
                        }
                        case ondemand::json_type::string: {
                            child_type_id = LogicalTypeId::VARCHAR;
                            break;
                        }
                        case ondemand::json_type::boolean: {
                            child_type_id = LogicalTypeId::BOOLEAN;
                            break;
                        }
                        case ondemand::json_type::null: {
                            child_type_id = LogicalTypeId::SQLNULL;
                            break;
                        }
                        }
                        break; // see first element only
                    }
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::LIST)
                        + std::string("(") + std::to_string((uint8_t)child_type_id) + std::string(")");
                    break;
                }
                case ondemand::json_type::object: {
                    break;
                }
                case ondemand::json_type::number: {
                    ondemand::number_type t = field.value().get_number_type();
                    switch(t) {
                    case ondemand::number_type::signed_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BIGINT);
                        break;
                    case ondemand::number_type::unsigned_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::UBIGINT);
                        break;
                    case ondemand::number_type::floating_point_number:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::DOUBLE);
                        break;
                    }
                    break;
                }
                case ondemand::json_type::string: {
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::VARCHAR);
                    break;
                }
                case ondemand::json_type::boolean: {
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BOOLEAN);
                    break;
                }
                }

                //if (in_array && field.value().type() != ondemand::json_type::object) transactions[current_idx].emplace_back(current_prefix);
                if (field.value().type() != ondemand::json_type::object) { // TODO stop traversing if (child != (obj or arr))
                    uint64_t prop_id;
                    auto it = property_to_id_map.find(current_prefix);
                    if (it == property_to_id_map.end()) {
                        prop_id = GetNewPropertyID();
                        D_ASSERT(id_to_property_vec.size() == prop_id);
                        D_ASSERT(property_freq_vec.size() == prop_id);

                        property_to_id_map.insert({current_prefix, prop_id});
                        id_to_property_vec.push_back(current_prefix);
                        property_freq_vec.push_back(1);
                        // printf("New %s, %ld\n", current_prefix.c_str(), prop_id);
                    } else {
                        prop_id = it->second;
                        D_ASSERT(prop_id < property_freq_vec.size());
                        property_freq_vec[prop_id]++;
                        // printf("Find %s, %ld\n", current_prefix.c_str(), prop_id);
                    }
                    schema.push_back(prop_id);
                }
                recursive_collect_key_paths_jsonl(field.value(), current_prefix, in_array, schema, current_idx);
                current_prefix = old_prefix;
            }
            break;
        }
        case ondemand::json_type::number: {
            break;
        }
        case ondemand::json_type::string: {
            break;
        }
        case ondemand::json_type::boolean: {
            break;
        }
        case ondemand::json_type::null: {
            break;
        }
        }
    }

    bool recursive_collect_key_paths(ondemand::value element, std::string current_prefix, bool in_array, vector<vector<Item>> &transactions, int current_idx) {
        bool finished = false;
        bool count_num_tuples = !in_array;
        int num_tuples = 0;
        switch (element.type()) {
        case ondemand::json_type::array: {
            in_array = true;
            for (auto child : element.get_array()) {
                // We need the call to value() to get
                // an ondemand::value type.
                bool is_finished = recursive_collect_key_paths(child.value(), current_prefix, in_array, transactions, num_tuples);
                if (is_finished) {
                    finished = true;
                    break;
                }
                if (count_num_tuples) num_tuples++;
                if (num_tuples == TILE_SIZE) {
                    auto fpgrowth_start = std::chrono::high_resolution_clock::now();
                    // Run FpGrowth
                    std::cout << "\tRun FPGrowth for (" << num_tuples << ") tuples\n";
                    
                    const uint64_t minimum_support_threshold = 2;
                    const FPTree fptree{ transactions, minimum_support_threshold };
                    const std::set<Pattern> patterns = fptree_growth( fptree );
                    auto fpgrowth_end = std::chrono::high_resolution_clock::now();
                    fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

                    Pattern *current_mfp = nullptr;
                    // std::cout << "Pattern Size = " << patterns.size() << std::endl;
                    for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
                        // std::cout << "Set Size = " << it->first.size() << " ";
                        // std::cout << "({";
                        bool print_comma = false;
                        if (current_mfp == nullptr) {
                            current_mfp = new Pattern;
                            current_mfp->first = it->first;
                            current_mfp->second = it->second;
                        } else {
                            if (current_mfp->first.size() < it->first.size() && it->second >= (num_tuples * FREQUENCY_THRESHOLD)) {
                                current_mfp->first = it->first;
                                current_mfp->second = it->second;
                            }
                        }
                        // for (std::set<Item>::iterator item_it = it->first.begin(); item_it != it->first.end(); item_it++) {
                        //     if (print_comma) std::cout << ",";
                        //     std::cout << *item_it;
                        //     print_comma = true;
                        // }
                        // std::cout << "}, " << it->second << ")\n";
                    }
                    most_frequency_pattern.first = current_mfp->first;
                    most_frequency_pattern.second = current_mfp->second;

                    // Clear Transactions
                    for (int i = 0; i < TILE_SIZE; i++) transactions[i].clear();
                    num_tuples = 0;
                    finished = true;
                    break;
                }
            }

            if (num_tuples > 0) {
                auto fpgrowth_start = std::chrono::high_resolution_clock::now();
                // Run FpGrowth for remaining
                std::cout << "\tRun FPGrowth for (" << num_tuples << ") tuples\n";

                const uint64_t minimum_support_threshold = 2;
                const FPTree fptree{ transactions, minimum_support_threshold };
                const std::set<Pattern> patterns = fptree_growth( fptree );
                auto fpgrowth_end = std::chrono::high_resolution_clock::now();
                fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

                Pattern *current_mfp = nullptr;
                //std::cout << "Pattern Size = " << patterns.size() << std::endl;
                for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
                    //std::cout << "Set Size = " << it->first.size() << " ";
                    //std::cout << "({";
                    bool print_comma = false;
                    if (current_mfp == nullptr) {
                        current_mfp = new Pattern;
                        current_mfp->first = it->first;
                        current_mfp->second = it->second;
                    } else {
                        if (current_mfp->first.size() < it->first.size() && it->second >= (num_tuples * FREQUENCY_THRESHOLD)) {
                            current_mfp->first = it->first;
                            current_mfp->second = it->second;
                        }
                    }
                    //for (std::set<Item>::iterator item_it = it->first.begin(); item_it != it->first.end(); item_it++) {
                        // if (print_comma) std::cout << ",";
                    //     std::cout << *item_it;
                    //     print_comma = true;
                    // }
                    // std::cout << "}, " << it->second << ")\n";

                    most_frequency_pattern.first = current_mfp->first;
                    most_frequency_pattern.second = current_mfp->second;
                }

                // Clear Transactions
                for (int i = 0; i < TILE_SIZE; i++) transactions[i].clear();
                finished = true;
            }
            in_array = false;
            break;
        }
        case ondemand::json_type::object: {
            for (auto field : element.get_object()) {
                // key() returns the key as it appears in the raw
                // JSON document, if we want the unescaped key,
                // we should do field.unescaped_key().
                std::string old_prefix = current_prefix;
                std::string key = std::string(std::string_view(field.unescaped_key()));
                if (current_prefix == "") {
                    current_prefix = key;
                } else {
                    current_prefix = current_prefix + std::string("_") + key;
                }
                switch (field.value().type()) {
                case ondemand::json_type::array: {
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::LIST);
                    break;
                }
                case ondemand::json_type::object: {
                    //current_prefix = current_prefix + "obj";
                    break;
                }
                case ondemand::json_type::number: {
                    ondemand::number_type t = field.value().get_number_type();
                    switch(t) {
                    case ondemand::number_type::signed_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BIGINT);
                        break;
                    case ondemand::number_type::unsigned_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::UBIGINT);
                        break;
                    case ondemand::number_type::floating_point_number:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::DOUBLE);
                        break;
                    default:
                        std::cerr << t << std::endl;
                        throw InvalidInputException("?");
                    }
                    break;
                }
                case ondemand::json_type::string: {
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::VARCHAR);
                    break;
                }
                case ondemand::json_type::boolean: {
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BOOLEAN);
                    break;
                }
                }
                // std::cout << "\"" << key << "/" << current_prefix << "\": " << std::endl;;
                //if (in_array && field.value().type() != ondemand::json_type::object) transactions[current_idx].emplace_back(current_prefix);
                if (field.value().type() != ondemand::json_type::object) transactions[current_idx].emplace_back(current_prefix);
                bool is_finished = recursive_collect_key_paths(field.value(), current_prefix, in_array, transactions, current_idx);
                current_prefix = old_prefix;
                if (is_finished) {
                    finished = true;
                    break;
                }
            }
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            // std::cout << element.get_double();
            break;
        }
        case ondemand::json_type::string: {
            // get_string() would return escaped string, but
            // we are happy with unescaped string.
            // std::cout << "\"" << element.get_raw_json_string() << "\"";
            break;
        }
        case ondemand::json_type::boolean: {
            // std::cout << element.get_bool();
            break;
        }
        case ondemand::json_type::null: {
            // We check that the value is indeed null
            // otherwise: an error is thrown.
            // if(element.is_null()) {
            //   std::cout << "null";
            // }
            break;
        }
        }
        return finished;
    }

    void recursive_iterate_jsonl(ondemand::value element, std::string current_prefix, bool in_array, int current_idx, int current_col_idx, uint64_t cluster_id, DataChunk &data) {
        switch (element.type()) {
        case ondemand::json_type::array: {
            vector<Value> val_vectors;
            // val_vectors.reserve(4);
            for (auto child : element.get_array()) {
                // We need the call to value() to get
                // an ondemand::value type.
                switch(child.value().type()) {
                case ondemand::json_type::array:
                case ondemand::json_type::object:
                    recursive_iterate_jsonl(child.value(), current_prefix, in_array, current_idx, current_col_idx, cluster_id, data);
                    break;
                case ondemand::json_type::number: {
                    ondemand::number_type t = child.value().get_number_type();
                    switch(t) {
                    case ondemand::number_type::signed_integer: {
                        const Value int_val = Value::BIGINT(child.value().get_int64().value());
                        // icecream::ic.enable(); IC(val_vectors.size()); icecream::ic.disable();
                        // fprintf(stderr, "val_vectors ptr %p\n", val_vectors.data());
                        val_vectors.push_back(int_val);
                        break;
                    }
                    case ondemand::number_type::unsigned_integer:
                        val_vectors.push_back(Value::UBIGINT(child.value().get_uint64().value()));
                        break;
                    case ondemand::number_type::floating_point_number:
                        val_vectors.push_back(Value::DOUBLE(child.value().get_double().value()));
                        break;
                    default: {
                        break;
                    }
                    }
                    break;
                }
                case ondemand::json_type::string: {
                    std::string_view string_val = child.value().get_string();
                    val_vectors.push_back(Value(std::string(string_val)));
                    break;
                }
                case ondemand::json_type::boolean: {
                    break;
                }
                case ondemand::json_type::null: {
                    break;
                }
                }
            }
            // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx, val_vectors.size()); icecream::ic.disable();
            Value list_val = Value::LIST(val_vectors);
            data.SetValue(current_col_idx, current_idx, list_val);
            // val_vectors.clear();
            // vector<Value>().swap(val_vectors);
            in_array = false;
            break;
        }
        case ondemand::json_type::object: {
            for (auto field : element.get_object()) {
                // key() returns the key as it appears in the raw
                // JSON document, if we want the unescaped key,
                // we should do field.unescaped_key().
                std::string old_prefix = current_prefix;
                std::string key = std::string(std::string_view(field.unescaped_key()));
                if (current_prefix == "") {
                    current_prefix = key;
                } else {
                    current_prefix = current_prefix + std::string("_") + key;
                }
                // std::cout << "\"" << key << "/" << current_prefix << "\": ";
                auto key_idx = property_to_id_map_per_cluster[cluster_id].at(current_prefix);
                recursive_iterate_jsonl(field.value(), current_prefix, in_array, current_idx, key_idx, cluster_id, data);
                current_prefix = old_prefix;
            }
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            // std::cout << element.get_double();
            ondemand::number_type t = element.get_number_type();
            switch(t) {
            case ondemand::number_type::signed_integer: {
                int64_t *column_ptr = (int64_t *)data.data[current_col_idx].GetData();
                column_ptr[current_idx] = element.get_int64();
                FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
                break;
            }
            case ondemand::number_type::unsigned_integer: {
                uint64_t *column_ptr = (uint64_t *)data.data[current_col_idx].GetData();
                column_ptr[current_idx] = element.get_uint64();
                FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
                break;
            }
            case ondemand::number_type::floating_point_number: {
                double *column_ptr = (double *)data.data[current_col_idx].GetData();
                column_ptr[current_idx] = element.get_double();
                FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
                break;
            }
            }
            break;
        }
        case ondemand::json_type::string: {
            // get_string() would return escaped string, but
            // we are happy with unescaped string.
            std::string_view string_val = element.get_string();
            auto data_ptr = data.data[current_col_idx].GetData();
            // std::cout << "\"" << element.get_string() << "\"";
            // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx); icecream::ic.disable();
            ((string_t *)data_ptr)[current_idx] = StringVector::AddStringOrBlob(data.data[current_col_idx], 
                                                    (const char*)string_val.data(), string_val.size());
            FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
            break;
        }
        case ondemand::json_type::boolean: {
            // std::cout << element.get_bool();
            break;
        }
        case ondemand::json_type::null: {
            // We check that the value is indeed null
            // otherwise: an error is thrown.
            // if(element.is_null()) {
            //   std::cout << "null";
            // }
            break;
        }
        }
    }

    void recursive_iterate_json(ondemand::value element, std::string current_prefix, bool in_array, int current_idx, int current_col_idx, DataChunk &data) { // TODO remove transactions & current_idx    
        D_ASSERT(false);
        bool count_num_tuples = !in_array;
        int num_tuples = 0;
        switch (element.type()) {
        case ondemand::json_type::array: {
            bool old_in_array = in_array;
            in_array = true;
            vector<Value> val_vectors;
            // val_vectors.reserve(4);
            for (auto child : element.get_array()) {
                // We need the call to value() to get
                // an ondemand::value type.
                switch(child.value().type()) {
                case ondemand::json_type::array:
                    old_in_array = false;
                    recursive_iterate_json(child.value(), current_prefix, in_array, current_idx, current_col_idx, data);
                    break;
                case ondemand::json_type::object:
                    old_in_array = false;
                    recursive_iterate_json(child.value(), current_prefix, in_array, num_tuples, current_col_idx, data);
                    break;
                case ondemand::json_type::number: {
                    ondemand::number_type t = child.value().get_number_type();
                    switch(t) {
                    case ondemand::number_type::signed_integer: {
                        const Value int_val = Value::BIGINT(child.value().get_int64().value());
                        // fprintf(stderr, "val_vectors ptr %p\n", val_vectors.data());
                        val_vectors.push_back(int_val);
                        break;
                    }
                    case ondemand::number_type::unsigned_integer:
                        val_vectors.push_back(Value::UBIGINT(child.value().get_uint64().value()));
                        break;
                    case ondemand::number_type::floating_point_number:
                        val_vectors.push_back(Value::DOUBLE(child.value().get_double().value()));
                        break;
                    default: {
                        break;
                    }
                    }
                    break;
                }
                case ondemand::json_type::string: {
                    break;
                }
                case ondemand::json_type::boolean: {
                    break;
                }
                case ondemand::json_type::null: {
                    break;
                }
                }
                if (count_num_tuples) {
                    num_tuples++;
                    if (num_tuples == STORAGE_STANDARD_VECTOR_SIZE) {
                        // CreateExtent
                        fprintf(stderr, "CreateExtent %d\n", num_tuples);
                        data.SetCardinality(num_tuples);
                        ExtentID new_eid;
                        // ExtentID new_eid = ext_mng->CreateExtent(*client.get(), data, *property_schema_cat); // TODO
                        property_schema_cat->AddExtent(new_eid);

                        if (load_edge) {
                            // Initialize pid base
                            idx_t pid_base = (idx_t) new_eid;
                            pid_base = pid_base << 32;

                            // Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
                            auto map_build_start = std::chrono::high_resolution_clock::now();
                            if (key_column_idxs.size() == 0) {
                            } else if (key_column_idxs.size() == 1) {
                                LidPair lid_key;
                                lid_key.second = 0;
                                idx_t* key_column = (idx_t*) data.data[key_column_idxs[0]].GetData();
                                
                                for (idx_t seqno = 0; seqno < data.size(); seqno++) {
                                    lid_key.first = key_column[seqno];
                                    lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
                                }
                            } else if (key_column_idxs.size() == 2) {
                                LidPair lid_key;
                                idx_t* key_column_1 = (idx_t*) data.data[key_column_idxs[0]].GetData();
                                idx_t* key_column_2 = (idx_t*) data.data[key_column_idxs[1]].GetData();
                                
                                for (idx_t seqno = 0; seqno < data.size(); seqno++) {
                                    lid_key.first = key_column_1[seqno];
                                    lid_key.second = key_column_2[seqno];
                                    lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
                                }
                            } else {
                                throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                            }
                            
                            auto map_build_end = std::chrono::high_resolution_clock::now();
                            std::chrono::duration<double> map_build_duration = map_build_end - map_build_start;
                            fprintf(stdout, "Map Build Elapsed: %.3f\n", map_build_duration.count());

                        }
                        num_tuples = 0;
                        data.Reset(STORAGE_STANDARD_VECTOR_SIZE);
                    }
                }
            }
            if (old_in_array) {
                // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx, val_vectors.size()); icecream::ic.disable();
                data.SetValue(current_col_idx, current_idx, Value::LIST(val_vectors));
            }

            in_array = false;
            if (num_tuples > 0) {
                fprintf(stderr, "CreateExtent %d\n", num_tuples);
                // CreateExtent
                data.SetCardinality(num_tuples);
                ExtentID new_eid;
                // ExtentID new_eid = ext_mng->CreateExtent(*client.get(), data, *property_schema_cat); // TODO
                property_schema_cat->AddExtent(new_eid);
                if (load_edge) {
                    // Initialize pid base
                    idx_t pid_base = (idx_t) new_eid;
                    pid_base = pid_base << 32;

                    // Build Logical id To Physical id Mapping (= LID_TO_PID_MAP)
                    auto map_build_start = std::chrono::high_resolution_clock::now();
                    if (key_column_idxs.size() == 0) {
                    } else if (key_column_idxs.size() == 1) {
                        LidPair lid_key;
                        lid_key.second = 0;
                        idx_t* key_column = (idx_t*) data.data[key_column_idxs[0]].GetData();
                        
                        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
                            lid_key.first = key_column[seqno];
                            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
                        }
                    } else if (key_column_idxs.size() == 2) {
                        LidPair lid_key;
                        idx_t* key_column_1 = (idx_t*) data.data[key_column_idxs[0]].GetData();
                        idx_t* key_column_2 = (idx_t*) data.data[key_column_idxs[1]].GetData();
                        
                        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
                            lid_key.first = key_column_1[seqno];
                            lid_key.second = key_column_2[seqno];
                            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
                        }
                    } else {
                        throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                    }
                    
                    auto map_build_end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> map_build_duration = map_build_end - map_build_start;
                    fprintf(stdout, "Map Build Elapsed: %.3f\n", map_build_duration.count());

                }
                num_tuples = 0;
                data.Reset(STORAGE_STANDARD_VECTOR_SIZE);
            }
            break;
        }
        case ondemand::json_type::object: {
            for (auto field : element.get_object()) {
                // key() returns the key as it appears in the raw
                // JSON document, if we want the unescaped key,
                // we should do field.unescaped_key().
                std::string old_prefix = current_prefix;
                std::string key = std::string(std::string_view(field.unescaped_key()));
                if (current_prefix == "") {
                    current_prefix = key;
                } else {
                    current_prefix = current_prefix + std::string("_") + key;
                }
                // std::cout << "\"" << key << "/" << current_prefix << "\": ";
                auto key_it = std::find(most_common_key_paths.begin(), most_common_key_paths.end(), current_prefix);
                if (key_it == most_common_key_paths.end()) {
                    // Store this in the RowChunk
                    // std::cout << "?? " << current_prefix << std::endl;
                }
                int key_idx = std::distance(most_common_key_paths.begin(), key_it);
                recursive_iterate_json(field.value(), current_prefix, in_array, current_idx, key_idx, data);
                current_prefix = old_prefix;
            }
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            // std::cout << element.get_double();
            ondemand::number_type t = element.get_number_type();
            switch(t) {
            case ondemand::number_type::signed_integer:
                if (most_common_schema[current_col_idx] == LogicalType::BIGINT) {
                    int64_t *column_ptr = (int64_t *)data.data[current_col_idx].GetData();
                    // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx, element.get_int64()); icecream::ic.disable();
                    column_ptr[current_idx] = element.get_int64();
                } else {
                    // Store value in the RowChunk
                }
                break;
            case ondemand::number_type::unsigned_integer:
                if (most_common_schema[current_col_idx] == LogicalType::UBIGINT) {
                    uint64_t *column_ptr = (uint64_t *)data.data[current_col_idx].GetData();
                    // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx); icecream::ic.disable();
                    column_ptr[current_idx] = element.get_uint64();
                } else {
                    // Store value in the RowChunk
                }
                break;
            case ondemand::number_type::floating_point_number:
                if (most_common_schema[current_col_idx] == LogicalType::DOUBLE) {
                    double *column_ptr = (double *)data.data[current_col_idx].GetData();
                    // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx); icecream::ic.disable();
                    column_ptr[current_idx] = element.get_double();
                } else {
                    // Store value in the RowChunk
                }
                break;
            }
            break;
        }
        case ondemand::json_type::string: {
            // get_string() would return escaped string, but
            // we are happy with unescaped string.
            std::string_view string_val = element.get_string();
            auto data_ptr = data.data[current_col_idx].GetData();
            // std::cout << "\"" << element.get_string() << "\"";
            // icecream::ic.enable(); IC(); IC(current_col_idx, current_idx); icecream::ic.disable();
            ((string_t *)data_ptr)[current_idx] = StringVector::AddStringOrBlob(data.data[current_col_idx], 
                                                    (const char*)string_val.data(), string_val.size());
            break;
        }
        case ondemand::json_type::boolean: {
            // std::cout << element.get_bool();
            break;
        }
        case ondemand::json_type::null: {
            // We check that the value is indeed null
            // otherwise: an error is thrown.
            // if(element.is_null()) {
            //   std::cout << "null";
            // }
            break;
        }
        }
    }

    void recursive_print_json(ondemand::value element, std::string current_prefix, bool in_array) {
        bool add_comma;
        bool count_num_tuples = !in_array;
        int num_tuples = 0;
        switch (element.type()) {
        case ondemand::json_type::array: {
            in_array = true;
            std::cout << "[";
            add_comma = false;
            for (auto child : element.get_array()) {
                if (add_comma) {
                    std::cout << ",";
                }
                // We need the call to value() to get
                // an ondemand::value type.
                recursive_print_json(child.value(), current_prefix, in_array);
                if (count_num_tuples) num_tuples++;
                
                add_comma = true;
            }
            std::cout << "]";

            in_array = false;
            break;
        }
        case ondemand::json_type::object: {
            std::cout << "{";
            add_comma = false;
            for (auto field : element.get_object()) {
                if (add_comma) {
                    std::cout << ",";
                }
                // key() returns the key as it appears in the raw
                // JSON document, if we want the unescaped key,
                // we should do field.unescaped_key().
                std::string old_prefix = current_prefix;
                std::string key = std::string(std::string_view(field.unescaped_key()));
                if (current_prefix == "") {
                    current_prefix = key;
                } else {
                    current_prefix = current_prefix + std::string("_") + key;
                }
                std::cout << "\"" << key << "\": ";
                // std::cout << "\"" << key << "/" << current_prefix << "\": ";
                recursive_print_json(field.value(), current_prefix, in_array);
                current_prefix = old_prefix;
                add_comma = true;
            }
            std::cout << "}\n";
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            std::cout << element.get_double();
            break;
        }
        case ondemand::json_type::string: {
            // get_string() would return escaped string, but
            // we are happy with unescaped string.
            std::cout << "\"" << element.get_raw_json_string() << "\"";
            break;
        }
        case ondemand::json_type::boolean: {
            std::cout << element.get_bool();
            break;
        }
        case ondemand::json_type::null: {
            // We check that the value is indeed null
            // otherwise: an error is thrown.
            if(element.is_null()) {
                std::cout << "null";
            }
            break;
        }
        }
    }

    ondemand::parser parser;
    ondemand::document doc;
    ondemand::document_stream docs;
    ondemand::value val;
    simdjson::padded_string json;

    vector<vector<Item>> transactions;

    vector<uint64_t> corresponding_schemaID;
    vector<vector<uint64_t>> schema_groups;
    vector<int32_t> sg_to_cluster_vec;
    vector<string> id_to_property_vec;
    vector<uint64_t> property_freq_vec;
    vector<uint64_t> schema_property_freq_vec;
    vector<uint64_t> order;
    unordered_map<string, uint64_t> property_to_id_map;
    vector<unordered_map<string, uint64_t>> property_to_id_map_per_cluster;
    uint64_t propertyIDver = 0;
    SchemaHashTable sch_HT;
    Algorithm *cluster_algo;
    size_t num_clusters;

    vector<LogicalType> most_common_schema;
    vector<string> most_common_key_paths;
    Pattern most_frequency_pattern;
    ExtentManager *ext_mng;
    std::shared_ptr<ClientContext> client;
    Catalog *cat_instance;
    PropertySchemaCatalogEntry *property_schema_cat;
    vector<PropertySchemaCatalogEntry *> property_schema_cats;
    bool load_edge = false;
    vector<idx_t> key_column_idxs;
    unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_to_pid_map_instance;
    vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> *lid_to_pid_map;
};

} // namespace duckdb