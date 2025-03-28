#pragma once

#include <iostream>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <unordered_set>
#include <Python.h>
#include <simdjson.h>

#include "icecream.hpp"
#include "common/logger.hpp"
#include "common/vector.hpp"
#include "common/clustering/dbscan.h"
#include "common/clustering/optics.hpp"
#include "storage/schemaless/schema_hash_table.hpp"

using namespace simdjson;

#define TILE_SIZE 1024 // or 4096
#define NEO4J_VERTEX_ID_NAME "id"
#define COST_MAX 10000000000.00
#define COST_MIN 0
#define MAX_THREADS 32

// Thresholds
#define FREQUENCY_THRESHOLD 0.95
#define SET_SIM_THRESHOLD 0.99
#define SET_EDIT_THRESHOLD 2
#define JACCARD_THRESHOLD 1
#define WEIGHTEDJACCARD_THRESHOLD 0.3
#define COSINE_THRESHOLD 0.5
#define DICE_THRESHOLD 0.4
#define OVERLAP_THRESHOLD 0.9
#define VEC_OVHD_THRESHOLD 1024
#define MERGE_THRESHOLD 0.15

namespace duckdb {

#ifndef LIDPAIR
#define LIDPAIR
typedef std::pair<idx_t, idx_t> LidPair;
#endif

typedef uint64_t NumTuples;
typedef uint32_t PropertyID;
typedef vector<std::pair<vector<PropertyID>, NumTuples>> SchemaGroups;

const uint32_t MERGED_SCHEMA = std::numeric_limits<uint32_t>::max();

class GraphSIMDJSONFileParser {

/** CONFIGURATIONS **/
public:
    enum class ClusterAlgorithmType {
        DBSCAN,
        OPTICS,
        AGGLOMERATIVE,
        GMM,
        PGSE,
        SINGLECLUSTER,
        SEPERATECLUSTERS
    };

    enum class CostModel {
        OURS,
        SETEDIT,
        JACCARD,
        WEIGHTEDJACCARD,
        COSINE,
        DICE,
        OVERLAP
    };
    
    enum class LayeringOrder {
        ASCENDING,
        DESCENDING,
        NO_SORT
    };

    enum class MergeInAdvance {
        IN_STORAGE,
        IN_QUERY_TIME,
    };

    const ClusterAlgorithmType cluster_algo_type = ClusterAlgorithmType::AGGLOMERATIVE;
    const CostModel cost_model = CostModel::OURS;
    const LayeringOrder layering_order = LayeringOrder::DESCENDING;
    const MergeInAdvance merge_in_advance = MergeInAdvance::IN_QUERY_TIME;
/*******************/


public:
    GraphSIMDJSONFileParser() {}
    ~GraphSIMDJSONFileParser() {
    }

    GraphSIMDJSONFileParser(std::shared_ptr<ClientContext> client_, ExtentManager *ext_mng_, Catalog *cat_instance_) {
        client = client_;
        ext_mng = ext_mng_;
        cat_instance = cat_instance_;

        sch_HT.resize(1000); // TODO appropriate size

        spdlog::info("[GraphSIMDJSONFileParser] CostSchemaVal: {}", CostSchemaVal);
        spdlog::info("[GraphSIMDJSONFileParser] CostNullVal: {}", CostNullVal);
        spdlog::info("[GraphSIMDJSONFileParser] CostVectorizationVal: {}", CostVectorizationVal);
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

    size_t InitJsonFile(const char *json_file_path) {
        input_json_file_path = std::string(json_file_path);
        json = padded_string::load(input_json_file_path);
        docs = parser.iterate_many(json);
        return 0;
    }

    void LoadJson(string &label_name, vector<string> &label_set, const char *json_key, DataChunk &data, GraphCatalogEntry *graph_cat, PartitionCatalogEntry *partition_cat, GraphComponentType gctype = GraphComponentType::INVALID) {
        boost::timer::cpu_timer clustering_timer;
        switch (cluster_algo_type) {
        case ClusterAlgorithmType::DBSCAN: {
            // Extract Schema (bag semantic) & Preprocessing (calculate distance matrix)
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterSchemaDBScan();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::OPTICS: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterSchemaOptics();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::AGGLOMERATIVE: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterSchemaAgglomerative();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::PGSE: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterSchemaPGSE();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::GMM: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterSchemaGMM();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::SINGLECLUSTER: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterAllSchemas();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        case ClusterAlgorithmType::SEPERATECLUSTERS: {
            _ExtractSchema(gctype);
            _PreprocessSchemaForClustering(false);

            clustering_timer.start();
            // Clustering
            _ClusterEachSchemaSeparately();
            clustering_timer.stop();

            // Create Extents
            _CreateExtents(gctype, graph_cat, label_name, label_set);
            break;
        }
        default:
            break;
        }
        auto cluster_time_ms = clustering_timer.elapsed().wall / 1000000.0;
        std::cout << "\nCluster Time: "  << cluster_time_ms << " ms" << std::endl;
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
                std::vector<uint32_t> tmp_vec;

                string current_prefix = "";
                recursive_collect_key_paths_jsonl(doc_["properties"], current_prefix, true, tmp_vec, num_tuples);

                int64_t schema_id;
                sch_HT.find(tmp_vec, schema_id);
                // for (auto i = 0; i < tmp_vec.size(); i++) {
                //     fprintf(stdout, "%ld ", tmp_vec[i]);
                // }
                // fprintf(stdout, ": %ld\n", schema_id);
                if (schema_id == INVALID_TUPLE_GROUP_ID) { // not found
                    schema_id = schema_groups_with_num_tuples.size();
                    sch_HT.insert(tmp_vec, schema_id);
                    schema_groups_with_num_tuples.push_back(std::make_pair(std::move(tmp_vec), 1));
                    corresponding_schemaID.push_back(schema_id);
                } else {
                    corresponding_schemaID.push_back(schema_id);
                    schema_groups_with_num_tuples[schema_id].second++;
                }
                num_tuples++;
            }
            schema_property_freq_vec.resize(property_freq_vec.size(), 0);
            for (size_t i = 0; i < schema_groups_with_num_tuples.size(); i++) {
                for (size_t j = 0; j < schema_groups_with_num_tuples[i].first.size(); j++) {
                    schema_property_freq_vec[schema_groups_with_num_tuples[i].first[j]]++;
                }
            }
            printf("schema_groups.size = %ld\n", schema_groups_with_num_tuples.size());

            for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
                fprintf(stdout, "Schema %d: ", i);
                for (auto j = 0; j < schema_groups_with_num_tuples[i].first.size(); j++) {
                    fprintf(stdout, "%d ", schema_groups_with_num_tuples[i].first[j]);
                }
                fprintf(stdout, ": %ld\n", schema_groups_with_num_tuples[i].second);
            }

            return;
        } else if (gctype == GraphComponentType::EDGE) {
            D_ASSERT(false); // not implemented yet
            for (auto doc_ : docs) {
                std::string_view type = doc_["type"].get_string();

                ondemand::value labels = doc_["labels"];
                D_ASSERT(labels.type() == ondemand::json_type::array);
                // recursive_print_json(doc_["properties"], "", false);

                // Get Scheam from samples
            }
        }
    }

    void _PreprocessSchemaForClustering(bool use_setsim_algo = true) {
        // Sort tokens by frequency
        // printf("Schema Freq vec\n");
        // for (int i = 0; i < schema_property_freq_vec.size(); i++) {
        //     printf("%d-th (%s) freq: %ld, ", i, id_to_property_vec[i].c_str(), schema_property_freq_vec[i]);
        // }
        // printf("\n");
        order.resize(schema_property_freq_vec.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return schema_property_freq_vec[a] < schema_property_freq_vec[b];
        });

        if (!use_setsim_algo) return;
    }

    void _GenerateDistanceMatrix() {
        // distance_matrix = Eigen::MatrixXd::Zero(schema_groups.size(), schema_groups.size());
        
        // for (auto i = 0; i < schema_groups.size(); i++) {
        //     std::sort(schema_groups[i].begin(), schema_groups[i].end());
        // }

        // double min_distance = std::numeric_limits<double>::max();
        // for (auto i = 0; i < schema_groups.size(); i++) {
        //     for (auto j = i + 1; j < schema_groups.size(); j++) {
        //         double distance = _ComputeDistance(i, j);
        //         distance_matrix(i, j) = distance;
        //         distance_matrix(j, i) = distance;

        //         if (distance < min_distance) {
        //             min_distance = distance;
        //         }
        //     }
        // }

        // if (min_distance < 0) {
        //     min_distance = -min_distance;
        //     for (auto i = 0; i < schema_groups.size(); i++) {
        //         for (auto j = i + 1; j < schema_groups.size(); j++) {
        //             distance_matrix(i, j) += (min_distance + 1.0);
        //             distance_matrix(j, i) += (min_distance + 1.0);
        //         }
        //     }
        // }

        // std::cout << "Generate Distance Matrix Done!\n";
        // std::cout << distance_matrix << std::endl;
    }

    double _ComputeDistance(size_t rowid1, size_t rowid2) {
        double cost_schema = -CostSchemaVal;
        double cost_null = CostNullVal;
        double cost_vectorization = CostVectorizationVal;

        int64_t num_nulls1 = 0;
        int64_t num_nulls2 = 0;
        idx_t i = 0;
        idx_t j = 0;
        while (i < schema_groups_with_num_tuples[rowid1].first.size() && j < schema_groups_with_num_tuples[rowid2].first.size()) {
            if (schema_groups_with_num_tuples[rowid1].first[i] == schema_groups_with_num_tuples[rowid2].first[j]) {
                i++;
                j++;
            } else if (schema_groups_with_num_tuples[rowid1].first[i] < schema_groups_with_num_tuples[rowid2].first[j]) {
                num_nulls1++;
                i++;
            } else {
                num_nulls2++;
                j++;
            }
        }
        while (i < schema_groups_with_num_tuples[rowid1].first.size()) {
            num_nulls1++;
            i++;
        }
        while (j < schema_groups_with_num_tuples[rowid2].first.size()) {
            num_nulls2++;
            j++;
        }

        cost_null *= (num_nulls1 * schema_groups_with_num_tuples[rowid1].second + num_nulls2 * schema_groups_with_num_tuples[rowid2].second);
        if (schema_groups_with_num_tuples[rowid1].second < VEC_OVHD_THRESHOLD ||
            schema_groups_with_num_tuples[rowid2].second < VEC_OVHD_THRESHOLD) {
            cost_vectorization *=
                (_ComputeVecOvh(schema_groups_with_num_tuples[rowid1].second +
                                schema_groups_with_num_tuples[rowid2].second) -
                 _ComputeVecOvh(schema_groups_with_num_tuples[rowid1].second) -
                 _ComputeVecOvh(schema_groups_with_num_tuples[rowid2].second));
        }
        else {
            cost_vectorization = 0;
        }

        double distance = cost_schema + cost_null + cost_vectorization;
        return distance;
    }

    double _ComputeCostMergingSchemaGroups(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        double cost_schema = -CostSchemaVal;
        double cost_null = CostNullVal;
        double cost_vectorization = CostVectorizationVal;

        int64_t num_nulls1 = 0;
        int64_t num_nulls2 = 0;
        idx_t i = 0;
        idx_t j = 0;
        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                num_nulls1++;
                i++;
            } else {
                num_nulls2++;
                j++;
            }
        }
        while (i < schema_group1.first.size()) {
            num_nulls1++;
            i++;
        }
        while (j < schema_group2.first.size()) {
            num_nulls2++;
            j++;
        }

        cost_null *= (num_nulls1 * schema_group2.second + num_nulls2 * schema_group1.second);
        cost_vectorization *= (_ComputeVecOvh(schema_group1.second + schema_group2.second)
            - _ComputeVecOvh(schema_group1.second) - _ComputeVecOvh(schema_group2.second));

        double distance = cost_schema + cost_null + cost_vectorization;

        return distance;
    }

    double _ComputeCostMergingSchemaSetEdit(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        int64_t num_nulls1 = 0;
        int64_t num_nulls2 = 0;
        idx_t i = 0;
        idx_t j = 0;
        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                num_nulls1++;
                i++;
            } else {
                num_nulls2++;
                j++;
            }
        }
        while (i < schema_group1.first.size()) {
            num_nulls1++;
            i++;
        }
        while (j < schema_group2.first.size()) {
            num_nulls2++;
            j++;
        }

        double distance = num_nulls1 + num_nulls2;
        return distance;
    }

    double _ComputeCostMergingSchemaGroupsJaccard(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        int64_t num_nulls1 = 0;
        int64_t num_nulls2 = 0;
        idx_t i = 0;
        idx_t j = 0;
        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                num_nulls1++;
                i++;
            } else {
                num_nulls2++;
                j++;
            }
        }
        while (i < schema_group1.first.size()) {
            num_nulls1++;
            i++;
        }
        while (j < schema_group2.first.size()) {
            num_nulls2++;
            j++;
        }

        int64_t num_common = (schema_group1.first.size() + schema_group2.first.size() - num_nulls1 - num_nulls2) / 2;

        double distance = num_common / (double) (schema_group1.first.size() + schema_group2.first.size() - num_common);
        return distance;
    }

    double _ComputeCostMergingSchemaGroupsWeightedJaccard(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        // Step 0: Get maximum property id
        uint32_t max_property_id = schema_group1.first.back();
        if (schema_group2.first.back() > max_property_id) {
            max_property_id = schema_group2.first.back();
        }

        // Step 1: Calculate frequencies
        vector<uint64_t> property_frequencies(max_property_id + 1, 0); // Is it + 1 or not? 
        for (const auto &property : schema_group1.first) {
            property_frequencies[property] += schema_group1.second;
        }
        for (const auto &property : schema_group2.first) {
            property_frequencies[property] += schema_group2.second;
        }

        // Step 2: Calculate weights
        vector<double> weights(max_property_id + 1, 0.0);
        for (int i = 0; i < property_frequencies.size(); i++) {
            weights[i] = 1.0 / sqrt(static_cast<double>(property_frequencies[i]));
        }

        // Step 3: Compute weighted Jaccard similarity
        double intersection_weight = 0.0;
        double union_weight = 0.0;

        idx_t i = 0, j = 0;
        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                intersection_weight += weights[schema_group1.first[i]];
                union_weight += weights[schema_group1.first[i]];
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                union_weight += weights[schema_group1.first[i]];
                i++;
            } else {
                union_weight += weights[schema_group2.first[j]];
                j++;
            }
        }

        // Handle remaining elements
        while (i < schema_group1.first.size()) {
            union_weight += weights[schema_group1.first[i]];
            i++;
        }
        while (j < schema_group2.first.size()) {
            union_weight += weights[schema_group2.first[j]];
            j++;
        }

        // Calculate and return the weighted Jaccard similarity
        double WEIGHTEDJACCARD = intersection_weight / union_weight;
        return WEIGHTEDJACCARD;
    }

    double _ComputeCostMergingSchemaGroupsCosine(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        double dot_product = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        idx_t i = 0;
        idx_t j = 0;

        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                dot_product += schema_group1.first[i] * schema_group2.first[j];
                magnitude1 += schema_group1.first[i] * schema_group1.first[i];
                magnitude2 += schema_group2.first[j] * schema_group2.first[j];
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                magnitude1 += schema_group1.first[i] * schema_group1.first[i];
                i++;
            } else {
                magnitude2 += schema_group2.first[j] * schema_group2.first[j];
                j++;
            }
        }

        while (i < schema_group1.first.size()) {
            magnitude1 += schema_group1.first[i] * schema_group1.first[i];
            i++;
        }
        while (j < schema_group2.first.size()) {
            magnitude2 += schema_group2.first[j] * schema_group2.first[j];
            j++;
        }

        double cosine_similarity = dot_product / (sqrt(magnitude1) * sqrt(magnitude2));
        return cosine_similarity;
    }

    double _ComputeCostMergingSchemaGroupsDice(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        int64_t num_common = 0;
        idx_t i = 0;
        idx_t j = 0;

        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                num_common++;
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                i++;
            } else {
                j++;
            }
        }

        int64_t size1 = schema_group1.first.size();
        int64_t size2 = schema_group2.first.size();

        double dice_similarity = (2.0 * num_common) / (size1 + size2);
        return dice_similarity;
    }

    double _ComputeCostMergingSchemaGroupsOverlap(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2)
    {
        int64_t num_common = 0;
        idx_t i = 0;
        idx_t j = 0;

        while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
            if (schema_group1.first[i] == schema_group2.first[j]) {
                num_common++;
                i++;
                j++;
            } else if (schema_group1.first[i] < schema_group2.first[j]) {
                i++;
            } else {
                j++;
            }
        }

        int64_t size1 = schema_group1.first.size();
        int64_t size2 = schema_group2.first.size();
        int64_t min_size = std::min(size1, size2);

        double overlap_similarity = num_common / (double)min_size;
        return overlap_similarity;
    }

    double _ComputeVecOvh(size_t num_tuples) {
        D_ASSERT(num_tuples >= 1);
        if (num_tuples > VEC_OVHD_THRESHOLD) return 0.0;
        else return (double) VEC_OVHD_THRESHOLD / num_tuples;
    }

    void _ComputeCoordinateMatrix() {
        // Mij = ((D1j)^2 + (Di1)^2 - (Dij)^2) / 2
        // Eigen::MatrixXd gram_matrix =
        //     Eigen::MatrixXd::Zero(schema_groups.size(), schema_groups.size());
        // for (auto i = 0; i < schema_groups.size(); i++) {
        //     for (auto j = 0; j < schema_groups.size(); j++) {
        //         // TODO efficient way?
        //         gram_matrix(i, j) =
        //             0.5 * (distance_matrix(0, j) * distance_matrix(0, j) +
        //                    distance_matrix(i, 0) * distance_matrix(i, 0) -
        //                    distance_matrix(i, j) * distance_matrix(i, j));
        //     }
        // }

        // std::cout << "Generate Gram Matrix Done!\n";
        // std::cout << gram_matrix << std::endl;
        // Eigen::SelfAdjointEigenSolver<Eigen::MatrixXd> eigensolver(gram_matrix);
        // if (eigensolver.info() != Eigen::Success) {
        //     throw InternalException("Failed to compute eigenvalues");
        // }

        // std::cout << "Eigen decomposition results\n";
        // // std::cout << "The eigenvalues of A are:\n" << eigensolver.eigenvalues() << std::endl;
        // // std::cout << "Here's a matrix whose columns are eigenvectors of A \n"
        // //     << "corresponding to these eigenvalues:\n"
        // //     << eigensolver.eigenvectors() << std::endl;
        // std::cout << eigensolver.eigenvectors() << std::endl;
        // std::cout << eigensolver.eigenvalues() << std::endl;
        // std::cout << eigensolver.eigenvalues().array().sqrt().matrix() << std::endl;
        // std::cout << eigensolver.eigenvectors() * eigensolver.eigenvalues().array().sqrt().matrix().asDiagonal() << std::endl;
    }


    double CalculateCost(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2,
        const CostModel _cost_model,
        size_t num_schemas)
    {
        if (_cost_model == CostModel::OURS) {
            return _ComputeDistanceMergingSchemaOurs(schema_group1, schema_group2, num_schemas);
        } else if (_cost_model == CostModel::SETEDIT) {
            return _ComputeCostMergingSchemaSetEdit(schema_group1, schema_group2);
        } else if (_cost_model == CostModel::JACCARD) {
            return _ComputeCostMergingSchemaGroupsJaccard(schema_group1, schema_group2);
        } else if (_cost_model == CostModel::WEIGHTEDJACCARD) {
            return _ComputeCostMergingSchemaGroupsWeightedJaccard(schema_group1, schema_group2);
        } else if (_cost_model == CostModel::COSINE) {
            return _ComputeCostMergingSchemaGroupsCosine(schema_group1, schema_group2);
        } else if (_cost_model == CostModel::DICE) {
            return _ComputeCostMergingSchemaGroupsDice(schema_group1, schema_group2);
        } else if (_cost_model == CostModel::OVERLAP) {
            return _ComputeCostMergingSchemaGroupsOverlap(schema_group1, schema_group2);
        } else {
            D_ASSERT(false);
            return COST_MAX; // Fallback in case of an unsupported cost model
        }
    }

    double _ComputeDistanceMergingSchemaOurs(
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group1,
        std::pair<std::vector<uint32_t>, uint64_t> &schema_group2,
        size_t num_schemas)
    {
        try {
            if (schema_group1.first.empty() || schema_group2.first.empty()) {
                throw std::invalid_argument("Schema group vectors cannot be empty.");
            }

            double cost_schema = -CostSchemaVal * log(num_schemas);
            // double cost_schema = -CostSchemaVal;
            double cost_null = CostNullVal;
            double cost_vectorization = CostVectorizationVal;

            int64_t num_nulls1 = 0;
            int64_t num_nulls2 = 0;
            idx_t i = 0;
            idx_t j = 0;
            while (i < schema_group1.first.size() && j < schema_group2.first.size()) {
                if (schema_group1.first[i] == schema_group2.first[j]) {
                    i++;
                    j++;
                } else if (schema_group1.first[i] < schema_group2.first[j]) {
                    num_nulls1++;
                    i++;
                } else {
                    num_nulls2++;
                    j++;
                }
            }
            while (i < schema_group2.first.size()) {
                num_nulls1++;
                i++;
            }
            while (j < schema_group1.first.size()) {
                num_nulls2++;
                j++;
            }

            cost_null *= (num_nulls1 * schema_group1.second + num_nulls2 * schema_group2.second);
            if (schema_group1.second < VEC_OVHD_THRESHOLD ||
                schema_group2.second < VEC_OVHD_THRESHOLD) {
                cost_vectorization *= (_ComputeVecOvh(schema_group1.second + schema_group2.second)
                    - _ComputeVecOvh(schema_group1.second) - _ComputeVecOvh(schema_group2.second));
            } else {
                cost_vectorization = 0.0;
            }
            double distance = cost_schema + cost_null + cost_vectorization;
            return distance;
        } catch (const std::exception &e) {
            std::cerr << "Error in _ComputeDistanceMergingSchemaOurs: " << e.what() << std::endl;
            throw; // Re-throw the exception after logging
        }
    }

    void _ClusterSchemaDBScan() {
        // sort schema
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                      schema_groups_with_num_tuples[i].first.end());
        }

        // run dbscan
        auto dbscan = DBSCAN<std::pair<std::vector<uint32_t>, uint64_t>, double>();
        dbscan.Run(&schema_groups_with_num_tuples, 1, 0.5f, 1,
                   [&](const std::pair<std::vector<uint32_t>, uint64_t> &a,
                       const std::pair<std::vector<uint32_t>, uint64_t> &b) {
                        int64_t num_nulls1 = 0;
                        int64_t num_nulls2 = 0;
                        idx_t i = 0;
                        idx_t j = 0;
                        while (i < a.first.size() && j < b.first.size()) {
                            if (a.first[i] == b.first[j]) {
                                i++;
                                j++;
                            } else if (a.first[i] < b.first[j]) {
                                num_nulls1++;
                                i++;
                            } else {
                                num_nulls2++;
                                j++;
                            }
                        }
                        while (i < a.first.size()) {
                            num_nulls1++;
                            i++;
                        }
                        while (j < b.first.size()) {
                            num_nulls2++;
                            j++;
                        }

                        int64_t num_common = (a.first.size() + b.first.size() - num_nulls1 - num_nulls2) / 2;

                        double distance = num_common / (double) (a.first.size() + b.first.size() - num_common);
                        return distance;
                   });

        auto &clusters = dbscan.Clusters;
        auto &noise = dbscan.Noise;

        sg_to_cluster_vec.resize(schema_groups_with_num_tuples.size());
        num_clusters = clusters.size() + noise.size();
        cluster_tokens.reserve(num_clusters);
        
        for (auto i = 0; i < clusters.size(); i++) {
            std::unordered_set<uint32_t> cluster_tokens_set;
            std::cout << "Cluster " << i << ": ";
            for (auto j = 0; j < clusters[i].size(); j++) {
                std::cout << clusters[i][j] << ", ";
                sg_to_cluster_vec[clusters[i][j]] = i;
                cluster_tokens_set.insert(
                    std::begin(schema_groups_with_num_tuples[clusters[i][j]].first),
                    std::end(schema_groups_with_num_tuples[clusters[i][j]].first));
            }
            std::cout << std::endl;

            cluster_tokens.push_back(std::vector<uint32_t>());
            cluster_tokens.back().insert(
                std::end(cluster_tokens.back()), std::begin(cluster_tokens_set),
                std::end(cluster_tokens_set));
        }

        size_t num_clusters_before = clusters.size();
        for (auto i = 0; i < noise.size(); i++) {
            std::cout << noise[i] << ", ";
            sg_to_cluster_vec[noise[i]] = num_clusters_before + i;
            cluster_tokens.push_back(
                std::move(schema_groups_with_num_tuples[noise[i]].first));
        }
        std::cout << std::endl;
    }

    void _ClusterSchemaOptics()
    {
        // sort schema
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                      schema_groups_with_num_tuples[i].first.end());
        }

        // optics clustering
        auto reach_dists = optics::compute_reachability_dists<
            std::pair<std::vector<uint32_t>, uint64_t>>(
            schema_groups_with_num_tuples, 12, 0.5f,
            [&](const std::pair<std::vector<uint32_t>, uint64_t> &a,
                const std::pair<std::vector<uint32_t>, uint64_t> &b) {
                double cost_current = 2 * CostSchemaVal +
                                      _ComputeVecOvh(a.second) +
                                      _ComputeVecOvh(b.second);

                int64_t num_nulls1 = 0;
                int64_t num_nulls2 = 0;
                idx_t i = 0;
                idx_t j = 0;
                while (i < a.first.size() && j < b.first.size()) {
                    if (a.first[i] == b.first[j]) {
                        i++;
                        j++;
                    }
                    else if (a.first[i] < b.first[j]) {
                        num_nulls1++;
                        i++;
                    }
                    else {
                        num_nulls2++;
                        j++;
                    }
                }
                while (i < a.first.size()) {
                    num_nulls1++;
                    i++;
                }
                while (j < b.first.size()) {
                    num_nulls2++;
                    j++;
                }

                double cost_after = CostSchemaVal +
                                    CostNullVal * (num_nulls1 * a.second +
                                                   num_nulls2 * b.second) +
                                    _ComputeVecOvh(a.second + b.second);
                double distance = cost_after / cost_current;
                return distance;
            });

        // get chi clusters?

        // get cluster indices
        auto clusters = optics::get_cluster_indices(reach_dists, 10);
        _PopulateClusteringResults(clusters);
    }

    void _ClusterSchemaAgglomerative() {
        // sort schema
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                      schema_groups_with_num_tuples[i].first.end());
        }

        // layered approach
        vector<uint32_t> num_tuples_order;
        num_tuples_order.resize(schema_groups_with_num_tuples.size());
        std::iota(num_tuples_order.begin(), num_tuples_order.end(), 0);
        if (layering_order == LayeringOrder::ASCENDING) {
            std::sort(num_tuples_order.begin(), num_tuples_order.end(),
                    [&](size_t a, size_t b) {
                        return schema_groups_with_num_tuples[a].second <
                                schema_groups_with_num_tuples[b].second;
                    });
        }
        else if (layering_order == LayeringOrder::DESCENDING) {
            std::sort(num_tuples_order.begin(), num_tuples_order.end(),
                    [&](size_t a, size_t b) {
                        return schema_groups_with_num_tuples[a].second >
                                schema_groups_with_num_tuples[b].second;
                    });
        }
        sg_to_cluster_vec.resize(schema_groups_with_num_tuples.size());
        
        int num_layers = 0;
        vector<uint32_t> layer_boundaries;
        SplitIntoMultipleLayers(schema_groups_with_num_tuples, num_tuples_order,
                                num_layers, layer_boundaries);

        vector<std::pair<uint32_t, std::vector<uint32_t>>> current_merge_state;
        for (uint32_t i = 0; i < num_layers; i++) {
            size_t size_to_reserve = i == 0 ? layer_boundaries[i]
                                            : current_merge_state.size() +
                                                  layer_boundaries[i] -
                                                  layer_boundaries[i - 1];
            current_merge_state.reserve(size_to_reserve);
            for (uint32_t j = i == 0 ? 0 : layer_boundaries[i - 1];
                 j < layer_boundaries[i]; j++) {
                std::vector<uint32_t> temp_vec;
                temp_vec.push_back(num_tuples_order[j]);
                current_merge_state.push_back(std::make_pair(num_tuples_order[j], std::move(temp_vec)));
            }
            ClusterSchemasInCurrentLayer(schema_groups_with_num_tuples,
                                         num_tuples_order, layer_boundaries,
                                         i, current_merge_state, cost_model);
            
            // remove nullptrs
            current_merge_state.erase(
                std::remove_if(begin(current_merge_state), end(current_merge_state),
                               [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
                end(current_merge_state));
            
            for (auto i = 0; i < current_merge_state.size(); i++) {
                if (current_merge_state[i].first == std::numeric_limits<uint32_t>::max()) { continue; }

                std::cout << "Cluster " << i << " (" << schema_groups_with_num_tuples[current_merge_state[i].first].second << ") : ";
                for (auto j = 0; j < schema_groups_with_num_tuples[current_merge_state[i].first].first.size(); j++) {
                    std::cout << schema_groups_with_num_tuples[current_merge_state[i].first].first[j] << ", ";
                }
                std::cout << std::endl;

                std::cout << "\t";
                for (auto j = 0; j < current_merge_state[i].second.size(); j++) {
                    std::cout << current_merge_state[i].second[j] << ", ";
                }
                std::cout << std::endl;
            }
        }

        std::cout << "Number of final clusters: " << current_merge_state.size() << std::endl;

        if (merge_in_advance == MergeInAdvance::IN_STORAGE) {
            _MergeInAdvance(current_merge_state);
        }

        num_clusters = current_merge_state.size();
        cluster_tokens.reserve(current_merge_state.size());
        for (auto i = 0; i < current_merge_state.size(); i++) {
            if (current_merge_state[i].first == std::numeric_limits<uint32_t>::max()) { continue; }

            cluster_tokens.push_back(std::move(schema_groups_with_num_tuples[current_merge_state[i].first].first));
            std::sort(cluster_tokens.back().begin(), cluster_tokens.back().end());

            for (auto j = 0; j < current_merge_state[i].second.size(); j++) {
                sg_to_cluster_vec[current_merge_state[i].second[j]] = i;
            }
        }
    }

    void _ClusterSchemaPGSE() {

        // sort schema
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                      schema_groups_with_num_tuples[i].first.end());
        }

        // layered approach
        vector<uint32_t> num_tuples_order;
        num_tuples_order.resize(schema_groups_with_num_tuples.size());
        std::iota(num_tuples_order.begin(), num_tuples_order.end(), 0);
        sg_to_cluster_vec.resize(schema_groups_with_num_tuples.size());
        
        int num_layers = 0;
        vector<uint32_t> layer_boundaries;
        layer_boundaries.push_back(num_tuples_order.size());
        num_layers = layer_boundaries.size();

        vector<std::pair<uint32_t, std::vector<uint32_t>>> current_merge_state;
        for (uint32_t i = 0; i < num_layers; i++) {
            size_t size_to_reserve = i == 0 ? layer_boundaries[i]
                                            : current_merge_state.size() +
                                                  layer_boundaries[i] -
                                                  layer_boundaries[i - 1];
            current_merge_state.reserve(size_to_reserve);
            for (uint32_t j = i == 0 ? 0 : layer_boundaries[i - 1];
                 j < layer_boundaries[i]; j++) {
                std::vector<uint32_t> temp_vec;
                temp_vec.push_back(num_tuples_order[j]);
                current_merge_state.push_back(std::make_pair(num_tuples_order[j], std::move(temp_vec)));
            }
            ClusterSchemasInCurrentLayer(schema_groups_with_num_tuples,
                                         num_tuples_order, layer_boundaries,
                                         i, current_merge_state, CostModel::WEIGHTEDJACCARD);
            
            // remove nullptrs
            current_merge_state.erase(
                std::remove_if(begin(current_merge_state), end(current_merge_state),
                               [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
                end(current_merge_state));
        }

        // Step 3: Populate cluster_tokens in sorted order
        num_clusters = current_merge_state.size();
        cluster_tokens.reserve(current_merge_state.size());

        for (auto i = 0; i < current_merge_state.size(); i++) {

            std::cout << "Cluster " << i << " (" << schema_groups_with_num_tuples[current_merge_state[i].first].second << ") : ";
            for (auto j = 0; j < schema_groups_with_num_tuples[current_merge_state[i].first].first.size(); j++) {
                std::cout << schema_groups_with_num_tuples[current_merge_state[i].first].first[j] << ", ";
            }
            std::cout << std::endl;

            cluster_tokens.push_back(std::move(schema_groups_with_num_tuples[current_merge_state[i].first].first));
            std::sort(cluster_tokens.back().begin(), cluster_tokens.back().end());

            std::cout << "\t";
            for (auto j = 0; j < current_merge_state[i].second.size(); j++) {
                std::cout << current_merge_state[i].second[j] << ", ";
            }
            std::cout << std::endl;

            for (auto j = 0; j < current_merge_state[i].second.size(); j++) {
                sg_to_cluster_vec[current_merge_state[i].second[j]] = i;
            }
        }
    }


    // Function to merge all schemas into a single cluster
    void _ClusterAllSchemas() {
        // Step 1: Sort all schema groups
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                    schema_groups_with_num_tuples[i].first.end());
        }

        // Step 2: Initialize clusters
        std::vector<std::vector<std::size_t>> clusters;

        // Step 3: Place all schema indices into one cluster
        std::vector<size_t> schemas_in_cluster(schema_groups_with_num_tuples.size());
        std::iota(schemas_in_cluster.begin(), schemas_in_cluster.end(), 0); // Fill with schema group indices
        clusters.push_back(schemas_in_cluster); // Add all schemas to one single cluster

        // Step 4: Populate clustering results for the merged cluster
        _PopulateClusteringResults(clusters);
    }

    // Function to split all schemas into individual clusters
    void _ClusterEachSchemaSeparately() {
        // Step 1: Sort all schema groups
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                    schema_groups_with_num_tuples[i].first.end());
        }

        // Step 2: Initialize clusters
        std::vector<std::vector<std::size_t>> clusters;

        // Step 3: Create individual clusters for each schema
        for (size_t i = 0; i < schema_groups_with_num_tuples.size(); ++i) {
            clusters.push_back({i}); // Each schema group index gets its own cluster
        }

        // Step 4: Populate clustering results for the split clusters
        _PopulateClusteringResults(clusters);
    }

    void _ClusterSchemaGMM() {
        // sort schema
        for (auto i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            std::sort(schema_groups_with_num_tuples[i].first.begin(),
                      schema_groups_with_num_tuples[i].first.end());
        }

        // Initialize
        std::vector<std::vector<std::size_t>> clusters;
        std::vector<size_t> schemas_in_cluster(schema_groups_with_num_tuples.size());
        std::iota(schemas_in_cluster.begin(), schemas_in_cluster.end(), 0);

        // Initialize Python interpreter
        if (p_sklearn_module == nullptr) {
            PyObject* p_name = PyUnicode_DecodeFSDefault("sklearn.mixture");
            p_sklearn_module = PyImport_Import(p_name);
            Py_DECREF(p_name);
        }

        // Run GMM Clustering
        _GMMClustering(clusters, schemas_in_cluster);
        _PopulateClusteringResults(clusters);
    }

    void _GMMClustering(std::vector<std::vector<std::size_t>>& clusters,
                        std::vector<size_t>& schemas_in_cluster) {
        D_ASSERT(schemas_in_cluster.size() > 0);

        if (schemas_in_cluster.size() == 1) {
            clusters.push_back(schemas_in_cluster);
            return;
        }

        vector<std::pair<vector<uint32_t>, uint64_t>*> _schema_groups_with_num_tuples;
        _GetSchemaGroupsWithNumTuplesInCluster(schemas_in_cluster, _schema_groups_with_num_tuples);

        D_ASSERT(_schema_groups_with_num_tuples.size() == schemas_in_cluster.size());

        vector<uint32_t> reference_schema_group;
        _GetReferenceSchemaGroup(_schema_groups_with_num_tuples, reference_schema_group);
        D_ASSERT(reference_schema_group.size() == 1); // 1-most frequent property id

        vector<float> similarities;
        _ComputeSimilarities(_schema_groups_with_num_tuples, reference_schema_group, similarities);
        
        D_ASSERT(similarities.size() == _schema_groups_with_num_tuples.size());

        vector<vector<float>> feature_vector;
        _ComputeFeatureVector(_schema_groups_with_num_tuples, similarities, feature_vector);

        vector<uint32_t> per_tuple_predictions;
        _FitPredictGMMPython(feature_vector, per_tuple_predictions);

        vector<uint32_t> per_schema_predictions;
        _GetPerSchemaPredictions(per_tuple_predictions, _schema_groups_with_num_tuples, per_schema_predictions);

        D_ASSERT(per_schema_predictions.size() == _schema_groups_with_num_tuples.size());

        if (_IsClusteringEnded(per_schema_predictions)) {
            clusters.push_back(schemas_in_cluster);
            return;
        }
        else {
            std::vector<size_t> cluster_1_schemas;
            std::vector<size_t> cluster_2_schemas;
            _SplitSchemasBasedOnPrediction(per_schema_predictions, schemas_in_cluster, cluster_1_schemas, cluster_2_schemas);
            _GMMClustering(clusters, cluster_1_schemas);
            _GMMClustering(clusters, cluster_2_schemas);
            return;
        }
    }

    void _GetSchemaGroupsWithNumTuplesInCluster(std::vector<size_t>& schemas_in_cluster, 
        vector<std::pair<vector<uint32_t>, uint64_t>*>& _schema_groups_with_num_tuples) {
        _schema_groups_with_num_tuples.reserve(schemas_in_cluster.size());
        for (auto i = 0; i < schemas_in_cluster.size(); i++) {
            _schema_groups_with_num_tuples.push_back(&schema_groups_with_num_tuples[schemas_in_cluster[i]]);
        }
    }

    void _GetPerSchemaPredictions(vector<uint32_t>& per_tuple_predictions, 
                                    vector<std::pair<vector<uint32_t>, uint64_t>*>& _schema_groups_with_num_tuples,
                                    vector<uint32_t>& per_schema_predictions) {
        size_t offset = 0;
        per_schema_predictions.reserve(_schema_groups_with_num_tuples.size());
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            uint32_t num_tuples = _schema_groups_with_num_tuples[i]->second;
            per_schema_predictions.push_back(per_tuple_predictions[offset]);
            offset += num_tuples;
        }
    }

    void _SplitSchemasBasedOnPrediction(vector<uint32_t> &predictions,
                                        std::vector<size_t> &schemas_in_cluster,
                                        std::vector<size_t> &cluster_1_schemas,
                                        std::vector<size_t> &cluster_2_schemas)
    {
        for (auto i = 0; i < predictions.size(); i++) {
            if (predictions[i] == 0) {
                cluster_1_schemas.push_back(schemas_in_cluster[i]);
            }
            else {
                cluster_2_schemas.push_back(schemas_in_cluster[i]);
            }
        }

        D_ASSERT(cluster_1_schemas.size() + cluster_2_schemas.size() ==
                 schemas_in_cluster.size());
        D_ASSERT(cluster_1_schemas.size() > 0);
        D_ASSERT(cluster_2_schemas.size() > 0);
    }


    void _PopulateClusteringResults(std::vector<std::vector<std::size_t>>& clusters) {
        sg_to_cluster_vec.resize(schema_groups_with_num_tuples.size());
        num_clusters = clusters.size();
        cluster_tokens.reserve(num_clusters);

        // Step 1: Compute the number of tuples for each cluster
        // std::vector<std::pair<std::size_t, std::size_t>> cluster_tuples_count;
        // for (std::size_t i = 0; i < clusters.size(); i++) {
        //     std::size_t tuple_count = 0;
        //     for (auto j : clusters[i]) {
        //         tuple_count += schema_groups_with_num_tuples[j].second;
        //     }
        //     cluster_tuples_count.push_back({i, tuple_count});
        // }

        // Step 2: Sort the clusters based on the number of tuples in descending order
        // std::sort(cluster_tuples_count.begin(), cluster_tuples_count.end(),
        //         [](const std::pair<std::size_t, std::size_t>& a, const std::pair<std::size_t, std::size_t>& b) {
        //             return a.second > b.second;
        //         });

        // Step 3: Populate cluster_tokens in sorted order
        // for (const auto& cluster_info : cluster_tuples_count) {
        for (auto i = 0; i < clusters.size(); i++) {
            // std::size_t cluster_idx = cluster_info.first;
            std::unordered_set<uint32_t> cluster_tokens_set;
            std::cout << "Cluster " << i << ": ";
            for (auto j = 0; j < clusters[i].size(); j++) {
                std::cout << clusters[i][j] << ", ";
                sg_to_cluster_vec[clusters[i][j]] = i;
                cluster_tokens_set.insert(
                    std::begin(schema_groups_with_num_tuples[clusters[i][j]].first),
                    std::end(schema_groups_with_num_tuples[clusters[i][j]].first));
            }
            std::cout << std::endl;

            cluster_tokens.push_back(std::vector<uint32_t>());
            cluster_tokens.back().insert(
                std::end(cluster_tokens.back()), std::begin(cluster_tokens_set),
                std::end(cluster_tokens_set));
        }
    }

    bool _IsClusteringEnded(vector<uint32_t>& predictions) {
        for (auto i = 1; i < predictions.size(); i++) {
            if (predictions[i] != predictions[0]) {
                return false;
            }
        }
        return true;
    }

    void _GetReferenceSchemaGroup(vector<std::pair<vector<uint32_t>, uint64_t>*>& _schema_groups_with_num_tuples, 
                        vector<uint32_t>& reference_schema_group) {
        // Get maximum property id
        uint32_t max_property_id = 0;
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            if (_schema_groups_with_num_tuples[i]->first.back() > max_property_id) {
                max_property_id = _schema_groups_with_num_tuples[i]->first.back();
            }
        }

        // Count per property occurence
        vector<uint32_t> property_occurence(max_property_id + 1, 0);
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            for (auto j = 0; j < _schema_groups_with_num_tuples[i]->first.size(); j++) {
                property_occurence[_schema_groups_with_num_tuples[i]->first[j]]++;
            }
        }

        // Get the most frequent property id
        uint32_t most_frequent_property_id = 0;
        uint32_t most_frequent_property_occurence = 0;
        for (auto i = 0; i < property_occurence.size(); i++) {
            if (property_occurence[i] > most_frequent_property_occurence) {
                most_frequent_property_id = i;
                most_frequent_property_occurence = property_occurence[i];
            }
        }

        // Get the schema group with the most frequent property id
        reference_schema_group.push_back(most_frequent_property_id);
    }

    void _ComputeSimilarities(vector<std::pair<vector<uint32_t>, uint64_t>*>& _schema_groups_with_num_tuples,
                    vector<uint32_t>& reference_schema_group, vector<float> &similarities) {
        similarities.reserve(_schema_groups_with_num_tuples.size());
        
        // Calculate dice coefficient
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            float similarity = 0.0;
            uint32_t intersection = 0;
            uint32_t union_size = reference_schema_group.size() + _schema_groups_with_num_tuples[i]->first.size();
            for (auto j = 0; j < reference_schema_group.size(); j++) {
                for (auto k = 0; k < _schema_groups_with_num_tuples[i]->first.size(); k++) {
                    if (reference_schema_group[j] == _schema_groups_with_num_tuples[i]->first[k]) {
                        intersection++;
                        break;
                    }
                }
            }
            union_size -= intersection;
            similarity = 2.0 * intersection / (union_size + intersection);
            similarities.push_back(similarity);
        }
    }

    void _vectorToPyList(const std::vector<std::vector<float>>& vec, PyObject* pyList) {
        for (size_t i = 0; i < vec.size(); ++i) {
            PyObject* innerList = PyList_New(vec[i].size());
            for (size_t j = 0; j < vec[i].size(); ++j) {
                PyObject* num = PyFloat_FromDouble(vec[i][j]);
                PyList_SetItem(innerList, j, num); // PyList_SetItem steals the reference to num
            }
            PyList_SetItem(pyList, i, innerList); // PyList_SetItem steals the reference to innerList
        }
    }

    void _pyListToVector(PyObject* pyList, std::vector<uint32_t>& vec) {
        if (PyList_Check(pyList)) {
            size_t size = PyList_Size(pyList);
            vec.resize(size);
            for (size_t i = 0; i < size; ++i) {
                PyObject* item = PyList_GetItem(pyList, i); // Borrowed reference
                vec[i] = static_cast<uint32_t>(PyLong_AsUnsignedLong(item));
            }
        }
    }

    void _FitPredictGMMPython(std::vector<std::vector<float>>& feature_vector, std::vector<uint32_t>& predictions) {
        try {
            // Convert C++ vector to Python list of lists
            PyObject* py_feature_vector = PyList_New(feature_vector.size());
            _vectorToPyList(feature_vector, py_feature_vector);

            if (p_sklearn_module != NULL) {
                // Get the BayesianGaussianMixture class
                PyObject* pClass = PyObject_GetAttrString(p_sklearn_module, "BayesianGaussianMixture");

                if (pClass && PyCallable_Check(pClass)) {
                    // Create an instance of BayesianGaussianMixture with keyword arguments
                    PyObject* pKwargs = PyDict_New();
                    PyDict_SetItemString(pKwargs, "n_components", PyLong_FromLong(2));
                    PyDict_SetItemString(pKwargs, "tol", PyFloat_FromDouble(1.0));
                    PyDict_SetItemString(pKwargs, "max_iter", PyLong_FromLong(10));

                    PyObject* pInstance = PyObject_Call(pClass, PyTuple_New(0), pKwargs);
                    Py_DECREF(pKwargs);

                    if (pInstance != NULL) {
                        // Prepare arguments for the fit method
                        PyObject* pFitArgs = PyTuple_New(1);
                        PyTuple_SetItem(pFitArgs, 0, py_feature_vector);
                        Py_INCREF(py_feature_vector); // Increment ref count because SetItem steals a reference

                        // Call the fit method
                        PyObject* pFitResult = PyObject_CallMethod(pInstance, "fit", "(O)", py_feature_vector);
                        Py_DECREF(pFitArgs);

                        if (pFitResult != NULL) {
                            Py_DECREF(pFitResult);

                            // Call the predict method
                            PyObject* pPredictResult = PyObject_CallMethod(pInstance, "predict", "(O)", py_feature_vector);

                            if (pPredictResult != NULL) {
                                // Convert the NumPy array to a Python list
                                PyObject* pPredictList = PyObject_CallMethod(pPredictResult, "tolist", NULL);
                                Py_DECREF(pPredictResult);

                                if (pPredictList != NULL && PyList_Check(pPredictList)) {
                                    // Convert Python list of predictions to C++ vector
                                    _pyListToVector(pPredictList, predictions);
                                    Py_DECREF(pPredictList);
                                } else {
                                    PyErr_Print();
                                }
                            } else {
                                PyErr_Print();
                            }
                        } else {
                            PyErr_Print();
                        }

                        Py_DECREF(pInstance);
                    } else {
                        PyErr_Print();
                    }

                    Py_DECREF(pClass);
                } else {
                    PyErr_Print();
                }
            } else {
                PyErr_Print();
            }

            Py_DECREF(py_feature_vector);
        } catch (...) {
            PyErr_Print();
        }
    }

    void _ComputeFeatureVector(vector<std::pair<vector<uint32_t>, uint64_t>*>& _schema_groups_with_num_tuples,
            vector<float> &similarities, vector<vector<float>> &feature_vector) {
        // Calculate total number of tuples
        uint64_t total_num_tuples = 0;
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            total_num_tuples += _schema_groups_with_num_tuples[i]->second;
        }
        feature_vector.reserve(total_num_tuples);

        // Calculate feature vector (pump each similarity value to the mulitple of instances)
        for (auto i = 0; i < _schema_groups_with_num_tuples.size(); i++) {
            for (auto j = 0; j < _schema_groups_with_num_tuples[i]->second; j++) {
                feature_vector.push_back({similarities[i]});
            }
        }
    }

    void SplitIntoMultipleLayers(
        const vector<std::pair<std::vector<uint32_t>, uint64_t>>
            &schema_groups_with_num_tuples,
        const vector<uint32_t> &num_tuples_order, int &num_layers,
        vector<uint32_t> &layer_boundaries)
    {
        uint64_t num_tuples_sum = 0;
        uint64_t num_schemas_sum = 0;
        if (layering_order == LayeringOrder::ASCENDING) {
            for (auto i = 0; i < num_tuples_order.size(); i++) {
                num_tuples_sum +=
                    schema_groups_with_num_tuples[num_tuples_order[i]].second;
                num_schemas_sum++;
                double avg_num_tuples = (double)num_tuples_sum / num_schemas_sum;
                if (schema_groups_with_num_tuples[num_tuples_order[i]].second >
                    avg_num_tuples * 1.5) {
                    layer_boundaries.push_back(i);
                    num_tuples_sum = 0;
                    num_schemas_sum = 0;
                }
            }
            if (layer_boundaries.size() == 0 || layer_boundaries.back() != num_tuples_order.size()) {
                layer_boundaries.push_back(num_tuples_order.size());
            }
        }
        else if (layering_order == LayeringOrder::DESCENDING) {
            layer_boundaries.push_back(num_tuples_order.size());
            for (int64_t i = num_tuples_order.size() - 1; i >= 0; i--) {
                num_tuples_sum +=
                    schema_groups_with_num_tuples[num_tuples_order[i]].second;
                num_schemas_sum++;
                double avg_num_tuples = (double)num_tuples_sum / num_schemas_sum;
                // std::cout << "num_tuples_sum: " << num_tuples_sum 
                //     << ", num_schemas_sum: " << num_schemas_sum << std::endl;
                // std::cout << "avg_num_tuples: " << avg_num_tuples << std::endl;
                // std::cout << "schema_groups_with_num_tuples[num_tuples_order[i]].second: " 
                //     << schema_groups_with_num_tuples[num_tuples_order[i]].second << std::endl;
                if (schema_groups_with_num_tuples[num_tuples_order[i]].second >
                    avg_num_tuples * 1.5) {
                    layer_boundaries.push_back(i);
                    num_tuples_sum = 0;
                    num_schemas_sum = 0;
                }
            }
            std::reverse(layer_boundaries.begin(), layer_boundaries.end());
        }
        else {
            layer_boundaries.push_back(num_tuples_order.size());
        }

        num_layers = layer_boundaries.size();

        for (auto i = 0; i < layer_boundaries.size(); i++) {
            std::cout << "Layer " << i << " boundary: " << layer_boundaries[i] << std::endl;
        }

        // num_layers = 2;
        // layer_boundaries.push_back(num_tuples_order.size() / 2);
        // layer_boundaries.push_back(num_tuples_order.size());
    }

    void ClusterSchemasInCurrentLayer(
        vector<std::pair<std::vector<uint32_t>, uint64_t>>
            &schema_groups_with_num_tuples,
        const vector<uint32_t> &num_tuples_order,
        const vector<uint32_t> &layer_boundaries,
        uint32_t current_layer,
        vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state,
        const CostModel _cost_model)
    {
        uint32_t iteration = 0;
        do {
	        SCOPED_TIMER_SIMPLE(ClusterSchemasInCurrentLayer, spdlog::level::info, spdlog::level::debug);
            spdlog::info("[ClusterSchemasInCurrentLayer] Layer: {} Iteration: {}", current_layer, iteration);

            SUBTIMER_START(ClusterSchemasInCurrentLayer, "PrecomputeMergePairs");
            auto merge_pairs = PrecomputeMergePairs(current_merge_state, _cost_model);
            auto merged_size = merge_pairs.size();
            SUBTIMER_STOP(ClusterSchemasInCurrentLayer, "PrecomputeMergePairs");

            if (merged_size == 0) break;

            size_t new_merge_state_start_idx = current_merge_state.size();
            size_t new_schema_start_idx = schema_groups_with_num_tuples.size();
            current_merge_state.resize(new_merge_state_start_idx + merged_size);
            schema_groups_with_num_tuples.resize(new_schema_start_idx + merged_size);

            SUBTIMER_START(ClusterSchemasInCurrentLayer, "MergeVertexlets");
            #pragma omp parallel for schedule(dynamic) num_threads(MAX_THREADS)
            for (size_t i = 0; i < merged_size; ++i) {
                spdlog::trace("[ClusterSchemasInCurrentLayer] Merging pair: {} {}", merge_pairs[i].first, merge_pairs[i].second);
                MergeVertexlets(
                    merge_pairs[i].first, 
                    merge_pairs[i].second, 
                    current_merge_state, 
                    new_schema_start_idx + i,
                    new_merge_state_start_idx + i
                );
            }
            SUBTIMER_STOP(ClusterSchemasInCurrentLayer, "MergeVertexlets");

            iteration++;
        } while (true);

        spdlog::info("[ClusterSchemasInCurrentLayer] Done with Total Iterations: {}", iteration);
    }


    std::vector<std::pair<uint32_t, uint32_t>> PrecomputeMergePairs(
        const std::vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state,
        const CostModel _cost_model)
    {
	    SCOPED_TIMER_SIMPLE(PrecomputeMergePairs, spdlog::level::info, spdlog::level::debug);

        SUBTIMER_START(PrecomputeMergePairs, "Initialize Heaps");
        std::vector<CostHeap> heaps;
        InitializeHeaps(current_merge_state, _cost_model, heaps);
        SUBTIMER_STOP(PrecomputeMergePairs, "Initialize Heaps");
        
	    SUBTIMER_START(PrecomputeMergePairs, "Fill Cost Heaps");
        FillCostHeap(current_merge_state, _cost_model, heaps);
        SUBTIMER_STOP(PrecomputeMergePairs, "Fill Cost Heaps");

        SUBTIMER_START(PrecomputeMergePairs, "Find Merge Pairs");
        std::vector<std::pair<uint32_t, uint32_t>> final_pairs;
        FindMergePairs(heaps, _cost_model, final_pairs);
        SUBTIMER_STOP(PrecomputeMergePairs, "Find Merge Pairs");
        
        spdlog::info("[PrecomputeMergePairs] {} merge pairs generated", final_pairs.size());
        return final_pairs;
    }

    using CostPair = std::pair<double, uint32_t>;
    using CostHeap = std::priority_queue<CostPair, std::vector<CostPair>, std::function<bool(const CostPair&, const CostPair&)>>;

    inline double GetWorstCost(CostModel model) {
        return (model == CostModel::OURS || model == CostModel::SETEDIT)
                    ? std::numeric_limits<double>::max()
                    : std::numeric_limits<double>::lowest();
    }

    inline std::function<bool(const CostPair&, const CostPair&)> GetCostCompare(CostModel model) {
        auto cost_compare_great = [](const CostPair &a, const CostPair &b) { 
            return (a.first > b.first) || (a.first == b.first && a.second > b.second); 
        };
        auto cost_compare_less = [](const CostPair &a, const CostPair &b) { 
            return (a.first < b.first) || (a.first == b.first && a.second < b.second);
        };
        auto cost_compare = (model == CostModel::OURS || model == CostModel::SETEDIT)
                                ? cost_compare_great
                                : cost_compare_less;
        return cost_compare;
    }       

    inline bool PassesThreshold(double cost, CostModel model) {
        switch (model) {
            case CostModel::OURS:
                return cost <= 0;
            case CostModel::SETEDIT:
                return cost <= SET_EDIT_THRESHOLD;
            case CostModel::OVERLAP:
                return cost >= OVERLAP_THRESHOLD;
            case CostModel::JACCARD:
                return cost >= JACCARD_THRESHOLD;
            case CostModel::WEIGHTEDJACCARD:
                return cost >= WEIGHTEDJACCARD_THRESHOLD;
            case CostModel::COSINE:
                return cost >= COSINE_THRESHOLD;
            case CostModel::DICE:
                return cost >= DICE_THRESHOLD;
            default:
                return false;
        }
    }

    void FillCostHeap(
        const std::vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state,
        const CostModel _cost_model,
        std::vector<CostHeap> &heaps)
    {
        const size_t num_tuples_total = current_merge_state.size();
        #pragma omp parallel for schedule(dynamic) num_threads(MAX_THREADS)
        for (uint32_t i = 0; i < num_tuples_total; ++i) {
            if (current_merge_state[i].first == MERGED_SCHEMA) {
                continue;
            }

            for (uint32_t j = i + 1; j < num_tuples_total; ++j) {
                if (current_merge_state[j].first == MERGED_SCHEMA) {
                    continue;
                }
                
                uint32_t group1_idx = current_merge_state[i].first;
                uint32_t group2_idx = current_merge_state[j].first;

                double cost = CalculateCost(schema_groups_with_num_tuples[group1_idx],
                                            schema_groups_with_num_tuples[group2_idx],
                                            _cost_model,
                                            schema_groups_with_num_tuples.size());
                
                if (PassesThreshold(cost, _cost_model)) {
                    heaps[i].emplace(cost, j);
                }
            }
        }

        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            spdlog::trace("[FillCostHeap] Heaps filled");
            for (uint32_t i = 0; i < num_tuples_total; ++i) {
                spdlog::trace("[FillCostHeap] Heap {} size: {}", i, heaps[i].size());
            }
        }
    }

    void FillFrontiers(
        std::vector<CostHeap> &heaps,
        std::vector<CostPair> &frontiers,
        std::unordered_set<uint32_t> &active_schemas,
        std::vector<bool> &schema_merged,
        bool initialize)
    {
        spdlog::trace("[FillFrontiers] {} active schemas initially", active_schemas.size());

        std::vector<uint32_t> active_schemas_vec(active_schemas.begin(), active_schemas.end());

        // print schemas
        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            for (auto &schema_idx : active_schemas_vec) {
                spdlog::trace("[FillFrontiers] Schema: {}", schema_idx);
            }
        }

        std::vector<int> inactive_schemas(active_schemas_vec.size(), true);
        #pragma omp parallel for schedule(dynamic) num_threads(MAX_THREADS)
        for (size_t i = 0; i < active_schemas_vec.size(); ++i) {
            auto schema_idx = active_schemas_vec[i];
            auto current_pair = frontiers[schema_idx];
            if (initialize || schema_merged[current_pair.second]) {
                while(!heaps[schema_idx].empty()) {
                    auto [cost, pair_idx] = heaps[schema_idx].top();
                    heaps[schema_idx].pop();
                    if (schema_merged[pair_idx]) {
                        continue;
                    }
                    else {
                        inactive_schemas[i] = false;
                        frontiers[schema_idx] = std::make_pair(cost, pair_idx);
                        break;
                    }
                }
            }
            else {
                inactive_schemas[i] = false;
            }
        }

        // Print frontiers
        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            for (auto &pair : frontiers) {
                spdlog::trace("[FillFrontiers] Frontier: {} {}", pair.first, pair.second);
            }
        }

        // TODO: Need Efficient Erase
        for (size_t i = 0; i < active_schemas_vec.size(); ++i) {
            if (inactive_schemas[i]) {
                spdlog::trace("[FillFrontiers] Erasing schema {}", active_schemas_vec[i]);
                active_schemas.erase(active_schemas_vec[i]);
            }
        }

        spdlog::trace("[FillFrontiers] {} active schemas remained after update", active_schemas.size());
    }

    std::pair<uint32_t, uint32_t> FindBestMergePair(
        const CostModel _cost_model,
        std::vector<CostPair> &frontiers,
        std::unordered_set<uint32_t> &active_schemas)
    {
        spdlog::trace("[FindBestMergePair] {} active schemas initially", active_schemas.size());
        uint32_t best_idx, best_pair_idx;
        double best_cost =  GetWorstCost(_cost_model);
        auto cost_compare = GetCostCompare(_cost_model);
        for (uint32_t idx : active_schemas) {
            auto [cost, pair_idx] = frontiers[idx];
            if (cost_compare(std::make_pair(best_cost, 0), std::make_pair(cost, 0))) {
                best_cost = cost;
                best_idx = idx;
                best_pair_idx = pair_idx;
            }
        }
        spdlog::trace("[FindBestMergePair] {} and {} are the best pair", best_idx, best_pair_idx);
        return std::make_pair(best_idx, best_pair_idx);
    }

    void FindMergePairs(
        std::vector<CostHeap> &heaps,
        const CostModel _cost_model,
        std::vector<std::pair<uint32_t, uint32_t>> &merge_pairs)
    {
        size_t num_heaps = heaps.size();
        std::vector<std::pair<uint32_t, uint32_t>> final_pairs;
        std::vector<CostPair> frontiers(num_heaps);
        std::unordered_set<uint32_t> active_schemas;
        std::vector<bool> schema_merged(num_heaps, false);

        spdlog::debug("[FindMergePairs] Identify active schemas");
        for (uint32_t i = 0; i < num_heaps; ++i) {
            if (!heaps[i].empty()) {
                active_schemas.insert(i);
            }
        }

        spdlog::debug("[FindMergePairs] Initially Fill frontiers");
        FillFrontiers(
            heaps,
            frontiers,
            active_schemas,
            schema_merged,
            true
        );

        spdlog::debug("[FindMergePairs] Iteratively Find Merge Pairs");
        while (!active_schemas.empty()) {
            auto best_pair = FindBestMergePair(
                _cost_model,
                frontiers,
                active_schemas
            );

            merge_pairs.push_back(best_pair);
            active_schemas.erase(best_pair.first);
            active_schemas.erase(best_pair.second);
            schema_merged[best_pair.first] = true;
            schema_merged[best_pair.second] = true;

            spdlog::trace("[FindMergePairs] Update frontiers");
            FillFrontiers(
                heaps,
                frontiers,
                active_schemas,
                schema_merged,
                false
            );
        }

        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            for (auto &pair : merge_pairs) {
                spdlog::trace("[FindMergePairs] Pair: {} {}", pair.first, pair.second);
            }
        }
    }

    void InitializeHeaps(
        const std::vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state,
        const CostModel _cost_model,
        std::vector<CostHeap> &heaps)
    {
        const size_t num_tuples_total = current_merge_state.size();
        spdlog::info("[InitializeHeaps] Number of tuples: {}", num_tuples_total);
        auto cost_compare = GetCostCompare(_cost_model);
        
        heaps.reserve(num_tuples_total);
        for (size_t i = 0; i < num_tuples_total; ++i) {
            heaps.emplace_back(cost_compare);
        }
        spdlog::debug("[InitializeHeaps] {} Heaps initialized", num_tuples_total);
    }

    void MergeVertexlets(uint32_t idx1, uint32_t idx2, 
                     std::vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state,
                     size_t new_schema_group_idx, size_t new_output_idx)
    {
        if (idx1 >= current_merge_state.size() || idx2 >= current_merge_state.size()) {
            spdlog::error("Error: Index out of bounds for current_merge_state. "
                    "idx1: {}, idx2: {}, current_merge_state.size(): {}", idx1, idx2, current_merge_state.size());
            return;
        }

        auto &schema_group1 = schema_groups_with_num_tuples[current_merge_state[idx1].first];
        auto &schema_group2 = schema_groups_with_num_tuples[current_merge_state[idx2].first];

        std::vector<uint32_t> merged_schema;
        merged_schema.reserve(schema_group1.first.size() + schema_group2.first.size());
        std::merge(schema_group1.first.begin(), schema_group1.first.end(),
                schema_group2.first.begin(), schema_group2.first.end(),
                std::back_inserter(merged_schema));
        merged_schema.erase(std::unique(merged_schema.begin(), merged_schema.end()), merged_schema.end());
        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            spdlog::trace("[MergeVertexlets] Merged schema: {}", join_vector(merged_schema));
        }

        uint64_t merged_num_tuples = schema_group1.second + schema_group2.second;
        schema_groups_with_num_tuples[new_schema_group_idx] = std::make_pair(std::move(merged_schema), merged_num_tuples);
        spdlog::trace("[MergeVertexlets] Add schema {} with {} tuples", new_schema_group_idx, merged_num_tuples);

        std::vector<uint32_t> merged_indices;
        merged_indices.reserve(current_merge_state[idx1].second.size() + current_merge_state[idx2].second.size());
        merged_indices.insert(merged_indices.end(), current_merge_state[idx1].second.begin(), current_merge_state[idx1].second.end());
        merged_indices.insert(merged_indices.end(), current_merge_state[idx2].second.begin(), current_merge_state[idx2].second.end());
        if (spdlog::default_logger_raw()->should_log(spdlog::level::trace)) {
            spdlog::trace("[MergeVertexlets] Merged indices: {}", join_vector(merged_indices));
        }

        current_merge_state[new_output_idx] = std::make_pair(new_schema_group_idx, std::move(merged_indices));
        spdlog::trace("[MergeVertexlets] Add merge state {}", new_output_idx);

        current_merge_state[idx1].first = MERGED_SCHEMA;
        current_merge_state[idx2].first = MERGED_SCHEMA;
    }

    vector<unsigned int> &GetClusterTokens(size_t cluster_idx) {
        switch(cluster_algo_type) {
            case ClusterAlgorithmType::OPTICS:
            case ClusterAlgorithmType::DBSCAN:
            case ClusterAlgorithmType::AGGLOMERATIVE:
            case ClusterAlgorithmType::GMM:
            case ClusterAlgorithmType::SINGLECLUSTER:
            case ClusterAlgorithmType::SEPERATECLUSTERS:
                return cluster_tokens[cluster_idx];
            default:
                D_ASSERT(false);
        }
        return cluster_tokens[cluster_idx];
    }

    void _CreateExtents(GraphComponentType gctype, GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
        if (gctype == GraphComponentType::VERTEX) {
            _CreateVertexExtents(graph_cat, label_name, label_set);
        } else if (gctype == GraphComponentType::EDGE) {
            _CreateEdgeExtents(graph_cat, label_name, label_set);
        }
    }

    void _CreateVertexExtents(GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
        // Common operations
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
        }
        // printf("\n");
        graph_cat->GetPropertyKeyIDs(*client.get(), key_names, types, universal_property_key_ids);
        partition_cat->SetSchema(*client.get(), key_names, types, universal_property_key_ids);

        // Initialize LID_TO_PID_MAP
        if (load_edge) {
            lid_to_pid_map->emplace_back(label_name, unordered_map<LidPair, idx_t, boost::hash<LidPair>>());
            lid_to_pid_map_instance = &lid_to_pid_map->back().second;
        }

        // range-based operation for memory-efficiency
        int64_t total_num_tuples = 0;
        const size_t CLUSTER_LOAD_CHUNK = 3000;
        size_t start_cluster_idx = 0;
        size_t end_cluster_idx = num_clusters > CLUSTER_LOAD_CHUNK ? CLUSTER_LOAD_CHUNK : num_clusters;
        while(true) {
            if (start_cluster_idx >= num_clusters) {
                break;
            }

            size_t num_clusters_to_process = end_cluster_idx - start_cluster_idx;

            vector<DataChunk> datas(num_clusters_to_process);
            property_to_id_map_per_cluster.clear();
            property_to_id_map_per_cluster.resize(num_clusters_to_process);

            // Create property schema catalog for each cluster
            property_schema_cats.clear();
            property_schema_cats.resize(num_clusters_to_process);
            vector<vector<idx_t>> per_cluster_key_column_idxs;
            per_cluster_key_column_idxs.resize(num_clusters_to_process);
            for (size_t i = 0; i < num_clusters_to_process; i++) {
                uint64_t cluster_id = start_cluster_idx + i;
                string property_schema_name = DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX + std::string(label_name) + "_" + std::to_string(cluster_id);
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

                vector<unsigned int> &tokens = GetClusterTokens(cluster_id);
                for (size_t token_idx = 0; token_idx < tokens.size(); token_idx++) {
                    // std::cout << tokens[token_idx] << ", ";
                    uint64_t original_idx = tokens[token_idx];

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

            // Iterate JSON file again & create extents
            vector<int64_t> num_tuples_per_cluster;
            num_tuples_per_cluster.resize(num_clusters_to_process, 0);
            int64_t num_tuples = 0;

            for (size_t local_cluster_id = 0; local_cluster_id < num_clusters_to_process; local_cluster_id++) {
                for (auto col_idx = 0; col_idx < datas[local_cluster_id].ColumnCount(); col_idx++) {
                    auto &validity = FlatVector::Validity(datas[local_cluster_id].data[col_idx]);
                    validity.Initialize(STORAGE_STANDARD_VECTOR_SIZE);
                    validity.SetAllInvalid(STORAGE_STANDARD_VECTOR_SIZE);
                }
            }
            
            int64_t doc_idx = 0;
            docs = parser.iterate_many(json); // TODO w/o parse?
            for (auto doc_ : docs) {
                uint64_t cluster_id =
                    sg_to_cluster_vec[corresponding_schemaID[doc_idx++]];
                uint64_t local_cluster_id = cluster_id - start_cluster_idx;

                if (cluster_id < start_cluster_idx || cluster_id >= end_cluster_idx) {
                    continue;
                }

                recursive_iterate_jsonl(doc_["properties"], "", true,
                                        num_tuples_per_cluster[local_cluster_id], 0,
                                        local_cluster_id, datas[local_cluster_id]);

                if (++num_tuples_per_cluster[local_cluster_id] ==
                    STORAGE_STANDARD_VECTOR_SIZE) {
                    // check remaining memory & flush if necessary
                    size_t remaining_memory;
                    ChunkCacheManager::ccm->GetRemainingMemoryUsage(remaining_memory);
                    if (remaining_memory < 100 * 1024 * 1024 * 1024UL) {
                        ChunkCacheManager::ccm
                            ->FlushDirtySegmentsAndDeleteFromcache(true);
                    }

                    // create extent
                    datas[local_cluster_id].SetCardinality(
                        num_tuples_per_cluster[local_cluster_id]);
                    ExtentID new_eid = ext_mng->CreateExtent(
                        *client.get(), datas[local_cluster_id], *partition_cat,
                        *property_schema_cats[local_cluster_id]);
                    property_schema_cats[local_cluster_id]->AddExtent(
                        new_eid, datas[local_cluster_id].size());

                    // store LID to PID info for edge loading
                    if (load_edge) {
                        StoreLidToPidInfo(datas[local_cluster_id],
                                        per_cluster_key_column_idxs[local_cluster_id],
                                        new_eid);
                    }

                    // reset num_tuples_per_cluster & datas
                    num_tuples_per_cluster[local_cluster_id] = 0;
                    datas[local_cluster_id].Reset(STORAGE_STANDARD_VECTOR_SIZE);
                    for (auto col_idx = 0;
                        col_idx < datas[local_cluster_id].ColumnCount(); col_idx++) {
                        auto &validity =
                            FlatVector::Validity(datas[local_cluster_id].data[col_idx]);
                        validity.Initialize(STORAGE_STANDARD_VECTOR_SIZE);
                        validity.SetAllInvalid(STORAGE_STANDARD_VECTOR_SIZE);
                    }
                }
                num_tuples++;
            }
            total_num_tuples += num_tuples;

            // Create extents for remaining datas
            for (size_t i = 0; i < num_clusters_to_process; i++) {
                size_t remaining_memory;
                ChunkCacheManager::ccm->GetRemainingMemoryUsage(remaining_memory);
                if (remaining_memory < 100 * 1024 * 1024 * 1024UL) {
                    ChunkCacheManager::ccm
                        ->FlushDirtySegmentsAndDeleteFromcache(true);
                }

                datas[i].SetCardinality(num_tuples_per_cluster[i]);
                ExtentID new_eid =
                    ext_mng->CreateExtent(*client.get(), datas[i], *partition_cat,
                                        *property_schema_cats[i]);
                property_schema_cats[i]->AddExtent(new_eid, datas[i].size());
                if (load_edge)
                    StoreLidToPidInfo(datas[i], per_cluster_key_column_idxs[i],
                                    new_eid);
            }

            printf("Vertex Load Range [%ld, %ld) Done\n", start_cluster_idx, end_cluster_idx);
            for (int i = 0; i < num_clusters_to_process; i++) {
                printf("cluster %d num_tuples: %ld\n", i + start_cluster_idx,
                    num_tuples_per_cluster[i]);
            }

            // Destroy datas
            for (size_t i = 0; i < num_clusters_to_process; i++) {
                datas[i].Destroy();
            }

            // Update cluster_id range
            start_cluster_idx += CLUSTER_LOAD_CHUNK;
            end_cluster_idx += CLUSTER_LOAD_CHUNK;
            if (end_cluster_idx >= num_clusters) end_cluster_idx = num_clusters;
        }

        printf("# of documents = %ld\n", total_num_tuples);
    }

    void _CreateEdgeExtents(GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
    }

    void _MergeInAdvance(vector<std::pair<uint32_t, std::vector<uint32_t>>> &current_merge_state) {
        D_ASSERT(merge_in_advance == MergeInAdvance::IN_STORAGE);
        // merge additionally based on cardinality
        // step 1. extract card & sort
        std::vector<std::pair<uint64_t, uint64_t>> num_tuples_per_cluster;
        num_tuples_per_cluster.reserve(current_merge_state.size());
        for (auto i = 0; i < current_merge_state.size(); i++) {
            if (current_merge_state[i].first == std::numeric_limits<uint32_t>::max()) { continue; }
            num_tuples_per_cluster.push_back(std::make_pair(
                schema_groups_with_num_tuples[current_merge_state[i].first].second, i));
        }
        std::sort(num_tuples_per_cluster.begin(), num_tuples_per_cluster.end(),
                [](const std::pair<std::uint64_t, uint64_t> &a,
                    const std::pair<std::uint64_t, uint64_t> &b) {
                    return a.second < b.second;  // sort in ascending order
                });
        // step 2. divide layer based on card
        vector<uint32_t> layer_boundaries;
        uint64_t cur_min_num_tuples = num_tuples_per_cluster[0].first;
        for (auto i = 0; i < num_tuples_per_cluster.size(); i++) {
            if (num_tuples_per_cluster[i].first >
                cur_min_num_tuples * (1 + MERGE_THRESHOLD)) {
                layer_boundaries.push_back(i);
                cur_min_num_tuples = num_tuples_per_cluster[i].first;
            }
        }
        if (layer_boundaries.size() == 0 ||
            layer_boundaries.back() != num_tuples_per_cluster.size()) {
            layer_boundaries.push_back(num_tuples_per_cluster.size());
        }
        // step 3. merge based on layer info
        uint64_t boundary_begin = 0;
        for (auto i = 0; i < layer_boundaries.size(); i++) {
            uint64_t merged_num_tuples = 0;
            std::vector<uint32_t> merged_schema;
            std::vector<uint32_t> merged_indices;
            for (auto j = boundary_begin; j < layer_boundaries[i]; j++) {
                auto temp_idx = num_tuples_per_cluster[j].second;
                auto idx = current_merge_state[temp_idx].first;
                auto &schema_group = schema_groups_with_num_tuples[idx];
                merged_schema.insert(merged_schema.end(),
                                    schema_group.first.begin(),
                                    schema_group.first.end());
                merged_indices.insert(merged_indices.end(),
                                    current_merge_state[temp_idx].second.begin(),
                                    current_merge_state[temp_idx].second.end());
                merged_num_tuples += schema_group.second;
                current_merge_state[temp_idx].first = std::numeric_limits<uint32_t>::max();
            }
            std::sort(merged_schema.begin(), merged_schema.end());
            merged_schema.erase(std::unique(merged_schema.begin(), merged_schema.end()), merged_schema.end());
            schema_groups_with_num_tuples.push_back(std::make_pair(std::move(merged_schema), merged_num_tuples));
            current_merge_state.push_back(std::make_pair(schema_groups_with_num_tuples.size() - 1, std::move(merged_indices)));
            boundary_begin = layer_boundaries[i];
        }
        // remove nullptrs
        current_merge_state.erase(
            std::remove_if(begin(current_merge_state), end(current_merge_state),
                            [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
            end(current_merge_state));
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

    void recursive_collect_key_paths_jsonl(ondemand::value element, std::string &current_prefix, bool in_array, vector<uint32_t> &schema, int current_idx) {
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
                            case ondemand::number_type::unsigned_integer:
                                child_type_id = LogicalTypeId::BIGINT;
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
                    case ondemand::number_type::unsigned_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BIGINT);
                        break;
                    // jhha: enfoce a property cannot be the signed and unsigned integer at the same time
                    // case ondemand::number_type::unsigned_integer:
                    //     current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::UBIGINT);
                    //     break;
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
                case ondemand::json_type::null: {
                    // @jhha:
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::VARCHAR);
                    break;
                }
                }

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
                        val_vectors.push_back(int_val);
                        break;
                    }
                    case ondemand::number_type::unsigned_integer:
                        val_vectors.push_back(Value::BIGINT(static_cast<int64_t>(child.value().get_uint64().value())));
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
                // std::cout << "\"" << key << "/" << current_prefix << "\": " << std::endl;
                auto key_idx = property_to_id_map_per_cluster[cluster_id].at(current_prefix);
                recursive_iterate_jsonl(field.value(), current_prefix, in_array, current_idx, key_idx, cluster_id, data);
                current_prefix = old_prefix;
            }
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            ondemand::number_type t = element.get_number_type();
            switch(t) {
            case ondemand::number_type::signed_integer: {
                int64_t *column_ptr = (int64_t *)data.data[current_col_idx].GetData();
                column_ptr[current_idx] = element.get_int64();
                FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
                break;
            }
            case ondemand::number_type::unsigned_integer: {
                // jhha: assume unsigned integer column's type is int64_t
                int64_t *column_ptr = (int64_t *)data.data[current_col_idx].GetData();
                column_ptr[current_idx] = static_cast<int64_t>(element.get_uint64());
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
            // @jhha: think this value as a empty string (temporal)
            std::string_view string_val = "";
            auto data_ptr = data.data[current_col_idx].GetData();
            ((string_t *)data_ptr)[current_idx] = StringVector::AddStringOrBlob(data.data[current_col_idx], 
                                                    (const char*)string_val.data(), string_val.size());
            FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
            break;
        }
        }
    }

    void recursive_iterate_json(ondemand::value element, std::string current_prefix, bool in_array, int current_idx, int current_col_idx, DataChunk &data) { 
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
    std::string input_json_file_path;

    SchemaGroups schema_groups_with_num_tuples;
    vector<uint64_t> corresponding_schemaID;
    vector<int32_t> sg_to_cluster_vec;
    vector<string> id_to_property_vec;
    vector<uint64_t> property_freq_vec;
    vector<uint64_t> schema_property_freq_vec;
    vector<uint64_t> order;
    unordered_map<string, uint64_t> property_to_id_map;
    vector<unordered_map<string, uint64_t>> property_to_id_map_per_cluster;
    uint64_t propertyIDver = 0;
    SchemaHashTable sch_HT;
    size_t num_clusters;
    vector<vector<uint32_t>> cluster_tokens;

    vector<LogicalType> most_common_schema;
    vector<string> most_common_key_paths;
    ExtentManager *ext_mng;
    std::shared_ptr<ClientContext> client;
    Catalog *cat_instance;
    PropertySchemaCatalogEntry *property_schema_cat;
    vector<PropertySchemaCatalogEntry *> property_schema_cats;
    bool load_edge = false;
    vector<idx_t> key_column_idxs;
    unordered_map<LidPair, idx_t, boost::hash<LidPair>> *lid_to_pid_map_instance;
    vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> *lid_to_pid_map;
    PyObject* p_sklearn_module = nullptr;

    // Tip: for Yago-tiny, set CostNullVal to 0.005 and CostSchemaVal to 300. It creates two clusters
    const double CostSchemaVal = 100;
    const double CostNullVal = 0.3;
    const double CostVectorizationVal = 10000;
};

} // namespace duckdb