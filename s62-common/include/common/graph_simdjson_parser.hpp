#pragma once

#include <iostream>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <unordered_set>
#include <Python.h>

#include "common/vector.hpp"
#include "common/enums/json_file_type.hpp"
#include "schemaless/schema_hash_table.hpp"
#include "schemaless/ssj/allpairs_cluster.h"
#include "simdjson.h"
#include "fptree.hpp"
#include "icecream.hpp"
#include "clustering/dbscan.h"
#include "clustering/optics.hpp"

using namespace simdjson;

#define TILE_SIZE 1024 // or 4096
#define NEO4J_VERTEX_ID_NAME "id"
#define COST_MAX 10000000000.00
#define COST_MIN 0

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

// static variable
std::chrono::duration<double> fpgrowth_duration;

namespace s62 {

#ifndef LIDPAIR
#define LIDPAIR
typedef std::pair<idx_t, idx_t> LidPair;
#endif

class GraphSIMDJSONFileParser {

/** CONFIGURATIONS **/
public:
    enum class ClusterAlgorithmType {
        ALLPAIRS,
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

        sch_HT.resize(1000);

        cluster_algo = new AllPairsCluster<Jaccard>(SET_SIM_THRESHOLD);
    }

    GraphSIMDJSONFileParser(std::shared_ptr<ClientContext> client_, ExtentManager *ext_mng_, Catalog *cat_instance_, double set_sim_threshold) {
        client = client_;
        ext_mng = ext_mng_;
        cat_instance = cat_instance_;

        sch_HT.resize(1000);

        cluster_algo = new AllPairsCluster<Jaccard>(set_sim_threshold);
    }

    void SetLidToPidMap (vector<std::pair<string, unordered_map<LidPair, idx_t, boost::hash<LidPair>>>> *lid_to_pid_map_) {
        load_edge = true;
        lid_to_pid_map = lid_to_pid_map_;
    }

    uint64_t GetNewPropertyID() {
        return propertyIDver++;
    }

    void StoreLidToPidInfo(DataChunk &data, vector<idx_t> &key_column_idxs, ExtentID eid) {
        D_ASSERT(key_column_idxs.size() == 1);
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
        input_json_file_path = std::string(json_file_path);
        json_file_type = jftype;

        if (jftype == JsonFileType::JSON) {
            // Load & Parse JSON File
            json = padded_string::load(input_json_file_path);
            // docs = parser.iterate_many(json);
            doc = parser.iterate(json);
            transactions.resize(TILE_SIZE);
        } else if (jftype == JsonFileType::JSONL) {
            json = padded_string::load(input_json_file_path);
            docs = parser.iterate_many(json);
            transactions.resize(TILE_SIZE);
        }
        return 0;
    }

    size_t InitJsonFile() {
        if (json_file_type == JsonFileType::JSON) {
            // Load & Parse JSON File
            json = padded_string::load(input_json_file_path);
            // docs = parser.iterate_many(json);
            doc = parser.iterate(json);
            transactions.resize(TILE_SIZE);
        } else if (json_file_type == JsonFileType::JSONL) {
            json = padded_string::load(input_json_file_path);
            docs = parser.iterate_many(json);
            transactions.resize(TILE_SIZE);
        }
        return 0;
    }

    void LoadJson(string &label_name, vector<string> &label_set, const char *json_key, DataChunk &data, JsonFileType jftype, GraphCatalogEntry *graph_cat, PartitionCatalogEntry *partition_cat, GraphComponentType gctype = GraphComponentType::INVALID) {
        boost::timer::cpu_timer clustering_timer;
        if (jftype == JsonFileType::JSON) {
            throw NotImplementedException("LoadJson for JSON is now deprecated");
        } else if (jftype == JsonFileType::JSONL) {
            switch (cluster_algo_type) {
            case ClusterAlgorithmType::ALLPAIRS: {
                // Extract Schema & Preprocessing
                _ExtractSchema(gctype);
                _PreprocessSchemaForClustering();

                clustering_timer.start();
                // Clustering
                _ClusterSchema();
                clustering_timer.stop();

                // Create Extents
                _CreateExtents(gctype, graph_cat, label_name, label_set);
                break;
            }
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
    }

    void _ExtractSchema(GraphComponentType gctype) {
        if (gctype == GraphComponentType::INVALID) {
            throw NotImplementedException("ExtractSchema for INVALID is not implemented yet");
        } else if (gctype == GraphComponentType::VERTEX) {
            int num_tuples = 0;
            for (auto doc_ : docs) { // iterate each jsonl document; one for each vertex
                // properties object has vertex properties; assume Neo4J dump file format
                std::vector<uint32_t> tmp_vec;

                string current_prefix = "";
                recursive_collect_key_paths_jsonl(doc_["properties"], current_prefix, true, tmp_vec, num_tuples);

                int64_t schema_id;
                sch_HT.find(tmp_vec, schema_id);
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

            return;
        } else if (gctype == GraphComponentType::EDGE) {
            D_ASSERT(false); // not implemented yet
            for (auto doc_ : docs) {
                std::string_view type = doc_["type"].get_string();

                ondemand::value labels = doc_["labels"];
                D_ASSERT(labels.type() == ondemand::json_type::array);
            }
        }
    }

    void _PreprocessSchemaForClustering(bool use_setsim_algo = true) {
        order.resize(schema_property_freq_vec.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return schema_property_freq_vec[a] < schema_property_freq_vec[b];
        });

        if (!use_setsim_algo) return;

        vector<uint64_t> inv_order(order.size());
        for (size_t i = 0; i < order.size(); i++) {
            inv_order[order[i]] = i;
        }

        for (size_t i = 0; i < schema_groups_with_num_tuples.size(); i++) {
            IntRecord rec;
            // rec.recordid = i;
            for (size_t j = 0; j < schema_groups_with_num_tuples[i].first.size(); j++) {
                rec.tokens.push_back(inv_order[schema_groups_with_num_tuples[i].first[j]]);
            }
            std::sort(rec.tokens.begin(), rec.tokens.end(), [&](size_t a, size_t b) {
                return a < b;
            });
            cluster_algo->addrecord(rec);
        }
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


    void _ClusterSchema() {
        cluster_algo->doindex();
        cluster_algo->docluster();

        auto &cluster_to_rid_lists = cluster_algo->getctorlists();
        sg_to_cluster_vec.resize(schema_groups_with_num_tuples.size(), -1);
        for (size_t i = 0; i < cluster_to_rid_lists.size(); i++) {
            for (size_t j = 0; j < cluster_to_rid_lists[i].size(); j++) {
                D_ASSERT(sg_to_cluster_vec.size() > cluster_to_rid_lists[i][j]);
                D_ASSERT(sg_to_cluster_vec[cluster_to_rid_lists[i][j]] == -1);
                sg_to_cluster_vec[cluster_to_rid_lists[i][j]] = i;
            }
        }
        num_clusters = cluster_to_rid_lists.size();
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
            for (auto j = 0; j < clusters[i].size(); j++) {
                sg_to_cluster_vec[clusters[i][j]] = i;
                cluster_tokens_set.insert(
                    std::begin(schema_groups_with_num_tuples[clusters[i][j]].first),
                    std::end(schema_groups_with_num_tuples[clusters[i][j]].first));
            }

            cluster_tokens.push_back(std::vector<uint32_t>());
            cluster_tokens.back().insert(
                std::end(cluster_tokens.back()), std::begin(cluster_tokens_set),
                std::end(cluster_tokens_set));
        }

        size_t num_clusters_before = clusters.size();
        for (auto i = 0; i < noise.size(); i++) {
            sg_to_cluster_vec[noise[i]] = num_clusters_before + i;
            cluster_tokens.push_back(
                std::move(schema_groups_with_num_tuples[noise[i]].first));
        }
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

        vector<std::pair<uint32_t, std::vector<uint32_t>>> temp_output;
        for (uint32_t i = 0; i < num_layers; i++) {
            size_t size_to_reserve = i == 0 ? layer_boundaries[i]
                                            : temp_output.size() +
                                                  layer_boundaries[i] -
                                                  layer_boundaries[i - 1];
            temp_output.reserve(size_to_reserve);
            for (uint32_t j = i == 0 ? 0 : layer_boundaries[i - 1];
                 j < layer_boundaries[i]; j++) {
                std::vector<uint32_t> temp_vec;
                temp_vec.push_back(num_tuples_order[j]);
                temp_output.push_back(std::make_pair(num_tuples_order[j], std::move(temp_vec)));
            }
            ClusterSchemasInCurrentLayer(schema_groups_with_num_tuples,
                                         num_tuples_order, layer_boundaries,
                                         i, temp_output, cost_model);
            
            // remove nullptrs
            temp_output.erase(
                std::remove_if(begin(temp_output), end(temp_output),
                               [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
                end(temp_output));
        }

        std::cout << "Number of final clusters: " << temp_output.size() << std::endl;

        if (merge_in_advance == MergeInAdvance::IN_STORAGE) {
            _MergeInAdvance(temp_output);
        }

        num_clusters = temp_output.size();
        cluster_tokens.reserve(temp_output.size());
        for (auto i = 0; i < temp_output.size(); i++) {
            if (temp_output[i].first == std::numeric_limits<uint32_t>::max()) { continue; }

            cluster_tokens.push_back(std::move(schema_groups_with_num_tuples[temp_output[i].first].first));
            std::sort(cluster_tokens.back().begin(), cluster_tokens.back().end());

            for (auto j = 0; j < temp_output[i].second.size(); j++) {
                sg_to_cluster_vec[temp_output[i].second[j]] = i;
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

        vector<std::pair<uint32_t, std::vector<uint32_t>>> temp_output;
        for (uint32_t i = 0; i < num_layers; i++) {
            size_t size_to_reserve = i == 0 ? layer_boundaries[i]
                                            : temp_output.size() +
                                                  layer_boundaries[i] -
                                                  layer_boundaries[i - 1];
            temp_output.reserve(size_to_reserve);
            for (uint32_t j = i == 0 ? 0 : layer_boundaries[i - 1];
                 j < layer_boundaries[i]; j++) {
                std::vector<uint32_t> temp_vec;
                temp_vec.push_back(num_tuples_order[j]);
                temp_output.push_back(std::make_pair(num_tuples_order[j], std::move(temp_vec)));
            }
            ClusterSchemasInCurrentLayer(schema_groups_with_num_tuples,
                                         num_tuples_order, layer_boundaries,
                                         i, temp_output, CostModel::WEIGHTEDJACCARD);
            
            // remove nullptrs
            temp_output.erase(
                std::remove_if(begin(temp_output), end(temp_output),
                               [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
                end(temp_output));
        }
        
        

        // Populate cluster_tokens in sorted order
        num_clusters = temp_output.size();
        cluster_tokens.reserve(temp_output.size());

        for (auto i = 0; i < temp_output.size(); i++) {
            cluster_tokens.push_back(std::move(schema_groups_with_num_tuples[temp_output[i].first].first));
            std::sort(cluster_tokens.back().begin(), cluster_tokens.back().end());

            for (auto j = 0; j < temp_output[i].second.size(); j++) {
                sg_to_cluster_vec[temp_output[i].second[j]] = i;
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

        boost::timer::cpu_timer gmm_timer;
        vector<uint32_t> reference_schema_group;
        _GetReferenceSchemaGroup(_schema_groups_with_num_tuples, reference_schema_group);
        auto gmm_timer_ms = gmm_timer.elapsed().wall / 1000000.0;
        std::cout << "\nGMM Time: "  << gmm_timer_ms << " ms" << std::endl;

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

        // Populate cluster_tokens in sorted order
        for (auto i = 0; i < clusters.size(); i++) {
            std::unordered_set<uint32_t> cluster_tokens_set;
            for (auto j = 0; j < clusters[i].size(); j++) {
                sg_to_cluster_vec[clusters[i][j]] = i;
                cluster_tokens_set.insert(
                    std::begin(schema_groups_with_num_tuples[clusters[i][j]].first),
                    std::end(schema_groups_with_num_tuples[clusters[i][j]].first));
            }

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
    }

    void ClusterSchemasInCurrentLayer(
        vector<std::pair<std::vector<uint32_t>, uint64_t>>
            &schema_groups_with_num_tuples,
        const vector<uint32_t> &num_tuples_order,
        const vector<uint32_t> &layer_boundaries,
        uint32_t current_layer,
        vector<std::pair<uint32_t, std::vector<uint32_t>>> &temp_output,
        const CostModel _cost_model)
    {
        uint32_t merged_count;
        uint32_t iteration = 0;
        uint32_t num_tuples_total = temp_output.size();
        std::vector<bool> visited(num_tuples_total, false);
        do {
            merged_count = 0;

            if (num_tuples_total > std::numeric_limits<uint32_t>::max()) {
                std::cerr << "Number of tuples exceeds the maximum value of uint32_t" << std::endl;
                break;
            }

            /* START_OF_COST_MODEL_BASED */
            auto cost_compare_great = [](const std::pair<double, std::pair<uint32_t, uint32_t>> &a,
                                        const std::pair<double, std::pair<uint32_t, uint32_t>> &b) {
                return a.first > b.first;
            };

            auto cost_compare_less = [](const std::pair<double, std::pair<uint32_t, uint32_t>> &a,
                                        const std::pair<double, std::pair<uint32_t, uint32_t>> &b) {
                return a.first < b.first;
            };

            // Pre-allocate memory for the vector
            std::vector<std::pair<double, std::pair<uint32_t, uint32_t>>> pre_allocated_vector;
            pre_allocated_vector.reserve(num_tuples_total); // Reserve space for all pairwise costs

            std::unique_ptr<std::priority_queue<std::pair<double, std::pair<uint32_t, uint32_t>>,
                                                std::vector<std::pair<double, std::pair<uint32_t, uint32_t>>>,
                                                std::function<bool(const std::pair<double, std::pair<uint32_t, uint32_t>> &,
                                                                const std::pair<double, std::pair<uint32_t, uint32_t>> &)>>> cost_pq_ptr;

            if (_cost_model == CostModel::OURS || _cost_model == CostModel::SETEDIT) {
                cost_pq_ptr = std::make_unique<std::priority_queue<std::pair<double, std::pair<uint32_t, uint32_t>>,
                                                                std::vector<std::pair<double, std::pair<uint32_t, uint32_t>>>,
                                                                std::function<bool(const std::pair<double, std::pair<uint32_t, uint32_t>> &,
                                                                                    const std::pair<double, std::pair<uint32_t, uint32_t>> &)>>>(
                    cost_compare_great, std::move(pre_allocated_vector));
            } else {
                cost_pq_ptr = std::make_unique<std::priority_queue<std::pair<double, std::pair<uint32_t, uint32_t>>,
                                                                std::vector<std::pair<double, std::pair<uint32_t, uint32_t>>>,
                                                                std::function<bool(const std::pair<double, std::pair<uint32_t, uint32_t>> &,
                                                                                    const std::pair<double, std::pair<uint32_t, uint32_t>> &)>>>(
                    cost_compare_less, std::move(pre_allocated_vector));
            }
            /* END_OF_COST_MODEL_BASED */
        
            auto &cost_pq = *cost_pq_ptr;
            const size_t TOPK = 10;
            vector<uint8_t> count_per_tuple;
            count_per_tuple.resize(num_tuples_total, 0);
            #pragma omp parallel for num_threads(32)
            for (uint32_t i = 0; i < num_tuples_total; ++i) {
                if (temp_output[i].first == std::numeric_limits<uint32_t>::max()) {
                    continue;
                }

                // Use a local heap for each tuple to store top-10 costs
                std::priority_queue<std::pair<double, std::pair<uint32_t, uint32_t>>,
                                    std::vector<std::pair<double, std::pair<uint32_t, uint32_t>>>,
                                    std::function<bool(const std::pair<double, std::pair<uint32_t, uint32_t>> &,
                                                    const std::pair<double, std::pair<uint32_t, uint32_t>> &)>>
                    local_heap((_cost_model == CostModel::OURS || _cost_model == CostModel::SETEDIT)
                                ? cost_compare_great
                                : cost_compare_less);

                for (uint32_t j = i + 1; j < num_tuples_total; ++j) {
                    uint32_t group1_idx = temp_output[i].first;
                    uint32_t group2_idx = temp_output[j].first;

                    double cost = (group1_idx == std::numeric_limits<uint32_t>::max() || 
                                group2_idx == std::numeric_limits<uint32_t>::max())
                                    ? (_cost_model == CostModel::OURS || _cost_model == CostModel::SETEDIT ? COST_MAX : COST_MIN)
                                    : CalculateCost(schema_groups_with_num_tuples[group1_idx], 
                                                    schema_groups_with_num_tuples[group2_idx],
                                                    _cost_model,
                                                    schema_groups_with_num_tuples.size());

                    // Skip invalid costs for OURS model
                    if (_cost_model == CostModel::OURS && cost > 0) {
                        continue;
                    } else if (_cost_model == CostModel::SETEDIT && cost > SET_EDIT_THRESHOLD) {
                        continue;
                    } else if (_cost_model == CostModel::OVERLAP && cost < OVERLAP_THRESHOLD) {
                        continue;
                    } else if (_cost_model == CostModel::JACCARD && cost < JACCARD_THRESHOLD) {
                        continue;
                    } else if (_cost_model == CostModel::WEIGHTEDJACCARD && cost < WEIGHTEDJACCARD_THRESHOLD) {
                        continue;
                    } else if (_cost_model == CostModel::COSINE && cost < COSINE_THRESHOLD) {
                        continue;
                    } else if (_cost_model == CostModel::DICE && cost < DICE_THRESHOLD) {
                        continue;
                    }

                    // Add the current cost to the local heap
                    local_heap.push(std::make_pair(cost, std::make_pair(i, j)));

                    // If the heap size exceeds 10, remove the worst (least priority) cost
                    if (local_heap.size() > TOPK) {
                        local_heap.pop();
                    }
                }

                count_per_tuple[i] = local_heap.size();

                while (!local_heap.empty()) {
                    #pragma omp critical
                    {
                        cost_pq.push(local_heap.top());
                    }
                    local_heap.pop();
                }
            }

            boost::timer::cpu_timer merge_timer;
            uint32_t num_tuples_added = 0;
            if (!cost_pq.empty()) {
                do {
                    std::pair<double, std::pair<uint32_t, uint32_t>> min_cost = cost_pq.top();
                    cost_pq.pop();

                    // Extract the indices directly from the pair
                    uint32_t idx1 = min_cost.second.first;
                    uint32_t idx2 = min_cost.second.second;

                    /* START_OF_COST_MODEL_BASED */
                    if (_cost_model == CostModel::OURS) {
                        if (min_cost.first > 0) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::SETEDIT) {
                        if (min_cost.first > SET_EDIT_THRESHOLD) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::JACCARD) {
                        if (min_cost.first < JACCARD_THRESHOLD || min_cost.first == COST_MIN) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::WEIGHTEDJACCARD) {
                        if (min_cost.first < WEIGHTEDJACCARD_THRESHOLD || min_cost.first == COST_MIN) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::COSINE) {
                        if (min_cost.first < COSINE_THRESHOLD || min_cost.first == COST_MIN) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::DICE) {
                        if (min_cost.first < DICE_THRESHOLD || min_cost.first == COST_MIN) {
                            break;
                        }
                    }
                    else if (_cost_model == CostModel::OVERLAP) {
                        if (min_cost.first < OVERLAP_THRESHOLD || min_cost.first == COST_MIN) {
                            break;
                        }
                    }
                    /* END_OF_COST_MODEL_BASED */

                    // Check if either of the indices have been visited
                    if (visited[idx1] || visited[idx2]) {
                        if (visited[idx1] && visited[idx2]) {
                            continue;
                        } else {
                            // one of them is not visited
                            uint32_t not_visited_idx = visited[idx1] ? idx2 : idx1;
                            if (--count_per_tuple[not_visited_idx] == 0) {
                                // refill
                                std::priority_queue<
                                    std::pair<double,
                                            std::pair<uint32_t, uint32_t>>,
                                    std::vector<std::pair<
                                        double, std::pair<uint32_t, uint32_t>>>,
                                    std::function<bool(
                                        const std::pair<
                                            double,
                                            std::pair<uint32_t, uint32_t>> &,
                                        const std::pair<
                                            double,
                                            std::pair<uint32_t, uint32_t>> &)>>
                                    local_heap(
                                        (_cost_model == CostModel::OURS ||
                                        _cost_model == CostModel::SETEDIT)
                                            ? cost_compare_great
                                            : cost_compare_less);

                                for (uint32_t j = 0; j < num_tuples_total;
                                    ++j) {
                                    if (j == not_visited_idx)
                                        continue;
                                    if (visited[j])
                                        continue;
                                    uint32_t group1_idx =
                                        temp_output[not_visited_idx].first;
                                    uint32_t group2_idx = temp_output[j].first;

                                    double cost =
                                        (group1_idx == std::numeric_limits<
                                                        uint32_t>::max() ||
                                        group2_idx == std::numeric_limits<
                                                        uint32_t>::max())
                                            ? (_cost_model == CostModel::OURS ||
                                                    _cost_model ==
                                                        CostModel::SETEDIT
                                                ? COST_MAX
                                                : COST_MIN)
                                            : CalculateCost(
                                                schema_groups_with_num_tuples
                                                    [group1_idx],
                                                schema_groups_with_num_tuples
                                                    [group2_idx],
                                                _cost_model,
                                                schema_groups_with_num_tuples
                                                    .size());

                                    // Skip invalid costs for OURS model
                                    if (_cost_model == CostModel::OURS &&
                                        cost > 0) {
                                        continue;
                                    }

                                    // Add the current cost to the local heap
                                    local_heap.push(std::make_pair(
                                        cost, std::make_pair(not_visited_idx, j)));

                                    // If the heap size exceeds 10, remove the worst (least priority) cost
                                    if (local_heap.size() > TOPK) {
                                        local_heap.pop();
                                    }
                                }

                                count_per_tuple[not_visited_idx] = local_heap.size();

                                while (!local_heap.empty()) {
                                    cost_pq.push(local_heap.top());
                                    local_heap.pop();
                                }
                            }
                        }
                        continue;
                    }

                    visited[idx1] = true;
                    visited[idx2] = true;
                    count_per_tuple[idx1] = 0;
                    count_per_tuple[idx2] = 0;

                    // Merge the two indices
                    MergeVertexlets(idx1, idx2, temp_output);
                    num_tuples_added++;
                    merged_count++;
                    visited.push_back(false);
                } while (!cost_pq.empty());
            }
            
            auto merge_time_ms = merge_timer.elapsed().wall / 1000000.0;
            
            num_tuples_total += num_tuples_added;
            iteration++;
        } while (merged_count > 0);
    }

    void MergeVertexlets(uint32_t idx1, uint32_t idx2, 
                        std::vector<std::pair<uint32_t, std::vector<uint32_t>>> &temp_output) {
        // Check if indices are within bounds for temp_output
        if (idx1 >= temp_output.size() || idx2 >= temp_output.size()) {
            std::cerr << "Error: Index out of bounds for temp_output. "
                    << "idx1: " << idx1 << ", idx2: " << idx2 
                    << ", temp_output.size(): " << temp_output.size() << std::endl;
            return;
        }

        // Check if the first indices from temp_output are valid for schema_groups_with_num_tuples
        if (temp_output[idx1].first >= schema_groups_with_num_tuples.size() || 
            temp_output[idx2].first >= schema_groups_with_num_tuples.size()) {
            std::cerr << "Error: Invalid schema group index from temp_output. "
                    << "temp_output[idx1].first: " << temp_output[idx1].first 
                    << ", temp_output[idx2].first: " << temp_output[idx2].first 
                    << ", schema_groups_with_num_tuples.size(): " << schema_groups_with_num_tuples.size() << std::endl;
            return;
        }

        // Safely retrieve schema groups
        auto &schema_group1 = schema_groups_with_num_tuples[temp_output[idx1].first];
        auto &schema_group2 = schema_groups_with_num_tuples[temp_output[idx2].first];

        // Merge schema vectors
        std::vector<uint32_t> merged_schema;
        try {
            merged_schema.reserve(schema_group1.first.size() + schema_group2.first.size());
            std::merge(schema_group1.first.begin(), schema_group1.first.end(),
                    schema_group2.first.begin(), schema_group2.first.end(),
                    std::back_inserter(merged_schema));
            merged_schema.erase(std::unique(merged_schema.begin(), merged_schema.end()), merged_schema.end());
        } catch (const std::exception &e) {
            std::cerr << "Error during schema merging: " << e.what() << std::endl;
            return;
        }

        // Compute merged number of tuples
        uint64_t merged_num_tuples = schema_group1.second + schema_group2.second;

        // Add new merged schema group
        schema_groups_with_num_tuples.push_back(std::make_pair(std::move(merged_schema), merged_num_tuples));

        // Merge indices
        std::vector<uint32_t> merged_indices;
        try {
            merged_indices.reserve(temp_output[idx1].second.size() + temp_output[idx2].second.size());
            merged_indices.insert(merged_indices.end(), temp_output[idx1].second.begin(), temp_output[idx1].second.end());
            merged_indices.insert(merged_indices.end(), temp_output[idx2].second.begin(), temp_output[idx2].second.end());
        } catch (const std::exception &e) {
            std::cerr << "Error during index merging: " << e.what() << std::endl;
            return;
        }

        // Add new merged entry to temp_output
        temp_output.push_back(std::make_pair(schema_groups_with_num_tuples.size() - 1, std::move(merged_indices)));

        // Invalidate the original entries in temp_output
        if (idx1 < temp_output.size()) {
            temp_output[idx1].first = std::numeric_limits<uint32_t>::max();
        } else {
            std::cerr << "Warning: Invalidating idx1 failed as idx1 is now out of bounds." << std::endl;
        }

        if (idx2 < temp_output.size()) {
            temp_output[idx2].first = std::numeric_limits<uint32_t>::max();
        } else {
            std::cerr << "Warning: Invalidating idx2 failed as idx2 is now out of bounds." << std::endl;
        }
    }

    vector<unsigned int> &GetClusterTokens(size_t cluster_idx) {
        switch(cluster_algo_type) {
            case ClusterAlgorithmType::ALLPAIRS:
                return cluster_algo->getclustertokens(cluster_idx);
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
                    uint64_t original_idx;
                    if (cluster_algo_type == ClusterAlgorithmType::ALLPAIRS) {
                        original_idx = order[tokens[token_idx]];
                    } else {
                        original_idx = tokens[token_idx];
                    }
                    
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
            docs = parser.iterate_many(json);
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

            // Destroy datas
            for (size_t i = 0; i < num_clusters_to_process; i++) {
                datas[i].Destroy();
            }

            // Update cluster_id range
            start_cluster_idx += CLUSTER_LOAD_CHUNK;
            end_cluster_idx += CLUSTER_LOAD_CHUNK;
            if (end_cluster_idx >= num_clusters) end_cluster_idx = num_clusters;
        }
    }

    void _CreateEdgeExtents(GraphCatalogEntry *graph_cat, string &label_name, vector<string> &label_set) {
    }

    void _MergeInAdvance(vector<std::pair<uint32_t, std::vector<uint32_t>>> &temp_output) {
        D_ASSERT(merge_in_advance == MergeInAdvance::IN_STORAGE);
        // merge additionally based on cardinality
        // step 1. extract card & sort
        std::vector<std::pair<uint64_t, uint64_t>> num_tuples_per_cluster;
        num_tuples_per_cluster.reserve(temp_output.size());
        for (auto i = 0; i < temp_output.size(); i++) {
            if (temp_output[i].first == std::numeric_limits<uint32_t>::max()) { continue; }
            num_tuples_per_cluster.push_back(std::make_pair(
                schema_groups_with_num_tuples[temp_output[i].first].second, i));
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
                auto idx = temp_output[temp_idx].first;
                auto &schema_group = schema_groups_with_num_tuples[idx];
                merged_schema.insert(merged_schema.end(),
                                    schema_group.first.begin(),
                                    schema_group.first.end());
                merged_indices.insert(merged_indices.end(),
                                    temp_output[temp_idx].second.begin(),
                                    temp_output[temp_idx].second.end());
                merged_num_tuples += schema_group.second;
                temp_output[temp_idx].first = std::numeric_limits<uint32_t>::max();
            }
            std::sort(merged_schema.begin(), merged_schema.end());
            merged_schema.erase(std::unique(merged_schema.begin(), merged_schema.end()), merged_schema.end());
            schema_groups_with_num_tuples.push_back(std::make_pair(std::move(merged_schema), merged_num_tuples));
            temp_output.push_back(std::make_pair(schema_groups_with_num_tuples.size() - 1, std::move(merged_indices)));
            boundary_begin = layer_boundaries[i];
        }
        // remove nullptrs
        temp_output.erase(
            std::remove_if(begin(temp_output), end(temp_output),
                            [](auto &x) { return x.first == std::numeric_limits<uint32_t>::max(); }),
            end(temp_output));
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
                    current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::VARCHAR);
                    break;
                }
                }

                if (field.value().type() != ondemand::json_type::object) { 
                    uint64_t prop_id;
                    auto it = property_to_id_map.find(current_prefix);
                    if (it == property_to_id_map.end()) {
                        prop_id = GetNewPropertyID();
                        D_ASSERT(id_to_property_vec.size() == prop_id);
                        D_ASSERT(property_freq_vec.size() == prop_id);

                        property_to_id_map.insert({current_prefix, prop_id});
                        id_to_property_vec.push_back(current_prefix);
                        property_freq_vec.push_back(1);
                    } else {
                        prop_id = it->second;
                        D_ASSERT(prop_id < property_freq_vec.size());
                        property_freq_vec[prop_id]++;
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
                    const uint64_t minimum_support_threshold = 2;
                    const FPTree fptree{ transactions, minimum_support_threshold };
                    const std::set<Pattern> patterns = fptree_growth( fptree );
                    auto fpgrowth_end = std::chrono::high_resolution_clock::now();
                    fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

                    Pattern *current_mfp = nullptr;
                    for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
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
                const uint64_t minimum_support_threshold = 2;
                const FPTree fptree{ transactions, minimum_support_threshold };
                const std::set<Pattern> patterns = fptree_growth( fptree );
                auto fpgrowth_end = std::chrono::high_resolution_clock::now();
                fpgrowth_duration += (fpgrowth_end - fpgrowth_start);

                Pattern *current_mfp = nullptr;
                for (std::set<Pattern>::iterator it = patterns.begin(); it != patterns.end(); ++it) {
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
                    case ondemand::number_type::unsigned_integer:
                        current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::BIGINT);
                        break;
                    // case ondemand::number_type::unsigned_integer:
                    //     current_prefix = current_prefix + std::string("_") + std::to_string((uint8_t)LogicalTypeId::UBIGINT);
                    //     break;
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
            break;
        }
        case ondemand::json_type::string: {
            // get_string() would return escaped string, but
            // we are happy with unescaped string.
            break;
        }
        case ondemand::json_type::boolean: {
            break;
        }
        case ondemand::json_type::null: {
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
            ((string_t *)data_ptr)[current_idx] = StringVector::AddStringOrBlob(data.data[current_col_idx], 
                                                    (const char*)string_val.data(), string_val.size());
            FlatVector::Validity(data.data[current_col_idx]).Set(current_idx, true);
            break;
        }
        case ondemand::json_type::boolean: {
            break;
        }
        case ondemand::json_type::null: {
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
                        data.SetCardinality(num_tuples);
                        ExtentID new_eid;
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
                        }
                        num_tuples = 0;
                        data.Reset(STORAGE_STANDARD_VECTOR_SIZE);
                    }
                }
            }
            if (old_in_array) {
                data.SetValue(current_col_idx, current_idx, Value::LIST(val_vectors));
            }

            in_array = false;
            if (num_tuples > 0) {
                // CreateExtent
                data.SetCardinality(num_tuples);
                ExtentID new_eid;
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
                auto key_it = std::find(most_common_key_paths.begin(), most_common_key_paths.end(), current_prefix);
                int key_idx = std::distance(most_common_key_paths.begin(), key_it);
                recursive_iterate_json(field.value(), current_prefix, in_array, current_idx, key_idx, data);
                current_prefix = old_prefix;
            }
            break;
        }
        case ondemand::json_type::number: {
            // assume it fits in a double
            ondemand::number_type t = element.get_number_type();
            switch(t) {
            case ondemand::number_type::signed_integer:
                if (most_common_schema[current_col_idx] == LogicalType::BIGINT) {
                    int64_t *column_ptr = (int64_t *)data.data[current_col_idx].GetData();
                    column_ptr[current_idx] = element.get_int64();
                }
                break;
            case ondemand::number_type::unsigned_integer:
                if (most_common_schema[current_col_idx] == LogicalType::UBIGINT) {
                    uint64_t *column_ptr = (uint64_t *)data.data[current_col_idx].GetData();
                    column_ptr[current_idx] = element.get_uint64();
                }
                break;
            case ondemand::number_type::floating_point_number:
                if (most_common_schema[current_col_idx] == LogicalType::DOUBLE) {
                    double *column_ptr = (double *)data.data[current_col_idx].GetData();
                    column_ptr[current_idx] = element.get_double();
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
            ((string_t *)data_ptr)[current_idx] = StringVector::AddStringOrBlob(data.data[current_col_idx], 
                                                    (const char*)string_val.data(), string_val.size());
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
    JsonFileType json_file_type;

    vector<vector<Item>> transactions;

    vector<uint64_t> corresponding_schemaID;
    vector<std::pair<vector<uint32_t>, uint64_t>> schema_groups_with_num_tuples;
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
    vector<vector<uint32_t>> cluster_tokens;

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
    PyObject* p_sklearn_module = nullptr;

    // Tip: for Yago-tiny, set CostNullVal to 0.005 and CostSchemaVal to 300. It creates two clusters
    const double CostSchemaVal = 300;
    const double CostNullVal = 0.01;
    const double CostVectorizationVal = 10;
};

} // namespace s62