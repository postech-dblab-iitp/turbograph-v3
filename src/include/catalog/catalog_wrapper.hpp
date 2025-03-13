#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/coalescing.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "function/function.hpp"
#include "optimizer/mdprovider/MDProviderTBGPP.h"

#include "icecream.hpp"

#include <tuple>
#include <unordered_map>

namespace duckdb {

class CatalogWrapper {

public:
    CatalogWrapper(DatabaseInstance &db) : db(db) {}
    ~CatalogWrapper() {}

    void GetEdgeAndConnectedSrcDstPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs,
        vector<uint64_t> &srcPartitionIDs, vector<uint64_t> &dstPartitionIDs, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            srcPartitionIDs.push_back(p_cat->GetSrcPartOid());
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    void GetSchemas(ClientContext &context, unordered_map<idx_t, vector<PropertyKeyID>>& TableOidsSchemas, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        vector<string> labelset_names;
        gcat->GetVertexLabels(labelset_names);

        for (auto &labelset_name : labelset_names) {
            vector<uint64_t> partitionIDs;
            vector<idx_t> oids;
            vector<string> partition_labelset_names = {labelset_name};
            partitionIDs = std::move(gcat->LookupPartition(context, partition_labelset_names, g_type));
            for (auto &pid : partitionIDs) {
                PartitionCatalogEntry *p_cat =
                    (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
                p_cat->GetPropertySchemaIDs(oids);
                for (auto &oid : oids) {
                    PropertySchemaCatalogEntry *ps_cat =
                    (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, oid);
                    auto key_ids_ptr = ps_cat->GetKeyIDs();
                    vector<PropertyKeyID> key_ids(key_ids_ptr->begin(), key_ids_ptr->end());
                    TableOidsSchemas.insert({oid, key_ids});
                }
            }
        }
    }

    void GetPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));
    }

    void GetSubPartitionIDsFromPartitions(ClientContext &context, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, vector<size_t> &numOidsPerPartition,
                                        GraphComponentType g_type, bool exclude_fakes = true) {
        auto &catalog = db.GetCatalog();

        for (auto &pid : partitionIDs) {
            vector<idx_t> sub_partition_oids;
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(sub_partition_oids);

            if (exclude_fakes) {
                size_t numValidOids = 0;
                for (auto &oid : sub_partition_oids) {
                    PropertySchemaCatalogEntry *ps_cat =
                        (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, oid);
                    if (!ps_cat->is_fake) {
                        oids.push_back(oid);
                        numValidOids++;
                    }
                }
                numOidsPerPartition.push_back(numValidOids);
            }
            else {
                oids.insert(oids.end(), sub_partition_oids.begin(), sub_partition_oids.end());
                numOidsPerPartition.push_back(sub_partition_oids.size());
            }
        }
    }

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    void GetConnectedEdgeSubPartitionIDs(ClientContext &context, vector<idx_t> &src_oids, vector<uint64_t> &partitionIDs, vector<uint64_t> &dstPartitionIDs) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        for (auto &src_oid : src_oids) {
            gcat->GetConnectedEdgeOids(context, src_oid, partitionIDs);
        }

        for (auto &edge_pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, edge_pid);
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    void GetConnectedEdgeSubPartitionIDs(ClientContext &context, vector<idx_t> &src_oids, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, vector<uint64_t> &dstPartitionIDs) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        for (auto &src_oid : src_oids) {
            gcat->GetConnectedEdgeOids(context, src_oid, partitionIDs);
        }

        for (auto &edge_pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, edge_pid);
            p_cat->GetPropertySchemaIDs(oids);
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    idx_t GetAggFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {
        auto &catalog = db.GetCatalog();
        AggregateFunctionCatalogEntry *aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetFuncEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, func_name, true);
        
        if (aggfunc_cat == nullptr) { throw InvalidInputException("Unsupported agg func name: " + func_name); }

        auto &agg_funcset = aggfunc_cat->functions->functions;
        D_ASSERT(agg_funcset.size() <= FUNC_GROUP_SIZE);
        std::string error_msg;
        idx_t agg_funcset_idx =
            Function::BindFunction(func_name, agg_funcset, arguments, error_msg);
        
        if (agg_funcset_idx == DConstants::INVALID_INDEX) { throw InvalidInputException("Unsupported agg func"); }

        idx_t aggfunc_mdid = FUNCTION_BASE_ID + (aggfunc_cat->GetOid() * FUNC_GROUP_SIZE) + agg_funcset_idx;
        return aggfunc_mdid;
    }
    
    idx_t GetScalarFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {
        auto &catalog = db.GetCatalog();
        ScalarFunctionCatalogEntry *scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetFuncEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, func_name, true);

        if (scalarfunc_cat == nullptr) { throw InvalidInputException("Unsupported scalar func name: " + func_name); }

        auto &scalar_funcset = scalarfunc_cat->functions->functions;
        D_ASSERT(scalar_funcset.size() <= FUNC_GROUP_SIZE);
        std::string error_msg;
        idx_t scalar_funcset_idx =
            Function::BindFunction(func_name, scalar_funcset, arguments, error_msg);
            
        if (scalar_funcset_idx == DConstants::INVALID_INDEX) { throw InvalidInputException("Unsupported scalar func"); }
        idx_t scalarfunc_mdid = FUNCTION_BASE_ID + (scalarfunc_cat->GetOid() * FUNC_GROUP_SIZE) + scalar_funcset_idx;
        return scalarfunc_mdid;
    }

    PartitionCatalogEntry *GetPartition(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat;
    }

    PropertySchemaCatalogEntry *RelationIdGetRelation(ClientContext &context, idx_t rel_oid) {
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, rel_oid);
        return ps_cat;
    }

    idx_t GetRelationPhysicalIDIndex(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetPhysicalIDIndexOid();
    }

    idx_t_vector *GetRelationAdjIndexes(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetAdjIndexOidVec();
    }

    idx_t_vector *GetRelationPropertyIndexes(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetPropertyIndexOidVec();
    }

    IndexCatalogEntry *GetIndex(ClientContext &context, idx_t index_oid) {
        auto &catalog = db.GetCatalog();
        IndexCatalogEntry *index_cat =
            (IndexCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, index_oid);
        return index_cat;
    }

    AggregateFunctionCatalogEntry *GetAggFunc(ClientContext &context, idx_t aggfunc_oid) {
        idx_t aggfunc_oid_ = (aggfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        AggregateFunctionCatalogEntry *aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetFuncEntry(context, DEFAULT_SCHEMA, aggfunc_oid_);
        return aggfunc_cat;
    }

    void GetAggFuncAndIdx(ClientContext &context, idx_t aggfunc_oid, AggregateFunctionCatalogEntry *&aggfunc_cat,
        idx_t &function_idx) {
        idx_t aggfunc_oid_ = (aggfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        function_idx = (aggfunc_oid - FUNCTION_BASE_ID) % FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetFuncEntry(context, DEFAULT_SCHEMA, aggfunc_oid_);
    }

    ScalarFunctionCatalogEntry *GetScalarFunc(ClientContext &context, idx_t scalarfunc_oid) {
        idx_t scalarfunc_oid_ = (scalarfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        ScalarFunctionCatalogEntry *scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetFuncEntry(context, DEFAULT_SCHEMA, scalarfunc_oid_);
        return scalarfunc_cat;
    }

    void GetScalarFuncAndIdx(ClientContext &context, idx_t scalarfunc_oid, ScalarFunctionCatalogEntry *&scalarfunc_cat,
        idx_t &function_idx) {
        idx_t scalarfunc_oid_ = (scalarfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        function_idx = (scalarfunc_oid - FUNCTION_BASE_ID) % FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetFuncEntry(context, DEFAULT_SCHEMA, scalarfunc_oid_);
    }

    void GetPropertyKeyToPropertySchemaMap(
        ClientContext &context, unordered_map<idx_t, vector<std::pair<uint64_t, uint64_t>>>
            &property_schema_index,
        vector<idx_t> &universal_schema_ids,
        vector<LogicalTypeId> &universal_types_id, vector<idx_t> &part_oids)
    {
        auto &catalog = db.GetCatalog();
        const void_allocator void_alloc =
            catalog.catalog_segment->get_segment_manager();

        // Concat all property keys and types
        for (auto &part_oid : part_oids) {
            PartitionCatalogEntry *part_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, part_oid);

            auto part_universal_schema_ids =
                part_cat->GetUniversalPropertyKeyIds();
            auto part_universal_types_id =
                part_cat->GetUniversalPropertyTypeIds();
            auto part_property_schema_index =
                part_cat->GetPropertySchemaIndex();
            auto part_property_names =
                part_cat->GetUniversalPropertyKeyNames();
            
            if (universal_schema_ids.empty()) {
                universal_schema_ids.reserve(part_universal_schema_ids->size());
                universal_types_id.reserve(part_universal_types_id->size());
                property_schema_index.reserve(part_property_schema_index->size());
            }

            // Merge
            for (auto i = 0; i < part_universal_schema_ids->size(); i++) {
                duckdb::idx_t property_key_id =
                    part_universal_schema_ids->at(i);
                auto it = property_schema_index.find(property_key_id);
                if (it == property_schema_index.end()) {
                    universal_schema_ids.push_back(
                        part_universal_schema_ids->at(i));
                    universal_types_id.push_back(
                        part_universal_types_id->at(i));
                    auto it = part_property_schema_index->find(property_key_id);
                    vector<std::pair<uint64_t, uint64_t>> pairs;
                    pairs.reserve(it->second.size());
                    for (const auto& pair : it->second) {
                        pairs.push_back(pair);
                    }
                    property_schema_index.emplace(property_key_id, std::move(pairs));
                }
                else {
                    auto &original_pair_vector = it->second;
                    auto &new_pair_vector =
                        part_property_schema_index->find(property_key_id)
                            ->second;
                    original_pair_vector.insert(original_pair_vector.end(),
                                                new_pair_vector.begin(),
                                                new_pair_vector.end());
                }
            }
        }
    }

    string GetTypeName(idx_t type_id) {
        return LogicalTypeIdToString((LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
    }

    idx_t GetTypeSize(idx_t type_id) {
        LogicalTypeId type_id_ = (LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);
        uint16_t extra_info = ((type_id - LOGICAL_TYPE_BASE_ID) / NUM_MAX_LOGICAL_TYPES);
        if (type_id_ == LogicalTypeId::DECIMAL) {
            uint8_t width = (uint8_t)(extra_info >> 8);
            uint8_t scale = (uint8_t)(extra_info & 0xFF);
            LogicalType tmp_type = LogicalType::DECIMAL(width, scale);
            return GetTypeIdSize(tmp_type.InternalType());
        } else {
            LogicalType tmp_type(type_id_);
            return GetTypeIdSize(tmp_type.InternalType());
        }
    }

    bool isTypeFixedLength(idx_t type_id) {
        LogicalType tmp_type((LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
        return TypeIsConstantSize(tmp_type.InternalType());
    }

    idx_t GetAggregate(ClientContext &context, const char *aggname, idx_t type_id, int nargs) {
        auto &catalog = db.GetCatalog();
        auto *func = (AggregateFunctionCatalogEntry *)catalog.GetFuncEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, aggname);
        return func->GetOid();
    }

    idx_t GetComparisonOperator(idx_t left_type_id, idx_t right_type_id, ExpressionType etype) {
        return OPERATOR_BASE_ID
            + (((idx_t) etype) * (256 * 256))
            + (((left_type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES) * 256)
            + ((right_type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);
    }

    inline ExpressionType GetComparisonType(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return etype;
    }

    inline idx_t GetLeftTypeId(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256;
    }

    inline idx_t GetRightTypeId(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) % (256));
    }

    string GetOpName(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return ExpressionTypeToOrcaString(etype);
    }

    void GetOpInputTypes(idx_t op_id, idx_t &left_type_id, idx_t &right_type_id) {
        left_type_id = ((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256 + LOGICAL_TYPE_BASE_ID;
        right_type_id = ((op_id - OPERATOR_BASE_ID) % (256)) + LOGICAL_TYPE_BASE_ID;
    }

    idx_t GetOpFunc(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) / (256 * 256)) + EXPRESSION_TYPE_BASE_ID;
    }

    idx_t GetCommutatorOp(idx_t op_id) {
        idx_t left_type_id = GetLeftTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        idx_t right_type_id = GetRightTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        ExpressionType etype = GetComparisonType(op_id);
        return GetComparisonOperator(right_type_id, left_type_id, etype);
    }

    idx_t GetInverseOp(idx_t op_id) {
        idx_t left_type_id = GetLeftTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        idx_t right_type_id = GetRightTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        ExpressionType etype = GetComparisonType(op_id);
        ExpressionType inv_etype; // TODO get inv_etype using NegateComparisionExpression API
        switch (etype) {
            case ExpressionType::COMPARE_EQUAL:
                inv_etype = ExpressionType::COMPARE_NOTEQUAL;
                break;
            case ExpressionType::COMPARE_NOTEQUAL:
                inv_etype = ExpressionType::COMPARE_EQUAL;
                break;
            case ExpressionType::COMPARE_LESSTHAN:
                inv_etype = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
                break;
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                inv_etype = ExpressionType::COMPARE_GREATERTHAN;
                break;
            case ExpressionType::COMPARE_GREATERTHAN:
                inv_etype = ExpressionType::COMPARE_LESSTHANOREQUALTO;
                break;
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                inv_etype = ExpressionType::COMPARE_LESSTHAN;
                break;
            default:
                throw NotImplementedException("InverseOp is not implemented yet");
        }
        return GetComparisonOperator(left_type_id, right_type_id, inv_etype);
    }

    idx_t GetOpFamiliesForScOp(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        LogicalTypeId left_type_id = (LogicalTypeId) (((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256);
        LogicalTypeId right_type_id = (LogicalTypeId) ((op_id - OPERATOR_BASE_ID) % (256));
        idx_t left_type_physical_id = (idx_t) LogicalType(left_type_id).InternalType();
        idx_t right_type_physical_id = (idx_t) LogicalType(right_type_id).InternalType();

        return OPERATOR_FAMILY_BASE_ID
            + (((idx_t) etype) * (256 * 256))
            + (left_type_physical_id * 256)
            + (right_type_physical_id);
    }

    void CoalesceTablesOids(ClientContext &context,
                            vector<uint64_t> &property_key_ids,
                            vector<idx_t> &table_oids,
                            gpmd::MDProviderTBGPP *provider,
                            idx_t &coalesced_table_oid,
                            vector<uint64_t> &properties_location)
    {
        D_ASSERT(table_oids.size() > 0);
        D_ASSERT(provider != nullptr);
        constexpr idx_t REPR_IDX = 0;
        auto &catalog = db.GetCatalog();

        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, table_oids[REPR_IDX]);
        auto part_oid = ps_cat->GetPartitionOID();
        auto part_id = ps_cat->GetPartitionID();

        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA,
                                                      part_oid);
        vector<PropertyKeyID> merged_property_key_ids;

        uint64_t virtual_table_oid = 0;
        if (!(provider->CheckVirtualTableExists(table_oids,
                                                virtual_table_oid))) {
            string property_schema_name =
                part_cat->GetName() + DEFAULT_TEMPORAL_INFIX +
                std::to_string(part_cat->GetNewTemporalID());

            CreatePropertySchemaInfo virtual_ps_info(
                DEFAULT_SCHEMA, property_schema_name.c_str(), part_id,
                part_oid);

            PropertySchemaCatalogEntry *virtual_ps_cat =
                (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
                    context, &virtual_ps_info);

            vector<LogicalType> merged_types;
            vector<PropertyKeyID> merged_property_key_ids;
            idx_t_vector *merged_offset_infos =
                virtual_ps_cat->GetOffsetInfos();
            idx_t_vector *merged_freq_values =
                virtual_ps_cat->GetFrequencyValues();
            uint64_t_vector *merged_ndvs = virtual_ps_cat->GetNDVs();
            uint64_t merged_num_tuples = 0;

            MergeSchemas(
                context, db, table_oids, merged_types, merged_property_key_ids
            );
            MergeHistograms(
                context, db, (uint32_t*)&table_oids[0], table_oids.size(), merged_property_key_ids,
                merged_offset_infos, merged_freq_values, merged_ndvs, merged_num_tuples
            );

            CreateIndexInfo idx_info(DEFAULT_SCHEMA,
                                    property_schema_name + "_id",
                                    IndexType::PHYSICAL_ID, part_oid,
                                    virtual_ps_cat->GetOid(), 0, {-1});

            IndexCatalogEntry *index_cat =
                (IndexCatalogEntry *)catalog.CreateIndex(context,
                                                        &idx_info);
         
            vector<string> key_names;
            gcat->GetPropertyNames(context, merged_property_key_ids,
                                key_names);
            virtual_ps_cat->SetFake();
            virtual_ps_cat->SetSchema(context, key_names, merged_types,
                                    merged_property_key_ids);
            virtual_ps_cat->SetPhysicalIDIndex(index_cat->GetOid());
            virtual_ps_cat->SetNumberOfLastExtentNumTuples(
                merged_num_tuples);

            provider->AddVirtualTable(table_oids, virtual_ps_cat->GetOid());  
            coalesced_table_oid = virtual_ps_cat->GetOid(); 
        }
        else {
            PropertySchemaCatalogEntry* virtual_ps_cat = (
                PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, virtual_table_oid);
            D_ASSERT(virtual_ps_cat != nullptr);
            D_ASSERT(virtual_ps_cat->IsFake());

            auto *key_ids = virtual_ps_cat->GetKeyIDs();
            merged_property_key_ids.reserve(key_ids->size());
            for (auto i = 0; i < key_ids->size(); i++) {
                merged_property_key_ids.push_back(key_ids->at(i));
            }
            coalesced_table_oid = virtual_ps_cat->GetOid();
        }

        // update property_location_in_representative - may be inefficient
        for (auto j = 0; j < property_key_ids.size(); j++) {
            auto it = std::find(merged_property_key_ids.begin(),
                                merged_property_key_ids.end(),
                                property_key_ids[j]);

            if (it != merged_property_key_ids.end()) {
                auto idx =
                    std::distance(merged_property_key_ids.begin(), it);
                properties_location.push_back(idx);
            } else {
                properties_location.push_back(
                    std::numeric_limits<uint64_t>::max()
                );
            }
        }
    }

    void ConvertTableOidsIntoRepresentativeOids(
        ClientContext &context, vector<uint64_t> &property_key_ids,
        vector<idx_t> &table_oids, gpmd::MDProviderTBGPP *provider,
        vector<idx_t> &representative_table_oids,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<vector<uint64_t>> &property_location_in_representative,
        vector<bool> &is_each_group_has_temporary_table)
    {
        Coalescing::do_coalescing(context, db, property_key_ids, table_oids, provider,
                                  representative_table_oids, table_oids_in_group,
                                  property_location_in_representative,
                                  is_each_group_has_temporary_table);
    }

    idx_t AddVirtualTable(ClientContext &context, uint32_t original_vtbl_oid,
                          uint32_t *oid_array, idx_t size)
    {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        // Get original table schema
        PropertySchemaCatalogEntry *original_property_schema_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, original_vtbl_oid);

        // Get first table in oid_array for partition info
        PropertySchemaCatalogEntry *property_schema_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, oid_array[0]);
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA,
                property_schema_cat->GetPartitionOID());

        string part_name = part_cat->GetName();
        string property_schema_name =
            part_name + DEFAULT_TEMPORAL_INFIX +
            std::to_string(part_cat->GetNewTemporalID());

        // Create new Property Schema Catalog Entry
        CreatePropertySchemaInfo propertyschema_info(
            DEFAULT_SCHEMA, property_schema_name.c_str(), part_cat->GetOid(),
            part_cat->GetOid());
        PropertySchemaCatalogEntry *temporal_ps_cat =
            (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
                context, &propertyschema_info);

        // Get original schema info // TODO string key names necessary?
        auto *original_types = original_property_schema_cat->GetTypes();
        auto *original_key_ids = original_property_schema_cat->GetKeyIDs();
        vector<string> key_names;
        gcat->GetPropertyNames(context, *original_key_ids, key_names);

        // Merge histograms from oid_array tables
        idx_t_vector *merged_offset_infos = temporal_ps_cat->GetOffsetInfos();
        idx_t_vector *merged_freq_values = temporal_ps_cat->GetFrequencyValues();
        uint64_t_vector *merged_ndvs = temporal_ps_cat->GetNDVs();
        uint64_t merged_num_tuples = 0;

        vector<PropertyKeyID> prop_key_ids;
        prop_key_ids.reserve(original_key_ids->size());
        for (auto i = 0; i < original_key_ids->size(); i++) {
            prop_key_ids.push_back((*original_key_ids)[i]);
        }
        MergeHistograms(context, db, oid_array, size, prop_key_ids,
                          merged_offset_infos, merged_freq_values,
                          merged_ndvs, merged_num_tuples);

        // create physical id index catalog
        CreateIndexInfo idx_info(DEFAULT_SCHEMA, property_schema_name + "_id",
                                 IndexType::PHYSICAL_ID, part_cat->GetOid(),
                                 temporal_ps_cat->GetOid(), 0, {-1});
        IndexCatalogEntry *index_cat =
            (IndexCatalogEntry *)catalog.CreateIndex(context, &idx_info);

        temporal_ps_cat->SetFake();
        temporal_ps_cat->SetSchema(context, key_names, *original_types,
                                   *original_key_ids);
        temporal_ps_cat->SetPhysicalIDIndex(index_cat->GetOid());
        temporal_ps_cat->SetNumberOfLastExtentNumTuples(merged_num_tuples);

        return temporal_ps_cat->GetOid();
    }

private:
    void MergeSchemas(
        ClientContext &context, DatabaseInstance &db,
        vector<idx_t> table_oids_to_be_merged,
        vector<LogicalType> &merged_types,
        vector<PropertyKeyID> &merged_property_key_ids)
    {
        unordered_set<PropertyKeyID> merged_schema;
        unordered_map<PropertyKeyID, LogicalTypeId> type_info;

        auto &catalog = db.GetCatalog();
        for (auto i = 0; i < table_oids_to_be_merged.size(); i++) {
            PropertySchemaCatalogEntry *ps_cat =
                (PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, table_oids_to_be_merged[i]);

            auto *types = ps_cat->GetTypes();
            auto *key_ids = ps_cat->GetKeyIDs();

            for (auto j = 0; j < key_ids->size(); j++) {
                merged_schema.insert(key_ids->at(j));
                if (type_info.find(key_ids->at(j)) == type_info.end()) {
                    type_info.insert({key_ids->at(j), types->at(j)});
                }
            }
        }

        for (auto it = merged_schema.begin(); it != merged_schema.end(); it++) {
            merged_property_key_ids.push_back(*it);
        }

        for (auto i = 0; i < merged_property_key_ids.size(); i++) {
            idx_t prop_key_id = merged_property_key_ids[i];
            merged_types.push_back(LogicalType(type_info.at(prop_key_id)));
        }
    }

    void MergeHistograms(
        ClientContext &context, DatabaseInstance &db,
        uint32_t *table_oids_to_be_merged, idx_t size, vector<PropertyKeyID> &original_key_ids,
        idx_t_vector *merged_offset_infos, uint64_t_vector *merged_freq_values,
        uint64_t_vector *merged_ndvs, uint64_t &merged_num_tuples)
    {
        auto &catalog = db.GetCatalog();
        unordered_map<PropertyKeyID, vector<idx_t>>
            intermediate_merged_freq_values;
        unordered_map<PropertyKeyID, idx_t> accumulated_ndvs;
        idx_t accumulated_ndvs_for_physical_id_col = 0;

        bool has_histogram = true;

        // merge histograms
        for (auto i = 0; i < size; i++) {
            idx_t table_oid = table_oids_to_be_merged[i];

            PropertySchemaCatalogEntry *ps_cat =
                (PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, table_oid);

            auto *key_ids = ps_cat->GetKeyIDs();
            auto *ndvs = ps_cat->GetNDVs();
            accumulated_ndvs_for_physical_id_col +=
                ps_cat->GetNumberOfRowsApproximately();
            merged_num_tuples += ps_cat->GetNumberOfRowsApproximately();

            has_histogram = has_histogram && ndvs->size() > 0;

            /**
             * TODO: adding NDVs is seems wrong.
             * We have to do correctly (e.g., setting max NDV)
            */
           
            if (has_histogram) {
                for (auto j = 0; j < key_ids->size(); j++) {
                    if (accumulated_ndvs.find(key_ids->at(j)) ==
                        accumulated_ndvs.end()) {
                        accumulated_ndvs.insert({key_ids->at(j), ndvs->at(j)});
                    }
                    else {
                        accumulated_ndvs[key_ids->at(j)] += ndvs->at(j);
                    }
                }
            }

            if (!has_histogram)
                continue;

            auto *offset_infos = ps_cat->GetOffsetInfos();
            auto *freq_values = ps_cat->GetFrequencyValues();

            for (auto j = 0; j < key_ids->size(); j++) {
                auto it = intermediate_merged_freq_values.find(key_ids->at(j));
                if (it == intermediate_merged_freq_values.end()) {
                    vector<idx_t> tmp_vec;
                    auto begin_offset = j == 0 ? 0 : offset_infos->at(j - 1);
                    auto end_offset = offset_infos->at(j);
                    auto freq_begin_offset = j == 0 ? 0 : begin_offset - (j);
                    auto freq_end_offset = end_offset - (j + 1);
                    for (auto k = freq_begin_offset; k < freq_end_offset; k++) {
                        tmp_vec.push_back(freq_values->at(k));
                    }
                    intermediate_merged_freq_values.insert(
                        {key_ids->at(j), std::move(tmp_vec)});
                }
                else {
                    auto &freq_vec = it->second;
                    auto begin_offset = j == 0 ? 0 : offset_infos->at(j - 1);
                    auto end_offset = offset_infos->at(j);
                    auto freq_begin_offset = j == 0 ? 0 : begin_offset - (j);
                    auto freq_end_offset = end_offset - (j + 1);
                    for (auto k = freq_begin_offset; k < freq_end_offset; k++) {
                        freq_vec[k - freq_begin_offset] += freq_values->at(k);
                    }
                }
            }
        }

        if (!has_histogram)
            return;

        merged_ndvs->push_back(accumulated_ndvs_for_physical_id_col);
        size_t accumulated_offset = 0;
        for (auto i = 0; i < original_key_ids.size(); i++) {
            idx_t prop_key_id = original_key_ids[i];
            auto it = intermediate_merged_freq_values.find(prop_key_id);
            if (it == intermediate_merged_freq_values.end()) {
                // Property not present in merged tables - use zero frequencies
                accumulated_offset += 1;
                merged_offset_infos->push_back(accumulated_offset);
                merged_ndvs->push_back(0);
            } else {
                auto &freq_vec = it->second;
                accumulated_offset += (freq_vec.size() + 1);
                merged_offset_infos->push_back(accumulated_offset);
                for (auto j = 0; j < freq_vec.size(); j++) {
                    merged_freq_values->push_back(freq_vec[j]);
                }
                auto ndv_it = accumulated_ndvs.find(prop_key_id);
                merged_ndvs->push_back(ndv_it != accumulated_ndvs.end() ? ndv_it->second : 0);
            }
        }
    }

    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb