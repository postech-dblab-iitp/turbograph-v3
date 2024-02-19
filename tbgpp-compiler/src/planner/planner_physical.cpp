#include "planner.hpp"

#include <wchar.h>
#include <algorithm>
#include <limits>
#include <numeric>
#include <set>
#include <string>

// locally used duckdb operators
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "execution/physical_operator/physical_blockwise_nl_join.hpp"
#include "execution/physical_operator/physical_cross_product.hpp"
#include "execution/physical_operator/physical_filter.hpp"
#include "execution/physical_operator/physical_hash_aggregate.hpp"
#include "execution/physical_operator/physical_hash_join.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_piecewise_merge_join.hpp"
#include "execution/physical_operator/physical_produce_results.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/physical_operator/physical_sort.hpp"
#include "execution/physical_operator/physical_top.hpp"
#include "execution/physical_operator/physical_top_n_sort.hpp"
#include "execution/physical_operator/physical_varlen_adjidxjoin.hpp"

#include "planner/expression/bound_between_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

namespace s62 {

void Planner::pGenPhysicalPlan(CExpression *orca_plan_root)
{
    pInitializeSchemaFlowGraph();
    vector<duckdb::CypherPhysicalOperator *> final_pipeline_ops =
        *pTraverseTransformPhysicalPlan(orca_plan_root);

    // Append PhysicalProduceResults
    duckdb::Schema final_output_schema =
        final_pipeline_ops[final_pipeline_ops.size() - 1]->schema;
    vector<duckdb::Schema> prev_local_schemas;
    duckdb::CypherPhysicalOperator *op;

    // calculate mapping for produceresults
    vector<uint8_t> projection_mapping;
    vector<vector<uint8_t>> projection_mappings;
    // TODO strange code..
    if (!generate_sfg) {
        for (uint8_t log_idx = 0; log_idx < logical_plan_output_colrefs.size();
             log_idx++) {
            for (uint8_t phy_idx = 0;
                 phy_idx < physical_plan_output_colrefs.size(); phy_idx++) {
                if (logical_plan_output_colrefs[log_idx]->Id() ==
                    physical_plan_output_colrefs[phy_idx]->Id()) {
                    projection_mapping.push_back(phy_idx);
                }
            }
        }
        D_ASSERT(projection_mapping.size() ==
                 logical_plan_output_colrefs.size());
        op = new duckdb::PhysicalProduceResults(final_output_schema,
                                                projection_mapping);
    }
    else {
        prev_local_schemas = pipeline_schemas.back();
        for (auto i = 0; i < prev_local_schemas.size(); i++) {
            auto local_schema =
                std::move(prev_local_schemas[i].getStoredTypes());
            projection_mappings.push_back(std::vector<uint8_t>());
            for (uint8_t log_idx = 0;
                 log_idx < logical_plan_output_colrefs.size(); log_idx++) {
                for (uint8_t phy_idx = 0;
                     phy_idx < physical_plan_output_colrefs.size(); phy_idx++) {
                    if (logical_plan_output_colrefs[log_idx]->Id() ==
                        physical_plan_output_colrefs[phy_idx]->Id()) {
                        // if (local_schema[phy_idx] ==
                        //     duckdb::LogicalType::SQLNULL) {
                        //     projection_mappings[i].push_back(
                        //         std::numeric_limits<uint8_t>::max());
                        // }
                        // else {
                        projection_mappings[i].push_back(phy_idx);
                        // }
                    }
                }
            }
        }
        op = new duckdb::PhysicalProduceResults(final_output_schema,
                                                projection_mappings);
    }

    final_pipeline_ops.push_back(op);
    D_ASSERT(final_pipeline_ops.size() > 0);

    if (generate_sfg) {
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(final_output_schema);

        pGenerateSchemaFlowGraph(final_pipeline_ops);
    }

    auto final_pipeline = new duckdb::CypherPipeline(final_pipeline_ops);

    pipelines.push_back(final_pipeline);

    // validate plan
    D_ASSERT(pValidatePipelines());
    return;
}

bool Planner::pValidatePipelines()
{
    bool ok = true;
    ok = ok && pipelines.size() > 0;
    for (auto &pipeline : pipelines) {
        ok = ok && (pipeline->pipelineLength >= 2);
    }
    ok = ok && (pipelines[pipelines.size() - 1]->GetSink()->type ==
                duckdb::PhysicalOperatorType::PRODUCE_RESULTS);
    return ok;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTraverseTransformPhysicalPlan(CExpression *plan_expr)
{

    vector<duckdb::CypherPhysicalOperator *> *result = nullptr;

    /* Matching order
		- UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan
			- UnionAll-ComputeScalar-Filter-TableScan|IndexScan => NodeScan|NodeIndexScan or Filter-NodeScan|NodeIndexScan
		- Projection => Projection
		- TableScan => EdgeScan
		// TODO fillme
	*/

    // based on op pass call to the corresponding func
    D_ASSERT(plan_expr != nullptr);
    D_ASSERT(plan_expr->Pop()->FPhysical());

    switch (plan_expr->Pop()->Eopid()) {
        case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
            // Currently not working
            if (pIsUnionAllOpAccessExpression(plan_expr)) {
                result = pTransformEopUnionAllForNodeOrEdgeScan(plan_expr);
            }
            else {
                D_ASSERT(false);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalIndexPathJoin: {
            result = pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(
                plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerNLJoin: {
            // cartesian product only
            if (pIsCartesianProduct(plan_expr)) {
                result = pTransformEopPhysicalInnerNLJoinToCartesianProduct(
                    plan_expr);
                break;
            }
            // otherwise handle NLJ
            result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftAntiSemiNLJoin: {
            result =
                pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr, true);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin: {
            result = pTransformEopPhysicalHashJoinToHashJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerMergeJoin: {
            result = pTransformEopPhysicalMergeJoinToMergeJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalLeftOuterNLJoin:       // LEFT
        case COperator::EOperatorId::EopPhysicalLeftSemiNLJoin:        // SEMI
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiNLJoin: {  // ANTI
            result = pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalInnerIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin: {
            bool is_left_outer =
                plan_expr->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin;
            CExpression *pexprInner = (*plan_expr)[1];
            if (pIsUnionAllOpAccessExpression(pexprInner)) {
                if (pIsIndexJoinOnPhysicalID(plan_expr)) {
                    result =
                        pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInner(
                            plan_expr);
                }
                else {
                    throw NotImplementedException("Schemaless adjidxjoin case");
                    // result = pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(plan_expr, is_left_outer);
                }
            }
            else {
                if (pIsIndexJoinOnPhysicalID(plan_expr)) {
                    result = pTransformEopPhysicalInnerIndexNLJoinToIdSeek(
                        plan_expr);
                }
                else {
                    result = pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(
                        plan_expr, is_left_outer);
                }
            }
            break;
        }
        // Try fitler projection
        case COperator::EOperatorId::EopPhysicalFilter: {
            if (plan_expr->operator[](0)->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalTableScan) {
                // Filter + Scan
                auto scan_p1 = vector<COperator::EOperatorId>(
                    {COperator::EOperatorId::EopPhysicalFilter,
                     COperator::EOperatorId::EopPhysicalTableScan});
                if (pMatchExprPattern(plan_expr, scan_p1, 0, true) &&
                    pIsFilterPushdownAbleIntoScan(plan_expr)) {
                    result = pTransformEopTableScan(plan_expr);
                }
                else {
                    result = pTransformEopPhysicalFilter(plan_expr);
                }
            }
            else {
                result = pTransformEopPhysicalFilter(plan_expr);
            }
            break;
        }
        case COperator::EOperatorId::EopPhysicalTableScan: {
            result = pTransformEopTableScan(plan_expr);
            break;
        }
        // Unary operators
        case COperator::EOperatorId::EopPhysicalComputeScalarColumnar: {
            result = pTransformEopProjectionColumnar(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalLimit: {
            // TODO we need to optimize Limit + Sort
            result = pTransformEopLimit(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalSort: {
            result = pTransformEopSort(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalHashAgg:
        case COperator::EOperatorId::EopPhysicalScalarAgg: {
            // PhysicalStreamAgg is not supported
            result = pTransformEopAgg(plan_expr);
            break;
        }
        default:
            D_ASSERT(false);
            break;
    }
    D_ASSERT(result != nullptr);

    /* Update latest plan output columns */
    auto *mp = this->memory_pool;
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    physical_plan_output_colrefs.clear();  // TODO strange.. multiple times?
    for (ULONG idx = 0; idx < output_cols->Size(); idx++) {
        physical_plan_output_colrefs.push_back(output_cols->operator[](idx));
    }

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopTableScan(
    CExpression *plan_expr)
{
    /*
		handles
		 - F + S
		 - S
	*/
    // for orca's pushdown mechanism, refer to CTranslatorExprToDXL::PdxlnFromFilter(CExpression *pexprFilter,
    auto *mp = this->memory_pool;

    // leaf node
    auto result = new vector<duckdb::CypherPhysicalOperator *>();

    CExpression *scan_expr = NULL;
    CExpression *filter_expr = NULL;
    CPhysicalTableScan *scan_op = NULL;
    CPhysicalFilter *filter_op = NULL;
    CExpression *filter_pred_expr = NULL;

    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        filter_expr = plan_expr;
        filter_op = (CPhysicalFilter *)filter_expr->Pop();
        filter_pred_expr = filter_expr->operator[](1);
        scan_expr = filter_expr->operator[](0);
        scan_op = (CPhysicalTableScan *)scan_expr->Pop();
        // TODO current assume all predicates are pushdown-able
        D_ASSERT(filter_pred_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarCmp);
        D_ASSERT(filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarIdent);
        D_ASSERT(filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarConst);
    }
    else {
        scan_expr = plan_expr;
        scan_op = (CPhysicalTableScan *)scan_expr->Pop();
    }
    bool do_filter_pushdown = filter_op != NULL;

#ifdef DYNAMIC_SCHEMA_INSTANTIATION
    CTableDescriptor *tab_desc = scan_op->Ptabdesc();
    if (tab_desc->IsInstanceDescriptor()) {
        // get partition catalog
        CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(tab_desc->MDId());
        OID instance_obj_id = table_mdid->Oid();

        duckdb::Catalog &cat_instance = context->db->GetCatalog();
        duckdb::GraphCatalogEntry *graph_cat =
            (duckdb::GraphCatalogEntry *)cat_instance.GetEntry(
                *context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
                DEFAULT_GRAPH);
        duckdb::PropertySchemaCatalogEntry *instance_ps_cat =
            (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                *context, DEFAULT_SCHEMA, instance_obj_id);
        OID partition_oid = instance_ps_cat->GetPartitionOID();

        duckdb::PartitionCatalogEntry *partition_cat =
            (duckdb::PartitionCatalogEntry *)cat_instance.GetEntry(
                *context, DEFAULT_SCHEMA, partition_oid);

        CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(
            mp);  // columns required for the output of NodeScan
        CColRefArray *scan_cols = scan_expr->Prpp()->PcrsRequired()->Pdrgpcr(
            mp);  // columns required to be scanned from storage
        // D_ASSERT(scan_cols->ContainsAll(output_cols)); 				// output_cols is the subset of scan_cols

        // generate plan for each schema
        vector<duckdb::idx_t> oids;
        vector<vector<uint64_t>> projection_mappings;
        vector<vector<uint64_t>> scan_projection_mappings;
        partition_cat->GetPropertySchemaIDs(oids);

        duckdb::Schema global_schema;
        vector<duckdb::Schema> local_schemas;
        vector<duckdb::LogicalType> global_types;
        vector<duckdb::idx_t> scan_cols_id;

        local_schemas.resize(oids.size());

        for (auto i = 0; i < scan_cols->Size(); i++) {
            ULONG col_id = scan_cols->operator[](i)->ColId();
            scan_cols_id.push_back(col_id);

            // get type from col_id -> type idx
            duckdb::LogicalTypeId type_id =
                graph_cat->GetTypeIdFromPropertyKeyID(col_id);
            D_ASSERT(type_id != duckdb::LogicalTypeId::DECIMAL);  // TODO
            global_types.push_back(duckdb::LogicalType(type_id));
        }

        for (auto i = 0; i < oids.size(); i++) {
            projection_mappings.push_back(vector<uint64_t>());
            scan_projection_mappings.push_back(vector<uint64_t>());

            vector<duckdb::LogicalType> local_types;

            // get schema info of i-th schema oids[i]
            duckdb::PropertySchemaCatalogEntry *ps_cat =
                (duckdb::PropertySchemaCatalogEntry *)cat_instance.GetEntry(
                    *context, DEFAULT_SCHEMA, oids[i]);
            duckdb::PropertyKeyID_vector *key_ids = ps_cat->GetKeyIDs();

            pGenerateMappingInfo(scan_cols_id, key_ids, global_types,
                                 local_types, projection_mappings.back(),
                                 scan_projection_mappings.back());
            local_schemas[i].setStoredTypes(local_types);
        }

        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({local_schemas.size()});
        pipeline_schemas.push_back(local_schemas);
        pipeline_union_schema.push_back(global_schema);

        duckdb::CypherPhysicalOperator *op = nullptr;
        if (!do_filter_pushdown) {
            op = new duckdb::PhysicalNodeScan(
                local_schemas, global_schema, move(oids),
                move(projection_mappings), move(scan_projection_mappings));
        }
        else {
            // op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, literal_val);
        }

        D_ASSERT(op != nullptr);
        result->push_back(op);

        return result;
    }
#endif

    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(scan_op->Ptabdesc()->MDId());
    OID table_obj_id = table_mdid->Oid();

    CColRefSet *output_cols =
        plan_expr->Prpp()
            ->PcrsRequired();  // columns required for the output of NodeScan
    CColRefSet *scan_cols =
        scan_expr->Prpp()
            ->PcrsRequired();  // columns required to be scanned from storage
    D_ASSERT(scan_cols->ContainsAll(
        output_cols));  // output_cols is the subset of scan_cols

    // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<uint64_t> scan_ident_mapping;
    pGenerateScanMapping(table_obj_id, scan_cols->Pdrgpcr(mp),
                         scan_ident_mapping);
    vector<duckdb::LogicalType> scan_types;
    pGenerateTypes(scan_cols->Pdrgpcr(mp), scan_types);
    D_ASSERT(scan_ident_mapping.size() == scan_cols->Size());
    scan_projection_mapping.push_back(scan_ident_mapping);

    // oids / projection_mapping
    vector<vector<uint64_t>> output_projection_mapping;
    vector<uint64_t> output_to_original_table_mapping;
    vector<uint64_t> output_to_scanned_table_mapping;
    pGenerateScanMapping(table_obj_id, output_cols->Pdrgpcr(mp),
                         output_to_original_table_mapping);
    D_ASSERT(output_to_original_table_mapping.size() == output_cols->Size());

    for (auto i = 0; i < output_to_original_table_mapping.size(); i++) {
        auto col_id = output_to_original_table_mapping[i];
        auto it = std::find(scan_ident_mapping.begin(),
                            scan_ident_mapping.end(), col_id);
        D_ASSERT(it != scan_ident_mapping.end());
        output_to_scanned_table_mapping.push_back(
            std::distance(scan_ident_mapping.begin(), it));
    }

    output_projection_mapping.push_back(output_to_scanned_table_mapping);
    vector<duckdb::LogicalType> types;
    vector<string> out_col_names;
    pGenerateTypes(output_cols->Pdrgpcr(mp), types);
    pGenerateColumnNames(output_cols->Pdrgpcr(mp), out_col_names);

    gpos::ULONG pred_attr_pos;
    duckdb::Value literal_val;
    if (do_filter_pushdown) {
        pGetFilterAttrPosAndValue(filter_pred_expr, pred_attr_pos, literal_val);
    }

    // oids
    vector<uint64_t> oids;
    oids.push_back(table_obj_id);
    D_ASSERT(oids.size() == 1);

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(out_col_names);
    duckdb::CypherPhysicalOperator *op = nullptr;

    if (!do_filter_pushdown) {
        op = new duckdb::PhysicalNodeScan(tmp_schema, oids,
                                          output_projection_mapping,
                                          scan_projection_mapping);
    }
    else {
		if (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptEq) {
			op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, literal_val);
		}
		else if (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptL) {
			op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, 
			duckdb::Value::MinimumValue(literal_val.type()), literal_val, true, false);
		}
		else if (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptLEq) {
			op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, 
			duckdb::Value::MinimumValue(literal_val.type()), literal_val, true, true);
		}
		else if (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptG) {
			op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, 
			literal_val, duckdb::Value::MaximumValue(literal_val.type()), false, true);
		}
		else if (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptGEq) {
			op = new duckdb::PhysicalNodeScan(tmp_schema, oids, output_projection_mapping, scan_types, scan_projection_mapping, pred_attr_pos, 
			literal_val, duckdb::Value::MaximumValue(literal_val.type()), true, true);
		}
		else {
			D_ASSERT(false);
		}
    }

    if (generate_sfg) {
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    D_ASSERT(op != nullptr);
    result->push_back(op);

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopUnionAllForNodeOrEdgeScan(CExpression *plan_expr)
{
    auto *mp = this->memory_pool;
    /* UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan*/
    /* UnionAll-ComputeScalar-Filter-TableScan|IndexScan => Filter-NodeScan|NodeIndexScan*/
    D_ASSERT(plan_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalSerialUnionAll);

    // filter
    bool is_filter_exist = false;
    bool is_filter_only_column_exist = false;
    vector<ULONG> unionall_output_original_col_ids;
    vector<duckdb::idx_t> bound_ref_idxs;

    // leaf node
    auto result = new vector<duckdb::CypherPhysicalOperator *>();
    vector<uint64_t> oids;
    vector<vector<uint64_t>> projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;

    CExpressionArray *projections = plan_expr->PdrgPexpr();
    const ULONG projections_size = projections->Size();

    duckdb::Schema global_schema;
    vector<duckdb::Schema> local_schemas;
    vector<duckdb::LogicalType> global_types;
    local_schemas.resize(projections_size);

    for (int i = 0; i < projections_size; i++) {
        CExpression *projection = projections->operator[](i);
        CExpression *scan_expr = NULL;
        CExpression *proj_list_expr = projection->PdrgPexpr()->operator[](1);
        CColRefArray *proj_output_cols =
            projection->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        if (projection->PdrgPexpr()->operator[](0)->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalFilter) {
            scan_expr = projection->PdrgPexpr()->operator[](0)->operator[](0);
            is_filter_exist = true;
        }
        else {
            scan_expr = projection->PdrgPexpr()->operator[](0);
        }

        CPhysicalScan *scan_op = (CPhysicalScan *)scan_expr->Pop();
        const ULONG proj_list_expr_size = proj_list_expr->PdrgPexpr()->Size();

        // TODO for index scan?
        CMDIdGPDB *table_mdid =
            CMDIdGPDB::CastMdid(scan_op->Ptabdesc()->MDId());
        OID table_obj_id = table_mdid->Oid();

        // collect object ids
        oids.push_back(table_obj_id);
        vector<duckdb::LogicalType> local_types;

        // initialize global schema
        if (i == 0) {
            global_types.resize(proj_list_expr_size,
                                duckdb::LogicalType::SQLNULL);
        }

        // for each object id, generate projection mapping. (if null projection required, ulong::max)
        projection_mapping.push_back(vector<uint64_t>());
        scan_projection_mapping.push_back(vector<uint64_t>());
        for (int j = 0; j < proj_list_expr_size; j++) {
            CExpression *proj_elem = proj_list_expr->PdrgPexpr()->operator[](j);
            CScalarProjectElement *proj_elem_op =
                (CScalarProjectElement *)proj_elem->Pop();
            auto scalarident_pattern = vector<COperator::EOperatorId>(
                {COperator::EOperatorId::EopScalarProjectElement,
                 COperator::EOperatorId::EopScalarIdent});

            CExpression *proj_item;
            if (pMatchExprPattern(proj_elem, scalarident_pattern)) {
                /* CScalarProjectList - CScalarIdent */
                CScalarIdent *ident_op = (CScalarIdent *)proj_elem->PdrgPexpr()
                                             ->
                                             operator[](0)
                                             ->Pop();
                auto col_idx =
                    pGetColIdxFromTable(table_obj_id, ident_op->Pcr());
                projection_mapping[i].push_back(j);
                scan_projection_mapping[i].push_back(col_idx);
                // add local types
                CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                    ident_op->Pcr()->RetrieveType()->MDId());
                OID type_oid = type_mdid->Oid();
                auto duckdb_type = pConvertTypeOidToLogicalType(type_oid);
                local_types.push_back(duckdb_type);
                // update global types
                if (global_types[j] == duckdb::LogicalType::SQLNULL) {
                    global_types[j] = duckdb_type;
                }
                /* for filter */
                if (is_filter_exist && i == 0) {
                    unionall_output_original_col_ids.push_back(
                        ident_op->Pcr()->Id());
                    // this column is not filter-only-used column
                    if (proj_output_cols->IndexOf(proj_elem_op->Pcr()) !=
                        gpos::ulong_max) {
                        bound_ref_idxs.push_back(j);
                    }
                    else {
                        is_filter_only_column_exist = true;
                    }
                }
            }
            else {
                /* CScalarProjectList - CScalarConst (null) */
                projection_mapping[i].push_back(j);
                scan_projection_mapping[i].push_back(
                    std::numeric_limits<uint64_t>::max());
                local_types.push_back(duckdb::LogicalTypeId::SQLNULL);

                /* for filter */
                if (is_filter_exist && i == 0) {
                    unionall_output_original_col_ids.push_back(
                        std::numeric_limits<ULONG>::max());
                    bound_ref_idxs.push_back(j);
                }
            }
        }
        local_schemas[i].setStoredTypes(local_types);
        // TODO else check if type conforms
    }

    global_schema.setStoredTypes(global_types);

    generate_sfg = true;

    // Update schema flow graph for scan
    pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
    num_schemas_of_childs.push_back({local_schemas.size()});
    pipeline_schemas.push_back(local_schemas);
    pipeline_union_schema.push_back(global_schema);

    if (is_filter_exist) {
        /**
         * Pattern
         * SerialUnionAll
         * |---ComputeScalar
         * |   |---Filter
         * |   |   |---TableScan
        */
        auto repr_slc_expr =
            plan_expr->PdrgPexpr()->operator[](0)->operator[](0);
        CColRefSet *output_cols = repr_slc_expr->Prpp()->PcrsRequired();
		CExpression *repr_filter_pred_expr = repr_slc_expr->operator[](1);
        CPhysicalFilter *filter_op = (CPhysicalFilter *)repr_slc_expr->Pop();
        /**
         * Strong assumption: filter predicates are only on non-schemaless columns
         * Binder will not include tables without the columns used by filter predicates.
         * 
         * However, if we need to process NULL-handling predicate (e.g., IS NULL, IS NOT NULL),
         * we may need to handle schemaless columns.
        */
        if (pIsFilterPushdownAbleIntoScan(repr_slc_expr)) {
            vector<int64_t> pred_attr_poss;
            vector<duckdb::Value> literal_vals;

            for (int i = 0; i < projections_size; i++) {
                gpos::ULONG pred_attr_pos;
                duckdb::Value literal_val;
                CExpression *projection = projections->operator[](i);
                CExpression *filter = projection->PdrgPexpr()->operator[](0);
                CExpression *filter_pred_expr = filter->operator[](1);
                pGetFilterAttrPosAndValue(filter_pred_expr, pred_attr_pos,
                                          literal_val);
                pred_attr_poss.push_back(pred_attr_pos);
                literal_vals.push_back(move(literal_val));
            }
            
			/* add expression type for pushdown */
			/* Note: this will not work on VARCHAR Type, only in numerics. If bug occurs, check here. */
			duckdb::CypherPhysicalOperator *op = nullptr;
			vector<duckdb::RangeFilterValue> range_filter_values;
			auto cmp_type = ((CScalarCmp*)(repr_filter_pred_expr->Pop()))->ParseCmpType();
			auto num_vals = literal_vals.size();
			switch (cmp_type) {
				case IMDType::ECmpType::EcmptEq:
					op = new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids, projection_mapping, scan_projection_mapping, 
													pred_attr_poss, literal_vals);
					break;
				case IMDType::ECmpType::EcmptL:
					for (int i = 0; i < num_vals; i++) range_filter_values.push_back({duckdb::Value::MinimumValue(literal_vals[i].type()), literal_vals[i], true, false});
					op = new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids, projection_mapping, scan_projection_mapping, 
													pred_attr_poss, range_filter_values);
					break;
				case IMDType::ECmpType::EcmptLEq:
					for (int i = 0; i < num_vals; i++) range_filter_values.push_back({duckdb::Value::MinimumValue(literal_vals[i].type()), literal_vals[i], true, true});
					op = new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids, projection_mapping, scan_projection_mapping, 
													pred_attr_poss, range_filter_values);
					break;
				case IMDType::ECmpType::EcmptG:
					for (int i = 0; i < num_vals; i++) range_filter_values.push_back({literal_vals[i], duckdb::Value::MaximumValue(literal_vals[i].type()), false, true});
					op = new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids, projection_mapping, scan_projection_mapping, 
													pred_attr_poss, range_filter_values);
					break;
				case IMDType::ECmpType::EcmptGEq:
					for (int i = 0; i < num_vals; i++) range_filter_values.push_back({literal_vals[i], duckdb::Value::MaximumValue(literal_vals[i].type()), true, true});
					op = new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids, projection_mapping, scan_projection_mapping, 
													pred_attr_poss, range_filter_values);
					break;
				default:
					D_ASSERT(false);
					break;
			}
            result->push_back(op);
        }
        else {
            // Generate filter exprs
            vector<unique_ptr<duckdb::Expression>> filter_exprs;
            CExpression *filter_pred_expr = repr_slc_expr->operator[](1);
            CColRefArray *repr_scan_cols = repr_slc_expr->PdrgPexpr()
                                               ->
                                               operator[](0)
                                               ->Prpp()
                                               ->PcrsRequired()
                                               ->Pdrgpcr(mp);
            auto repr_filter_expr =
                pTransformScalarExpr(filter_pred_expr, repr_scan_cols, nullptr);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                repr_filter_expr, repr_scan_cols,
                unionall_output_original_col_ids);
            auto repr_scan_projection_mapping = scan_projection_mapping[0];
            auto repr_projection_mapping = projection_mapping[0];
            filter_exprs.push_back(std::move(repr_filter_expr));

            // Create filter + nodeScan operator
            duckdb::CypherPhysicalOperator *filter_cypher_op =
                new duckdb::PhysicalFilter(global_schema, move(filter_exprs));
            duckdb::CypherPhysicalOperator *scan_cypher_op =
                new duckdb::PhysicalNodeScan(local_schemas, global_schema, oids,
                                             projection_mapping,
                                             scan_projection_mapping);
            result->push_back(scan_cypher_op);
            result->push_back(filter_cypher_op);

            // Update schema flow graph for filter
            pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
            num_schemas_of_childs.push_back({local_schemas.size()});
            pipeline_schemas.push_back(local_schemas);
            pipeline_union_schema.push_back(global_schema);
        }

        /* If we don't need filter-only-used column anymore, create projection operator */
        /**
         * CPhyiscalFilter's required output columns have filter-only-used columns.
         * However, CPhysicalComputeScalarC's required output columns does not have filter-only-used columns.
         * This is because CPhyiscalFilter thinks that CPhysicalComputeScalarC uses those columns.
         * 
         * Therefore, to create projection operator to remove filter-only-used columns, 
         * we need to compare CPhyiscalFilter's required output columns and CPhysicalComputeScalarC's required output columns.
         * 
         * Fortunately, we made NodeScan to output columns in order of CPhysicalComputeScalarC's projected columns.
         * And, the output columns of CPhyiscalFilter is as same as the NodeScan's output columns.
         * Therefore, to solve this problem, we just check each of projected column is in the required output columns of
         * CPhysicalComputeScalarC.
         * 
         * See bound_ref_idxs for code level implementation
         */
        CColRefArray *unionall_output_cols =
            plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        if (is_filter_only_column_exist) {
            // Obtain union schema
            duckdb::Schema proj_op_output_union_schema;
            vector<duckdb::LogicalType> proj_op_output_types;
            pGetColumnsDuckDBType(unionall_output_cols,
                                    proj_op_output_types);
            proj_op_output_union_schema.setStoredTypes(
                proj_op_output_types);

            // Obtain local schemas
            vector<duckdb::Schema> proj_op_output_local_schemas;
            pRemoveColumnsFromSchemas(local_schemas, bound_ref_idxs,
                                        proj_op_output_local_schemas);

            // Create projection exprs
            vector<unique_ptr<duckdb::Expression>> proj_exprs;
            pGetProjectionExprs(proj_op_output_types, bound_ref_idxs,
                                proj_exprs);

            // Create projection operator
            D_ASSERT(proj_exprs.size() != 0);
            D_ASSERT(proj_exprs.size() == unionall_output_cols->Size());
            duckdb::CypherPhysicalOperator *proj_op =
                new duckdb::PhysicalProjection(proj_op_output_union_schema,
                                                std::move(proj_exprs));
            result->push_back(proj_op);

            // Update schema flow graph for projection
            pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
            num_schemas_of_childs.push_back({local_schemas.size()});
            pipeline_schemas.push_back(proj_op_output_local_schemas);
            pipeline_union_schema.push_back(proj_op_output_union_schema);
        }
    }
    else {
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalNodeScan(
            local_schemas, global_schema, oids, projection_mapping,
            scan_projection_mapping);
        result->push_back(op);
    }

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(
    CExpression *plan_expr, bool is_left_outer)
{
    CMemoryPool *mp = this->memory_pool;

    // actuall since this adjidxjoin there is second child, we integrate as single child
    // first child = outer (blablabla...)
    // second child = inner ( proj - idxscan (x = y))

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;

    CPhysicalInnerIndexNLJoin *proj_op =
        (CPhysicalInnerIndexNLJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    // CColRefArray* inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    unordered_map<ULONG, uint64_t> id_map;
    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;
    vector<uint32_t> outer_col_map_seek;
    vector<uint32_t> inner_col_map_seek;

    vector<uint64_t> oids;
    vector<vector<uint64_t>> projection_mapping;
    vector<uint64_t> first_table_mapping;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    // 230303 srcidxcol = 0 (_id)
    uint64_t sid_col_idx = 0;
    uint64_t seek_sid_col_idx = 0;

    // TODO load only adjidx id => hardcode load sid only (we use backward idx)

    uint64_t adjidx_obj_id;  // 230303
    std::vector<uint32_t> sccmp_colids;
    CExpression *inner_root = pexprInner;

    bool do_filter_pushdown = false;
    CPhysicalFilter *filter_op = NULL;
    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    CExpression *idxscan_expr = NULL;
    CColRefArray *idxscan_output;

    /**
     * The pattern like
     * --CPhysicalComputeScalarC
     * ----CPhysicalIndexScan
     * is not handled properly.
     * 
     * I cannot fully understand this jhko's code (too complex for me).
     * I specially handle this case, but I think this is not the clean way.
     * I think I need to understand jhko's code and fix the bug with refactoring.
     * 
     * See code related to is_proj_exist and proj_expr.
    */
    bool is_proj_exist = false;
    CExpression *proj_expr = NULL;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexScan ||
            inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            // IdxScan
            idxscan_expr = inner_root;
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            adjidx_obj_id = (uint64_t)oid;

            // Get JoinColumnID
            for (uint32_t i = 0; i < inner_root->operator[](0)->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent
                         *)(inner_root->operator[](0)->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }

            // TODO there may be additional projection - we CURRENTLY do not consider projection
            idxscan_output = inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);

            for (ULONG i = 0; i < idxscan_output->Size(); i++) {
                CColRef *colref = idxscan_output->operator[](i);
                OID table_obj_id = CMDIdGPDB::CastMdid(
                                       ((CColRefTable *)colref)->GetMdidTable())
                                       ->Oid();
                if (i == 0) {
                    oids.push_back((uint64_t)table_obj_id);
                }
                auto table_col_idx = pGetColIdxFromTable(table_obj_id, colref);
                // in edge table case, we only consider attr_num >= 3
                if (table_col_idx >= 3)
                    first_table_mapping.push_back(table_col_idx);
            }
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalFilter) {
            filter_expr = inner_root;
            filter_op = (CPhysicalFilter *)filter_expr->Pop();
            filter_pred_expr = filter_expr->operator[](1);
            // TODO current assume all predicates are pushdown-able
            // D_ASSERT(filter_pred_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
            // D_ASSERT(filter_pred_expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent);
            // D_ASSERT(filter_pred_expr->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent);
            // // D_ASSERT(filter_pred_expr->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarConst);

            do_filter_pushdown = true;
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalComputeScalarColumnar) {
            // ComputeScalar
            is_proj_exist = true;
            proj_expr = inner_root;
        }

        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    projection_mapping.push_back(first_table_mapping);
    D_ASSERT(inner_root != pexprInner);

    D_ASSERT(oids.size() == 1);
    D_ASSERT(projection_mapping.size() == 1);

    bool sid_col_idx_found = false;
    // Construct mapping info
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        CColRef *col = outer_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        // find key column
        auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
        if (it != sccmp_colids.end()) {
            D_ASSERT(!sid_col_idx_found);
            sid_col_idx = col_idx;
            sid_col_idx_found = true;
        }
        // construct outer col map
        auto it_ = id_map.find(col_id);
        if (it_ == id_map.end()) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            auto id_idx = id_map.at(col_id);
            outer_col_map.push_back(id_idx);
        }
    }
    D_ASSERT(sid_col_idx_found);

    bool load_edge_property = false, load_eid = false;
    // construct inner col map
    CColRefArray *inner_cols =
        is_proj_exist ?
            proj_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp) :
            idxscan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        CColRef *col = inner_cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)col;
        ULONG col_id = col->Id();
        auto it_ = id_map.find(col_id);

        if (it_ == id_map.end()) {
            // inner_col_map.push_back( std::numeric_limits<uint32_t>::max() ); // TODO disabled 231103, bug
        }
        else {
            auto id_idx = it_->second;
            if (colref_table->AttrNum() >= 3) { // i.e., there is edge property except _sid and _tid
                const CName &col_name = colref_table->Name();
                wchar_t *col_name_str, *second_token, *pt;
                col_name_str =
                    new wchar_t[std::wcslen(col_name.Pstr()->GetBuffer()) + 1];
                std::wcscpy(col_name_str, col_name.Pstr()->GetBuffer());
                second_token = std::wcstok(col_name_str, L".", &pt);
                second_token = std::wcstok(NULL, L".", &pt);
                if ((std::wcsncmp(second_token, L"_sid", 4) == 0) ||
                    (std::wcsncmp(second_token, L"_tid", 4) == 0)) {
                    inner_col_map.push_back(id_idx);
                }
                else {
                    load_edge_property = true;
                    inner_col_map_seek.push_back(id_idx);
                }
            }
            else {
                if (colref_table->AttrNum() == -1)
                    load_eid = true;
                inner_col_map.push_back(id_idx);
            }
        }
    }

    bool compare_tgt;
    gpos::ULONG inner_pos, outer_pos;
    if (do_filter_pushdown) {
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id()));
        CColRefTable *rhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](1)->Pop())
                ->Pcr()
                ->Id()));

        outer_pos = outer_cols->IndexOf((CColRef *)lhs_colref);
        if (outer_pos == gpos::ulong_max) {
            inner_pos = idxscan_output->IndexOf((CColRef *)lhs_colref);
            outer_pos = outer_cols->IndexOf((CColRef *)rhs_colref);
            ULONG inner_attno = lhs_colref->AttrNum();
            D_ASSERT(inner_attno >= 0 && inner_attno <= 2);
            compare_tgt = (inner_attno != 0);
        }
        else {
            inner_pos = idxscan_output->IndexOf((CColRef *)rhs_colref);
            D_ASSERT(inner_pos >= 0 && inner_pos <= 2);
            ULONG inner_attno = rhs_colref->AttrNum();
            D_ASSERT(inner_attno >= 0 && inner_attno <= 2);
            compare_tgt = (inner_attno != 0);
        }
        D_ASSERT(inner_pos != gpos::ulong_max);
        D_ASSERT(outer_pos != gpos::ulong_max);
    }

    if (!load_edge_property) {
        /* Generate operator and push */
        duckdb::Schema tmp_schema;
        tmp_schema.setStoredTypes(types);

        duckdb::CypherPhysicalOperator *op;
        if (do_filter_pushdown) {
            op = new duckdb::PhysicalAdjIdxJoin(
                tmp_schema, adjidx_obj_id,
                is_left_outer ? duckdb::JoinType::LEFT
                              : duckdb::JoinType::INNER,
                sid_col_idx, load_eid, outer_col_map, inner_col_map, true,
                outer_pos, inner_pos);
        }
        else {
            op = new duckdb::PhysicalAdjIdxJoin(
                tmp_schema, adjidx_obj_id,
                is_left_outer ? duckdb::JoinType::LEFT
                              : duckdb::JoinType::INNER,
                sid_col_idx, load_eid, outer_col_map, inner_col_map);
        }

        if (generate_sfg) {  // TODO wrong code.. but
            vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
            pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
            num_schemas_of_childs.push_back({prev_local_schemas.size()});
            pipeline_schemas.push_back(prev_local_schemas);
            pipeline_union_schema.push_back(tmp_schema);
        }

        result->push_back(op);
    }
    else {
        // AdjIdxJoin -> Edge Id Seek
        bool load_eid_temporarily = !load_eid;
        duckdb::Schema tmp_schema_seek, tmp_schema_adjidxjoin;
        tmp_schema_seek.setStoredTypes(types);

        if (load_eid) {
            // if we already load eid, use it for id seek operation
            seek_sid_col_idx = inner_col_map[0];
            tmp_schema_adjidxjoin.setStoredTypes(types);

            outer_col_map_seek.resize(types.size());
            for (int i = 0; i < outer_col_map_seek.size(); i++) {
                auto it = std::find(inner_col_map_seek.begin(),
                                    inner_col_map_seek.end(), i);
                if (it == inner_col_map_seek.end())
                    outer_col_map_seek[i] = i;
                else
                    outer_col_map_seek[i] =
                        std::numeric_limits<uint32_t>::max();
            }
        }
        else {
            // if we do not load eid, we need to load eid temporarily for id seek operation

            // append id type & adjust tmp_schema, inner_col_map
            types.push_back(duckdb::LogicalType::ID);
            seek_sid_col_idx = types.size() - 1;
            tmp_schema_adjidxjoin.setStoredTypes(types);
            inner_col_map.push_back(seek_sid_col_idx);

            outer_col_map_seek.resize(types.size() - 1);
            for (int i = 0; i < outer_col_map_seek.size(); i++) {
                auto it = std::find(inner_col_map_seek.begin(),
                                    inner_col_map_seek.end(), i);
                if (it == inner_col_map_seek.end())
                    outer_col_map_seek[i] = i;
                else
                    outer_col_map_seek[i] =
                        std::numeric_limits<uint32_t>::max();
            }
            outer_col_map_seek.push_back(
                std::numeric_limits<uint32_t>::max());  // TODO always useless?
        }

        duckdb::CypherPhysicalOperator *op_adjidxjoin;
        if (do_filter_pushdown) {
            op_adjidxjoin = new duckdb::PhysicalAdjIdxJoin(
                tmp_schema_adjidxjoin, adjidx_obj_id,
                is_left_outer ? duckdb::JoinType::LEFT
                              : duckdb::JoinType::INNER,
                sid_col_idx, true, outer_col_map, inner_col_map, true,
                outer_pos, inner_pos, load_eid_temporarily);
        }
        else {
            op_adjidxjoin = new duckdb::PhysicalAdjIdxJoin(
                tmp_schema_adjidxjoin, adjidx_obj_id,
                is_left_outer ? duckdb::JoinType::LEFT
                              : duckdb::JoinType::INNER,
                sid_col_idx, true, outer_col_map, inner_col_map,
                load_eid_temporarily);
        }


        /**
         * Bugs
         * 1. No schema flow graph for seek operation
         * 2. No schema flow graph for adjidxjoin operation
         * 3. Wrong use of IdSeek operator
        */

        // TODO filter + seek
        duckdb::CypherPhysicalOperator *op_seek = new duckdb::PhysicalIdSeek(
            tmp_schema_seek, seek_sid_col_idx, oids, projection_mapping,
            outer_col_map_seek, inner_col_map_seek);

        result->push_back(op_adjidxjoin);
        result->push_back(op_seek);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(
    CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;
    CPhysicalIndexPathJoin *join_op =
        (CPhysicalIndexPathJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    D_ASSERT(pexprInner->Pop()->Eopid() == COperator::EopPhysicalIndexPathScan);
    CPhysicalIndexPathScan *pathscan_op =
        (CPhysicalIndexPathScan *)pexprInner->Pop();

    unordered_map<ULONG, uint64_t> id_map;
    std::vector<uint32_t> outer_col_map;
    std::vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));
        // fprintf(stdout, "AdjIdxJoin Insert %d %d\n", col_id, col_idx);

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    D_ASSERT(pathscan_op->Pindexdesc()->Size() == 1);
    D_ASSERT(pathscan_op->UpperBound() !=
             -1);  // TODO currently engine does not support infinite hop

    // Get JoinColumnID
    std::vector<uint32_t> sccmp_colids;
    CExpression *parent_exp = pexprInner;
    while (true) {
        CExpression *child_exp = (*parent_exp)[0];
        if (child_exp->Pop()->Eopid() ==
            COperator::EOperatorId::EopScalarIdent) {
            for (uint32_t i = 0; i < parent_exp->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent *)(parent_exp->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }
            break;
        }
        else {
            parent_exp = child_exp;
            continue;
        }
    }

    uint64_t sid_col_idx = 0;
    bool sid_col_idx_found = false;
    // Construct mapping info
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        CColRef *col = outer_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        // find key column
        auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
        if (it != sccmp_colids.end()) {
            D_ASSERT(!sid_col_idx_found);
            sid_col_idx = col_idx;
            sid_col_idx_found = true;
        }
        // construct outer col map
        auto it_ = id_map.find(col_id);
        if (it_ == id_map.end()) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            auto id_idx = id_map.at(col_id);
            outer_col_map.push_back(id_idx);
        }
    }
    D_ASSERT(sid_col_idx_found);

    // construct inner col map. // TODO we don't consider edge property now..
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        // if (col_idx == 0) continue; // 0405 we don't need this condition anymore?
        CColRef *col = inner_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        auto id_idx = id_map.at(
            col_id);  // std::out_of_range exception if col_id does not exist in id_map
        inner_col_map.push_back(id_idx);
    }

    D_ASSERT(pathscan_op->Pindexdesc()->Size() == 1);
    OID path_index_oid =
        CMDIdGPDB::CastMdid(pathscan_op->Pindexdesc()->operator[](0)->MDId())
            ->Oid();

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalVarlenAdjIdxJoin(
        tmp_schema, path_index_oid, duckdb::JoinType::INNER, sid_col_idx, false,
        pathscan_op->LowerBound(), pathscan_op->UpperBound(), outer_col_map,
        inner_col_map);

    result->push_back(op);

    if (generate_sfg) {  // TODO wrong code.. but
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;

    CPhysicalInnerIndexNLJoin *proj_op =
        (CPhysicalInnerIndexNLJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_inner_cols = GPOS_NEW(mp) CColRefSet(mp, outer_cols);
    outer_inner_cols->Include(pexprInner->Prpp()->PcrsRequired());

    unordered_map<ULONG, uint64_t> id_map;
    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;

    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    CExpression *idxscan_expr = NULL;
    duckdb::ExpressionType exp_type;
    CColRefArray *filter_pred_cols = GPOS_NEW(mp) CColRefArray(mp);
    vector<unique_ptr<duckdb::Expression>> filter_exprs;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan) {
            // IdxScan
            idxscan_expr = inner_root;
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            idx_obj_id = (uint64_t)oid;

            // Get JoinColumnID
            for (uint32_t i = 0; i < inner_root->operator[](0)->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent
                         *)(inner_root->operator[](0)->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }

            // TODO there may be additional projection - we CURRENTLY do not consider projection
            CColRefArray *output =
                inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);

            // try seek bypassing
            if ((output->Size() == 0) ||
                (output->Size() == 1 &&
                 pGetColIdxFromTable(
                     CMDIdGPDB::CastMdid(((CColRefTable *)output->operator[](0))
                                             ->GetMdidTable())
                         ->Oid(),
                     output->operator[](0)) == 0)) {
                // nothing changes, we don't need seek, pass directly
                return result;
            }
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            // IndexOnlyScan on physical id index. We don't need to do idseek
            // maybe we need to process filter expression
            D_ASSERT(inner_root->Arity() >= 1);
            CExpression *scalar_expr = inner_root;
            CExpression *scalar_expr_child = scalar_expr->operator[](0);
            vector<unique_ptr<duckdb::Expression>> scalar_filter_exprs;
            if (scalar_expr_child->Pop()->Eopid() != COperator::EopScalarCmp) {
                // we need additional filter
            }

            // if output_cols size != outer_cols, we need to do projection
            /* Generate operator and push */
            duckdb::Schema tmp_schema;
            vector<unique_ptr<duckdb::Expression>> proj_exprs;
            tmp_schema.setStoredTypes(types);

            // bool project_physical_id_column = (output_cols->Size() == outer_cols->Size()); // TODO always works?
            bool project_physical_id_column = true;  // TODO we need a logic..
            pGetAllScalarIdents(inner_root->operator[](0), sccmp_colids);
            for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
                CColRef *col = (*output_cols)[col_idx];
                ULONG idx = outer_cols->IndexOf(col);
                if (idx == gpos::ulong_max) {
                    // if (!project_physical_id_column) continue;
                    // if (types[col_idx].id() == duckdb::LogicalTypeId::ID) {
                    if (!project_physical_id_column) {
                        continue;
                    }
                    else {

                        // for (uint32_t i = 0; i < inner_root->operator[](0)->Arity(); i++) {
                        // 	CScalarIdent *sc_ident = (CScalarIdent *)(inner_root->operator[](0)->operator[](i)->Pop());
                        // 	sccmp_colids.push_back(sc_ident->Pcr()->Id());
                        // }
                        for (ULONG outer_col_idx = 0;
                             outer_col_idx < outer_cols->Size();
                             outer_col_idx++) {
                            CColRef *col = (*outer_cols)[outer_col_idx];
                            ULONG outer_col_id = col->Id();
                            auto it =
                                std::find(sccmp_colids.begin(),
                                          sccmp_colids.end(), outer_col_id);
                            if (it != sccmp_colids.end()) {
                                idx = outer_col_idx;
                                break;
                            }
                        }
                    }
                    if (idx == gpos::ulong_max)
                        continue;
                }
                D_ASSERT(idx != gpos::ulong_max);
                proj_exprs.push_back(
                    make_unique<duckdb::BoundReferenceExpression>(
                        types[col_idx], (int)idx));
            }
            if (proj_exprs.size() != 0) {
                D_ASSERT(proj_exprs.size() == output_cols->Size());
                duckdb::CypherPhysicalOperator *op =
                    new duckdb::PhysicalProjection(tmp_schema,
                                                   std::move(proj_exprs));
                result->push_back(op);

                if (generate_sfg) {  // TODO wrong code.. but
                    vector<duckdb::Schema> prev_local_schemas =
                        pipeline_schemas.back();
                    pipeline_operator_types.push_back(
                        duckdb::OperatorType::UNARY);
                    num_schemas_of_childs.push_back(
                        {prev_local_schemas.size()});
                    pipeline_schemas.push_back(prev_local_schemas);
                    pipeline_union_schema.push_back(tmp_schema);
                }
            }

            // release
            output_cols->Release();
            outer_cols->Release();
            inner_cols->Release();
            return result;
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalFilter) {
            has_filter = true;
            filter_expr = inner_root;
            filter_pred_expr = filter_expr->operator[](1);
            CColRefArray *filter_inner_cols =
                filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(
                    mp);  // idxscan

            if (filter_pred_expr->Pop()->Eopid() == COperator::EopScalarCmp &&
                ((CScalarCmp *)filter_pred_expr->Pop())->ParseCmpType() ==
                    IMDType::ECmpType::EcmptEq) {
                do_filter_pushdown = true;
                D_ASSERT(filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarIdent ||
                         filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarConst);
                D_ASSERT(filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarIdent ||
                         filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarConst);

                CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
                if (filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarIdent) {
                    CColRef *colref = (col_factory->LookupColRef(
                        ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                            ->Pcr()
                            ->Id()));
                    filter_pred_cols->Append(colref);
                }
                if (filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarIdent) {
                    CColRef *colref = (col_factory->LookupColRef(
                        ((CScalarIdent *)filter_pred_expr->operator[](1)->Pop())
                            ->Pcr()
                            ->Id()));
                    filter_pred_cols->Append(colref);
                }
            }
            else {
                do_filter_pushdown = false;
                filter_exprs.push_back(std::move(pTransformScalarExpr(
                    filter_pred_expr, outer_cols, filter_inner_cols)));
            }
            filter_inner_cols->Release();
        }
        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    D_ASSERT(inner_root != pexprInner);

    D_ASSERT(idxscan_expr != NULL);
    CColRefSet *inner_output_cols = pexprInner->Prpp()->PcrsRequired();
    CColRefSet *idxscan_output_cols = idxscan_expr->Prpp()->PcrsRequired();
    D_ASSERT(idxscan_output_cols->ContainsAll(inner_output_cols));

    CPhysicalIndexScan *idxscan_op = (CPhysicalIndexScan *)idxscan_expr->Pop();
    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
    OID table_obj_id = table_mdid->Oid();
    oids.push_back(table_obj_id);

    /**
	 * This code may have problem!
	 * We changed meaning of projection. Therefore, this code should be changed too.
	 * I did not changed since I did not started implementation of IdSeek.
	 * Soon I will change this.
	*/

    // oids / projection_mapping
    vector<vector<uint64_t>> output_projection_mapping;
    vector<uint64_t> output_ident_mapping;
    pGenerateScanMapping(table_obj_id, inner_output_cols->Pdrgpcr(mp),
                         output_ident_mapping);
    D_ASSERT(output_ident_mapping.size() == inner_output_cols->Size());
    output_projection_mapping.push_back(output_ident_mapping);
    // vector<duckdb::LogicalType> output_types;
    // pGenerateTypes(inner_output_cols->Pdrgpcr(mp), output_types);
    // D_ASSERT(output_types.size() == output_ident_mapping.size());

    // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<uint64_t> scan_ident_mapping;
    pGenerateScanMapping(table_obj_id, idxscan_output_cols->Pdrgpcr(mp),
                         scan_ident_mapping);
    vector<duckdb::LogicalType> scan_types;
    pGenerateTypes(idxscan_output_cols->Pdrgpcr(mp), scan_types);
    D_ASSERT(scan_ident_mapping.size() == idxscan_output_cols->Size());
    scan_projection_mapping.push_back(scan_ident_mapping);

    bool sid_col_idx_found = false;
    // Construct mapping info
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        CColRef *col = outer_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        // match _tid
        auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
        if (it != sccmp_colids.end()) {
            D_ASSERT(!sid_col_idx_found);
            sid_col_idx = col_idx;
            sid_col_idx_found = true;
        }
        // construct outer_col_map
        auto it_ = id_map.find(col_id);
        if (it_ == id_map.end()) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            auto id_idx = id_map.at(
                col_id);  // std::out_of_range exception if col_id does not exist in id_map
            outer_col_map.push_back(id_idx);
        }
    }
    D_ASSERT(sid_col_idx_found);

    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        CColRef *col = inner_cols->operator[](col_idx);
        ULONG col_id = col->Id();
        auto id_idx = id_map.at(
            col_id);  // std::out_of_range exception if col_id does not exist in id_map
        inner_col_map.push_back(id_idx);
    }
    // for (ULONG col_idx = 0; col_idx < idxscan_output_cols_arr->Size(); col_idx++) {
    // 	CColRef* col = idxscan_output_cols_arr->operator[](col_idx);
    // 	ULONG col_id = col->Id();
    // 	auto id_idx = id_map.at(col_id); // std::out_of_range exception if col_id does not exist in id_map
    // 	inner_col_map.push_back(id_idx);
    // }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id()));
        gpos::INT lhs_attrnum = lhs_colref->AttrNum();
        pred_attr_pos = lGetMDAccessor()
                            ->RetrieveRel(lhs_colref->GetMdidTable())
                            ->GetPosFromAttno(lhs_attrnum);
        pred_attr_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(lhs_colref->RetrieveType()->MDId())->Oid());
        pred_pos = output_cols->IndexOf((CColRef *)lhs_colref);
        CDatumGenericGPDB *datum =
            (CDatumGenericGPDB
                 *)(((CScalarConst *)filter_pred_expr->operator[](1)->Pop())
                        ->GetDatum());
        literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
            CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
            datum->GetByteArrayValue(), (uint64_t)datum->Size());
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (!do_filter_pushdown) {
        if (has_filter) {
            // D_ASSERT(scan_projection_mapping == output_projection_mapping); // TODO we currently support scan = output
            // duckdb::CypherPhysicalOperator *op =
            // 	new duckdb::PhysicalIdSeek(tmp_schema, sid_col_idx, oids, scan_projection_mapping, outer_col_map, inner_col_map, move(filter_exprs));
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_map, inner_col_map, scan_types,
                scan_projection_mapping, move(filter_exprs));
            result->push_back(op);
        }
        else {
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_map, inner_col_map);
            result->push_back(op);
        }
    }
    else {
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
            tmp_schema, sid_col_idx, oids, output_projection_mapping,
            outer_col_map, inner_col_map, scan_types, scan_projection_mapping,
            pred_attr_pos, literal_val);
        result->push_back(op);
    }

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    outer_inner_cols->Release();

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInner(
    CExpression *plan_expr)
{
    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CDrvdPropPlan *drvd_prop_plan = plan_expr->GetDrvdPropPlan();
    if (drvd_prop_plan->Pos()->UlSortColumns() > 0) {
        pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(
            plan_expr, result);
    }
    else {
        pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(
            plan_expr, result);
    }

    return result;
}

void Planner::
    pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(
        CExpression *plan_expr,
        vector<duckdb::CypherPhysicalOperator *> *result)
{
    CMemoryPool *mp = this->memory_pool;
    vector<duckdb::LogicalType> types;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_inner_cols = GPOS_NEW(mp) CColRefSet(mp, outer_cols);
    outer_inner_cols->Include(pexprInner->Prpp()->PcrsRequired());

    unordered_map<ULONG, uint64_t> id_map;
    vector<vector<uint32_t>> outer_col_maps;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<vector<uint64_t>> output_projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<duckdb::LogicalType>> scan_types;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalSerialUnionAll) {
            for (uint32_t i = 0; i < inner_root->Arity();
                 i++) {  // for each idx(only)scan expression
                // TODO currently support this pattern type only
                D_ASSERT(
                    inner_root->operator[](i)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalIndexScan);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](1)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarProjectList);

                CExpression *unionall_expr = inner_root;
                CExpression *inner_idxscan_expr =
                    inner_root->operator[](i)->operator[](0);
                CExpression *projectlist_expr =
                    inner_root->operator[](i)->operator[](1);

                // Get JoinColumnID
                for (uint32_t j = 0;
                     j < inner_idxscan_expr->operator[](0)->Arity(); j++) {
                    CScalarIdent *sc_ident =
                        (CScalarIdent *)(inner_idxscan_expr->operator[](0)
                                             ->
                                             operator[](j)
                                             ->Pop());
                    sccmp_colids.push_back(sc_ident->Pcr()->Id());
                }

                D_ASSERT(inner_idxscan_expr != NULL);
                CColRefSet *unionall_output_cols =
                    unionall_expr->Prpp()->PcrsRequired();
                CColRefSet *inner_output_cols =
                    pexprInner->Prpp()->PcrsRequired();
                CColRefSet *idxscan_output_cols =
                    inner_idxscan_expr->Prpp()->PcrsRequired();
                // D_ASSERT(idxscan_output_cols->ContainsAll(inner_output_cols));

                CPhysicalIndexScan *idxscan_op =
                    (CPhysicalIndexScan *)inner_idxscan_expr->Pop();
                OID table_obj_id =
                    CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId())->Oid();
                oids.push_back(table_obj_id);

                // oids / projection_mapping
                vector<uint64_t> output_ident_mapping;
                pGenerateScanMapping(table_obj_id,
                                     inner_output_cols->Pdrgpcr(mp),
                                     output_ident_mapping);
                D_ASSERT(output_ident_mapping.size() ==
                         inner_output_cols->Size());
                output_projection_mapping.push_back(output_ident_mapping);
                vector<duckdb::LogicalType> output_types;
                pGenerateTypes(inner_output_cols->Pdrgpcr(mp), output_types);
                D_ASSERT(output_types.size() == output_ident_mapping.size());

                // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
                vector<uint64_t> scan_ident_mapping;
                vector<duckdb::LogicalType> scan_type;

                bool load_system_col = false;
                bool sid_col_idx_found = false;

                outer_col_maps.push_back(std::vector<uint32_t>());
                inner_col_maps.push_back(std::vector<uint32_t>());

                if (i == 0) {
                    for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                        D_ASSERT(
                            projectlist_expr->operator[](j)->Pop()->Eopid() ==
                            COperator::EOperatorId::EopScalarProjectElement);
                        CScalarProjectElement *proj_elem =
                            (CScalarProjectElement
                                 *)(projectlist_expr->operator[](j)->Pop());
                        CColRefTable *proj_col =
                            (CColRefTable *)proj_elem->Pcr();
                        auto it = id_map.find(proj_col->ColId());
                        if (it != id_map.end()) {
                            union_inner_col_map.push_back(it->second);
                            if (proj_col->AttrNum() == INT(-1))
                                load_system_col = true;
                        }
                    }
                }

                // Construct inner mapping, scan projection mapping, scan type infos
                for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                    D_ASSERT(projectlist_expr->operator[](j)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarProjectElement);
                    CScalarProjectElement *proj_elem =
                        (CScalarProjectElement
                             *)(projectlist_expr->operator[](j)->Pop());
                    CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();

                    if (projectlist_expr->operator[](j)
                            ->
                            operator[](0)
                            ->Pop()
                            ->Eopid() ==
                        COperator::EOperatorId::EopScalarIdent) {
                        // non-null column
                        // This logic is built on the assumption that all columns except the system column will be included in the seek output
                        if (load_system_col) {
                            inner_col_maps[i].push_back(union_inner_col_map[j]);
                        }
                        else if (!load_system_col && (j != 0)) {
                            inner_col_maps[i].push_back(
                                union_inner_col_map[j - 1]);
                        }

                        INT attr_no = proj_col->AttrNum();
                        if ((attr_no == (INT)-1)) {
                            if (load_system_col) {
                                scan_ident_mapping.push_back(0);
                                scan_type.push_back(duckdb::LogicalType::ID);
                            }
                        }
                        else {
                            scan_ident_mapping.push_back(attr_no);
                            CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                                proj_col->RetrieveType()->MDId());
                            OID type_oid = type_mdid->Oid();
                            scan_type.push_back(
                                pConvertTypeOidToLogicalType(type_oid));
                        }
                    }
                    else if (projectlist_expr->operator[](j)
                                 ->
                                 operator[](0)
                                 ->Pop()
                                 ->Eopid() ==
                             COperator::EOperatorId::EopScalarConst) {
                        // null column
                    }
                    else {
                        throw duckdb::InvalidInputException(
                            "Project element types other than ident & const is "
                            "not desired");
                    }
                }

                scan_projection_mapping.push_back(scan_ident_mapping);
                scan_types.push_back(std::move(scan_type));

                // Construct outer mapping info
                for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                     col_idx++) {
                    CColRef *col = outer_cols->operator[](col_idx);
                    ULONG col_id = col->Id();
                    // match _tid
                    auto it = std::find(sccmp_colids.begin(),
                                        sccmp_colids.end(), col_id);
                    if (it != sccmp_colids.end()) {
                        D_ASSERT(!sid_col_idx_found);
                        sid_col_idx = col_idx;
                        sid_col_idx_found = true;
                    }
                    // construct outer_col_map
                    auto it_ = id_map.find(col_id);
                    if (it_ == id_map.end()) {
                        outer_col_maps[i].push_back(
                            std::numeric_limits<uint32_t>::max());
                    }
                    else {
                        auto id_idx = id_map.at(
                            col_id);  // std::out_of_range exception if col_id does not exist in id_map
                        outer_col_maps[i].push_back(id_idx);
                    }
                }
                D_ASSERT(sid_col_idx_found);
            }
        }

        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (!do_filter_pushdown) {
        if (has_filter) {
            throw NotImplementedException("InnerIdxNLJoin for Filter case");
        }
        else {
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_maps, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, true);
            result->push_back(op);
        }
    }
    else {
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    outer_inner_cols->Release();
}

void Planner::
    pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(
        CExpression *plan_expr,
        vector<duckdb::CypherPhysicalOperator *> *result)
{
    CMemoryPool *mp = this->memory_pool;

    vector<duckdb::LogicalType> types;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefSet *outer_inner_cols = GPOS_NEW(mp) CColRefSet(mp, outer_cols);
    outer_inner_cols->Include(pexprInner->Prpp()->PcrsRequired());

    unordered_map<ULONG, uint64_t> id_map;
    vector<vector<uint32_t>> outer_col_maps;
    vector<vector<uint32_t>> inner_col_maps;
    vector<uint32_t> union_inner_col_map;
    vector<vector<uint64_t>> output_projection_mapping;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<duckdb::LogicalType>> scan_types;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    uint64_t idx_obj_id;  // 230303
    uint64_t sid_col_idx;
    CExpression *inner_root = pexprInner;

    vector<uint64_t> oids;
    vector<uint32_t> sccmp_colids;
    vector<uint32_t> scident_colids;

    bool do_projection_on_idxscan = false;
    bool do_filter_pushdown = false;
    bool has_filter = false;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalSerialUnionAll) {
            for (uint32_t i = 0; i < inner_root->Arity();
                 i++) {  // for each idx(only)scan expression
                // TODO currently support this pattern type only
                D_ASSERT(
                    inner_root->operator[](i)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalIndexScan);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](1)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarProjectList);

                CExpression *unionall_expr = inner_root;
                CExpression *inner_idxscan_expr =
                    inner_root->operator[](i)->operator[](0);
                CExpression *projectlist_expr =
                    inner_root->operator[](i)->operator[](1);

                // Get JoinColumnID
                for (uint32_t j = 0;
                     j < inner_idxscan_expr->operator[](0)->Arity(); j++) {
                    CScalarIdent *sc_ident =
                        (CScalarIdent *)(inner_idxscan_expr->operator[](0)
                                             ->
                                             operator[](j)
                                             ->Pop());
                    sccmp_colids.push_back(sc_ident->Pcr()->Id());
                }

                D_ASSERT(inner_idxscan_expr != NULL);
                CColRefSet *unionall_output_cols =
                    unionall_expr->Prpp()->PcrsRequired();
                CColRefSet *inner_output_cols =
                    pexprInner->Prpp()->PcrsRequired();
                CColRefSet *idxscan_output_cols =
                    inner_idxscan_expr->Prpp()->PcrsRequired();
                // D_ASSERT(idxscan_output_cols->ContainsAll(inner_output_cols));

                CPhysicalIndexScan *idxscan_op =
                    (CPhysicalIndexScan *)inner_idxscan_expr->Pop();
                OID table_obj_id =
                    CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId())->Oid();
                oids.push_back(table_obj_id);

                // scan projection mapping - when doing filter pushdown, two mappings MAY BE different.
                vector<uint64_t> scan_ident_mapping;
                vector<duckdb::LogicalType> scan_type;

                bool load_system_col = false;
                bool sid_col_idx_found = false;

                outer_col_maps.push_back(std::vector<uint32_t>());
                inner_col_maps.push_back(std::vector<uint32_t>());

                // projection mapping (output to scan table mapping)
                vector<uint64_t> output_ident_mapping;

                if (i == 0) {
                    for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                        D_ASSERT(
                            projectlist_expr->operator[](j)->Pop()->Eopid() ==
                            COperator::EOperatorId::EopScalarProjectElement);
                        CScalarProjectElement *proj_elem =
                            (CScalarProjectElement
                                 *)(projectlist_expr->operator[](j)->Pop());
                        CColRefTable *proj_col =
                            (CColRefTable *)proj_elem->Pcr();
                        auto it = id_map.find(proj_col->ColId());
                        if (it != id_map.end()) {
                            union_inner_col_map.push_back(it->second);
                            if (proj_col->AttrNum() == INT(-1))
                                load_system_col = true;
                        }
                    }
                }

                // Construct innter mapping, scan projection mapping, scan type infos
                for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                    D_ASSERT(projectlist_expr->operator[](j)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarProjectElement);
                    CScalarProjectElement *proj_elem =
                        (CScalarProjectElement
                             *)(projectlist_expr->operator[](j)->Pop());
                    CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();

                    if (projectlist_expr->operator[](j)
                            ->
                            operator[](0)
                            ->Pop()
                            ->Eopid() ==
                        COperator::EOperatorId::EopScalarIdent) {
                        // non-null column
                        // This logic is built on the assumption that all columns except the system column will be included in the seek output
                        if (load_system_col) {
                            inner_col_maps[i].push_back(union_inner_col_map[j]);
                        }
                        else if (!load_system_col && (j != 0)) {
                            inner_col_maps[i].push_back(
                                union_inner_col_map[j - 1]);
                        }

                        INT attr_no = proj_col->AttrNum();
                        if ((attr_no == (INT)-1)) {
                            if (load_system_col) {
                                scan_ident_mapping.push_back(0);
                                output_ident_mapping.push_back(j);
                                scan_type.push_back(duckdb::LogicalType::ID);
                            }
                        }
                        else {
                            scan_ident_mapping.push_back(attr_no);
                            output_ident_mapping.push_back(j);
                            CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                                proj_col->RetrieveType()->MDId());
                            OID type_oid = type_mdid->Oid();
                            scan_type.push_back(
                                pConvertTypeOidToLogicalType(type_oid));
                        }
                    }
                    else if (projectlist_expr->operator[](j)
                                 ->
                                 operator[](0)
                                 ->Pop()
                                 ->Eopid() ==
                             COperator::EOperatorId::EopScalarConst) {
                        // null column
                    }
                    else {
                        throw duckdb::InvalidInputException(
                            "Project element types other than ident & const is "
                            "not desired");
                    }
                }

                scan_projection_mapping.push_back(scan_ident_mapping);
                scan_types.push_back(std::move(scan_type));
                output_projection_mapping.push_back(output_ident_mapping);

                // Construct outer mapping info
                for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                     col_idx++) {
                    CColRef *col = outer_cols->operator[](col_idx);
                    ULONG col_id = col->Id();
                    // match _tid
                    auto it = std::find(sccmp_colids.begin(),
                                        sccmp_colids.end(), col_id);
                    if (it != sccmp_colids.end()) {
                        D_ASSERT(!sid_col_idx_found);
                        sid_col_idx = col_idx;
                        sid_col_idx_found = true;
                    }
                    // construct outer_col_map
                    auto it_ = id_map.find(col_id);
                    if (it_ == id_map.end()) {
                        outer_col_maps[i].push_back(
                            std::numeric_limits<uint32_t>::max());
                    }
                    else {
                        auto id_idx = id_map.at(
                            col_id);  // std::out_of_range exception if col_id does not exist in id_map
                        outer_col_maps[i].push_back(id_idx);
                    }
                }
                D_ASSERT(sid_col_idx_found);
            }
        }

        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (!do_filter_pushdown) {
        if (has_filter) {
            throw NotImplementedException("InnerIdxNLJoin for Filter case");
        }
        else {
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_maps, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, false);
            result->push_back(op);
        }
    }
    else {
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /**
     * TODO: this code is currently wrong. It should be fixed.
     * IdSeek should generate multiple schemas.
     * However, this code now only generates one schema (union schema).
     * Instead, it uses tmp_schema given to the PhysicalIdSeek to initialize.
    */

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        // num_schemas_of_childs.push_back({prev_local_schemas.size(), inner_col_maps.size() + 1});
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    outer_inner_cols->Release();
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalHashJoinToHashJoin(CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerHashJoin *expr_op =
        (CPhysicalInnerHashJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprLeft = (*plan_expr)[0];
    CColRefArray *left_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = (*plan_expr)[1];
    CColRefArray *right_cols;
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::LogicalType> types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
        types.push_back(col_type);
    }
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(left_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            left_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            left_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(right_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            right_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            right_col_map.push_back(idx);
        }
    }

    duckdb::JoinType join_type = pTranslateJoinType(expr_op);
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    vector<duckdb::LogicalType> right_build_types;
    vector<uint64_t> right_build_map;
    if (join_type != duckdb::JoinType::ANTI &&
        join_type != duckdb::JoinType::SEMI &&
        join_type != duckdb::JoinType::MARK) {
        // hash build type (output_cols - left_cols; where values are required)
        // non-key columns, which means columns to be outputted after probing
        CColRefSet *cols_to_build = GPOS_NEW(mp) CColRefSet(mp);
        cols_to_build->Include(plan_expr->Prpp()->PcrsRequired());
        cols_to_build->Difference(pexprLeft->Prpp()->PcrsRequired());
        CColRefArray *cols_build_list = cols_to_build->Pdrgpcr(mp);
        // right_build_types, right_build_map
        for (ULONG col_idx = 0; col_idx < cols_build_list->Size(); col_idx++) {
            auto idx =
                right_cols->IndexOf(cols_build_list->operator[](col_idx));
            right_build_map.push_back(idx);
            OID type_oid =
                CMDIdGPDB::CastMdid(cols_build_list->operator[](col_idx)
                                        ->RetrieveType()
                                        ->MDId())
                    ->Oid();
            duckdb::LogicalType col_type =
                pConvertTypeOidToLogicalType(type_oid);
            right_build_types.push_back(col_type);
        }
        D_ASSERT(right_build_map.size() == right_build_types.size());
    }
    else {
        D_ASSERT(false);
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    // generate conditions
    vector<duckdb::JoinCondition> join_conds;
    pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                       left_cols, right_cols);

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalHashJoin(
        schema, move(join_conds), join_type, left_col_map, right_col_map,
        right_build_types, right_build_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalMergeJoinToMergeJoin(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerHashJoin *expr_op =
        (CPhysicalInnerHashJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprLeft = (*plan_expr)[0];
    CColRefArray *left_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = (*plan_expr)[1];
    CColRefArray *right_cols;
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalSerialUnionAll) {
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::LogicalType> types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
        types.push_back(col_type);
    }
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(left_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            left_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            left_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(right_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            right_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            right_col_map.push_back(idx);
        }
    }

    duckdb::JoinType join_type = pTranslateJoinType(expr_op);
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    // generate conditions
    vector<duckdb::JoinCondition> join_conds;
    pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                       left_cols, right_cols);

    // Calculate lhs and rhs types
    vector<duckdb::LogicalType> lhs_types;
    vector<duckdb::LogicalType> rhs_types;
    for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
        OID type_oid =
            CMDIdGPDB::CastMdid(
                left_cols->operator[](col_idx)->RetrieveType()->MDId())
                ->Oid();
        lhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        OID type_oid =
            CMDIdGPDB::CastMdid(
                right_cols->operator[](col_idx)->RetrieveType()->MDId())
                ->Oid();
        rhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalPiecewiseMergeJoin(
        schema, move(join_conds), join_type, lhs_types, rhs_types, left_col_map,
        right_col_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalInnerNLJoinToCartesianProduct(
    CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;

    D_ASSERT(plan_expr->Arity() == 3);

    /* Non-root - call LHS first for inner materialization */
    vector<duckdb::CypherPhysicalOperator *> *lhs_result =
        pTraverseTransformPhysicalPlan(
            plan_expr->PdrgPexpr()->operator[](0));  // outer
    vector<duckdb::CypherPhysicalOperator *> *rhs_result =
        pTraverseTransformPhysicalPlan(
            plan_expr->PdrgPexpr()->operator[](1));  // inner

    /* finish rhs pipeline */
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    vector<duckdb::LogicalType> types;
    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
        types.push_back(col_type);
    }
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        outer_col_map.push_back(
            output_cols->IndexOf(outer_cols->operator[](col_idx)));
    }
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        inner_col_map.push_back(
            output_cols->IndexOf(inner_cols->operator[](col_idx)));
    }

    // define op
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalCrossProduct(
        tmp_schema, outer_col_map, inner_col_map);

    // finish rhs pipeline
    rhs_result->push_back(op);
    auto pipeline = new duckdb::CypherPipeline(*rhs_result);
    pipelines.push_back(pipeline);

    // return lhs pipeline
    lhs_result->push_back(op);
    return lhs_result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalNLJoinToBlockwiseNLJoin(CExpression *plan_expr,
                                                      bool is_correlated)
{

    CMemoryPool *mp = this->memory_pool;

    D_ASSERT(plan_expr->Arity() == 3);

    /* Non-root - call left child */
    vector<duckdb::CypherPhysicalOperator *> *lhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    vector<duckdb::CypherPhysicalOperator *> *rhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](1));

    CPhysicalInnerNLJoin *expr_op = (CPhysicalInnerNLJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    vector<duckdb::LogicalType> types;
    vector<uint32_t> outer_col_map;
    vector<uint32_t> inner_col_map;

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = output_cols->operator[](col_idx);
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
        types.push_back(col_type);
    }
    for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(outer_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            outer_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            outer_col_map.push_back(idx);
        }
    }
    for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
        auto idx = output_cols->IndexOf(inner_cols->operator[](col_idx));
        if (idx == gpos::ulong_max) {
            inner_col_map.push_back(std::numeric_limits<uint32_t>::max());
        }
        else {
            inner_col_map.push_back(idx);
        }
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    duckdb::JoinType join_type = pTranslateJoinType(expr_op);
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    auto join_condition_expr = pTransformScalarExpr(
        (*plan_expr)[2], outer_cols, inner_cols);  // left - right

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalBlockwiseNLJoin(
        schema, move(join_condition_expr), join_type, outer_col_map,
        inner_col_map);

    if (is_correlated) {
        // finish lhs pipeline
        lhs_result->push_back(op);
        auto pipeline = new duckdb::CypherPipeline(*lhs_result);
        pipelines.push_back(pipeline);

        // return rhs pipeline
        rhs_result->push_back(op);
        return rhs_result;
    }
    else {
        // finish rhs pipeline
        rhs_result->push_back(op);
        auto pipeline = new duckdb::CypherPipeline(*rhs_result);
        pipelines.push_back(pipeline);

        // return lhs pipeline
        lhs_result->push_back(op);
        return lhs_result;
    }
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopLimit(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CExpression *limit_expr = plan_expr;
    CPhysicalLimit *limit_op = (CPhysicalLimit *)limit_expr->Pop();
    D_ASSERT(limit_expr->operator[](1)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);
    D_ASSERT(limit_expr->operator[](2)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);

    if (!limit_op->FHasCount())
        return result;

    int64_t offset, limit;
    CDatumInt8GPDB *offset_datum =
        (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](1)->Pop())
                               ->GetDatum());
    CDatumInt8GPDB *limit_datum =
        (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](2)->Pop())
                               ->GetDatum());
    offset = offset_datum->Value();
    limit = limit_datum->Value();

    duckdb::Schema tmp_schema;
    duckdb::CypherPhysicalOperator *last_op = result->back();
    tmp_schema.setStoredTypes(last_op->GetTypes());
    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalTop(tmp_schema, limit, offset);
    result->push_back(op);

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopProjectionColumnar(CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<unique_ptr<duckdb::Expression>> proj_exprs;
    vector<duckdb::LogicalType> types;
    vector<string> output_column_names;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    CPhysicalComputeScalarColumnar *proj_op =
        (CPhysicalComputeScalarColumnar *)plan_expr->Pop();
    CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
    CColRefArray *child_cols =
        pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprProjList = (*plan_expr)[1];  // Projection list

    // decide which proj elems to project - keep colref orders
    vector<ULONG> indices_to_project;
    for (ULONG ocol = 0; ocol < output_cols->Size(); ocol++) {
        bool found = false;
        for (ULONG elem_idx = 0; elem_idx < pexprProjList->Arity();
             elem_idx++) {
            if (((CScalarProjectElement *)(pexprProjList->operator[](elem_idx)
                                               ->Pop()))
                    ->Pcr()
                    ->Id() == output_cols->operator[](ocol)->Id()) {
                // matching ColRef found
                indices_to_project.push_back(elem_idx);
                found = true;
                break;
            }
        }
        if (!found) {
            throw duckdb::InvalidInputException("Projection column not found");
        }
    }

    // output_column_names.resize(output_cols->Size());
    // proj_exprs.resize(output_cols->Size());
    // types.resize(output_cols->Size());

    // for (auto i = 0; i < output_cols->Size(); i++) {
    //     CExpression *pexprProjElem =
    //         pexprProjList->operator[](i);  // CScalarProjectElement
    //     CExpression *pexprScalarExpr =
    //         pexprProjElem->operator[](0);  // CScalar... - expr tree root
    //     D_ASSERT(pexprScalarExpr->Pop()->Eopid() == COperator::EopScalarIdent);

    //     output_column_names[i] = std::move(pGetColNameFromColRef(
    //         ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));
    //     proj_exprs[i] =
    //         std::move(pTransformScalarExpr(pexprScalarExpr, child_cols));
    //     types[i] = proj_exprs[i]->return_type;
    // }

    for (auto &elem_idx : indices_to_project) {
        CExpression *pexprProjElem =
            pexprProjList->operator[](elem_idx);  // CScalarProjectElement
        CExpression *pexprScalarExpr =
            pexprProjElem->operator[](0);  // CScalar... - expr tree root

        output_column_names.push_back(pGetColNameFromColRef(
            ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));
        proj_exprs.push_back(
            std::move(pTransformScalarExpr(pexprScalarExpr, child_cols)));
        types.push_back(proj_exprs.back()->return_type);
    }

    // may be less, since we project duplicate projetions only once
    D_ASSERT(pexprProjList->Arity() >= proj_exprs.size());

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(output_column_names);

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalProjection(tmp_schema, std::move(proj_exprs));
    result->push_back(op);

    child_cols->Release();

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopAgg(
    CExpression *plan_expr)
{

    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    vector<duckdb::LogicalType> types;
    vector<duckdb::LogicalType> proj_types;
    vector<unique_ptr<duckdb::Expression>> agg_exprs;
    vector<unique_ptr<duckdb::Expression>> agg_groups;
    vector<string> output_column_names;
    // vector<duckdb::LogicalType> groups_type;
    // vector<ULONG> groups_idx;
    vector<ULONG> proj_mapping;

    CPhysicalAgg *agg_op = (CPhysicalAgg *)plan_expr->Pop();
    CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *child_cols =
        pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *interm_output_cols =
        plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
    CExpression *pexprProjList = (*plan_expr)[1];  // Projection list
    const CColRefArray *grouping_cols = agg_op->PdrgpcrGroupingCols();
    vector<unique_ptr<duckdb::Expression>> proj_exprs;
    vector<unique_ptr<duckdb::Expression>> post_proj_exprs;

    // get agg groups
    for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
         output_col_idx++) {
        CColRef *col = output_cols->operator[](output_col_idx);
        if (grouping_cols->IndexOf(col) == gpos::ulong_max)
            continue;
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
        types.push_back(col_type);
        proj_types.push_back(col_type);
        output_column_names.push_back(pGetColNameFromColRef(col));
        proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_cols->IndexOf(col)));
        agg_groups.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_cols->IndexOf(col)));
        proj_mapping.push_back(child_cols->IndexOf(col));
    }

    bool has_pre_projection = false;
    bool has_post_projection = false;
    bool adjust_agg_groups_performed = false;
    // handle aggregation expressions
    for (ULONG elem_idx = 0; elem_idx < pexprProjList->Arity(); elem_idx++) {
        CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);
        CExpression *pexprScalarExpr = pexprProjElem->operator[](0);
        CExpression *pexprAggExpr;

        output_column_names.push_back(pGetColNameFromColRef(
            ((CScalarProjectElement *)pexprProjElem->Pop())->Pcr()));

        if (pexprScalarExpr->Pop()->Eopid() != COperator::EopScalarAggFunc) {
            has_post_projection = true;
        }
        else {
            CExpression *aggargs_expr = pexprScalarExpr->operator[](0);
            if (aggargs_expr->Arity() == 0) {  // no child
                agg_exprs.push_back(std::move(
                    pTransformScalarExpr(pexprScalarExpr, child_cols)));
                types.push_back(agg_exprs.back()->return_type);
                continue;
            }
            if (aggargs_expr->operator[](0)->Pop()->Eopid() !=
                COperator::EopScalarIdent) {
                has_pre_projection = true;
                if (!adjust_agg_groups_performed) {
                    for (ULONG agg_group_idx = 0;
                         agg_group_idx < agg_groups.size(); agg_group_idx++) {
                        auto agg_group_expr =
                            (duckdb::BoundReferenceExpression *)
                                agg_groups[agg_group_idx]
                                    .get();
                        agg_group_expr->index = agg_group_idx;
                    }
                    ULONG accm_agg_expr_idx = 0;
                    for (ULONG agg_expr_idx = 0;
                         agg_expr_idx < agg_exprs.size(); agg_expr_idx++) {
                        auto agg_expr = (duckdb::BoundAggregateExpression *)
                                            agg_exprs[agg_expr_idx]
                                                .get();
                        for (ULONG agg_expr_child_idx = 0;
                             agg_expr_child_idx < agg_expr->children.size();
                             agg_expr_child_idx++) {
                            auto bound_expr =
                                (duckdb::BoundReferenceExpression *)agg_expr
                                    ->children[agg_expr_child_idx]
                                    .get();
                            bound_expr->index =
                                agg_groups.size() + accm_agg_expr_idx++;
                        }
                    }
                    adjust_agg_groups_performed = true;
                }
                proj_exprs.push_back(std::move(pTransformScalarExpr(
                    aggargs_expr->operator[](0), child_cols)));
                agg_exprs.push_back(std::move(pTransformScalarAggFunc(
                    pexprScalarExpr, child_cols, proj_exprs.back()->return_type,
                    proj_exprs.size() - 1)));
                proj_types.push_back(proj_exprs.back()->return_type);
                types.push_back(agg_exprs.back()->return_type);
            }
            else {
                proj_exprs.push_back(std::move(pTransformScalarExpr(
                    aggargs_expr->operator[](0), child_cols)));
                if (has_pre_projection) {
                    agg_exprs.push_back(std::move(
                        pTransformScalarAggFunc(pexprScalarExpr, child_cols,
                                                proj_exprs.back()->return_type,
                                                proj_exprs.size() - 1)));
                }
                else {
                    agg_exprs.push_back(std::move(
                        pTransformScalarExpr(pexprScalarExpr, child_cols)));
                }
                proj_types.push_back(proj_exprs.back()->return_type);
                types.push_back(agg_exprs.back()->return_type);
            }
        }
    }

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);
    tmp_schema.setStoredColumnNames(output_column_names);

    if (has_pre_projection) {
        duckdb::Schema proj_schema;
        proj_schema.setStoredTypes(proj_types);
        proj_schema.setStoredColumnNames(output_column_names);
        duckdb::CypherPhysicalOperator *proj_op =
            new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
        result->push_back(proj_op);
    }

    duckdb::CypherPhysicalOperator *op;
    if (agg_groups.empty()) {
        op = new duckdb::PhysicalHashAggregate(tmp_schema, move(agg_exprs));
    }
    else {
        op = new duckdb::PhysicalHashAggregate(tmp_schema, move(agg_exprs),
                                               move(agg_groups));
    }

    // finish pipeline
    result->push_back(op);
    auto pipeline = new duckdb::CypherPipeline(*result);
    pipelines.push_back(pipeline);

    if (generate_sfg) {
        // Generate for the previous pipeline
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
        pGenerateSchemaFlowGraph(*result);

        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    // new pipeline
    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    // if output_cols size != child_cols, we need to do projection

    // TODO use of interm_output_cols is wrong
    // if (interm_output_cols->Size() != output_cols->Size()) {
    // 	duckdb::Schema proj_schema;
    // 	vector<duckdb::LogicalType> proj_types;
    // 	for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
    // 		CColRef *col = (*output_cols)[col_idx];
    // 		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId() );
    // 		OID type_oid = type_mdid->Oid();
    // 		proj_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    // 	}
    // 	proj_schema.setStoredTypes(proj_types);

    // 	vector<unique_ptr<duckdb::Expression>> proj_exprs;
    // 	for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
    // 		CColRef *col = (*output_cols)[col_idx];
    // 		ULONG idx = interm_output_cols->IndexOf(col);
    // 		if (idx == gpos::ulong_max) { continue;	}
    // 		D_ASSERT(idx != gpos::ulong_max);
    // 		proj_exprs.push_back(
    // 			make_unique<duckdb::BoundReferenceExpression>(proj_types[col_idx], (int)idx));
    // 	}
    // 	if (proj_exprs.size() != 0) {
    // 		D_ASSERT(proj_exprs.size() == output_cols->Size());
    // 		duckdb::CypherPhysicalOperator* op =
    // 			new duckdb::PhysicalProjection(proj_schema, std::move(proj_exprs));
    // 		new_result->push_back(op);
    // 	}
    // }
    return new_result;
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopPhysicalFilter(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CPhysicalFilter *filter_op = (CPhysicalFilter *)plan_expr->Pop();
    CExpression *filter_expr = plan_expr;
    CExpression *filter_pred_expr = filter_expr->operator[](1);
    vector<unique_ptr<duckdb::Expression>> filter_exprs;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    vector<duckdb::LogicalType> output_types;
    pGetColumnsDuckDBType(output_cols, output_types);

    duckdb::ExpressionType exp_type;
    // D_ASSERT(filter_pred_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
    // CScalarCmp *sccmp = (CScalarCmp *)filter_pred_expr->Pop();
    // exp_type = pTranslateCmpType(sccmp->ParseCmpType());

    // pGenerateFilterExprs(outer_cols, exp_type, filter_pred_expr, filter_exprs);
    filter_exprs.push_back(
        std::move(pTransformScalarExpr(filter_pred_expr, outer_cols, nullptr)));

    duckdb::Schema tmp_schema;
    duckdb::CypherPhysicalOperator *last_op = result->back();
    tmp_schema.setStoredTypes(last_op->GetTypes());
    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalFilter(tmp_schema, move(filter_exprs));
    result->push_back(op);

    // we need further projection if we don't need filter column anymore
    if (output_cols->Size() != outer_cols->Size()) {
        duckdb::Schema output_schema;
        output_schema.setStoredTypes(output_types);
        vector<unique_ptr<duckdb::Expression>> proj_exprs;
        for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
            CColRef *col = (*output_cols)[col_idx];
            ULONG idx = outer_cols->IndexOf(col);
            D_ASSERT(idx != gpos::ulong_max);
            proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
                output_types[col_idx], (int)idx));
        }
        if (proj_exprs.size() != 0) {
            D_ASSERT(proj_exprs.size() == output_cols->Size());
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalProjection(
                output_schema, std::move(proj_exprs));
            result->push_back(op);
        }
    }

    return result;
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopSort(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    CPhysicalSort *proj_op = (CPhysicalSort *)plan_expr->Pop();

    const COrderSpec *pos = proj_op->Pos();

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    duckdb::CypherPhysicalOperator *last_op = result->back();
    auto &last_op_result_types = last_op->GetTypes();

    vector<duckdb::BoundOrderByNode> orders;
    for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++) {
        const CColRef *col = pos->Pcr(ul);
        ULONG idx = outer_cols->IndexOf(col);

        unique_ptr<duckdb::Expression> order_expr =
            make_unique<duckdb::BoundReferenceExpression>(
                last_op_result_types[idx],
                plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp)->IndexOf(col));

        duckdb::OrderType order_type =
            IMDId::MDIdCompare(pos->GetMdIdSortOp(ul),
                               col->RetrieveType()->GetMdidForCmpType(
                                   IMDType::EcmptG)) == false
                ? duckdb::OrderType::ASCENDING
                : duckdb::OrderType::DESCENDING;

        duckdb::BoundOrderByNode order(
            order_type, pTranslateNullType(pos->Ent(ul)), move(order_expr));
        orders.push_back(move(order));
    }

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(last_op->GetTypes());
    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalSort(
        tmp_schema, move(orders));  // TODO we have topn sort only..
    result->push_back(op);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result);
    pipelines.push_back(pipeline);

    if (generate_sfg) {
        // Generate for the previous pipeline
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
        pGenerateSchemaFlowGraph(*result);

        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    return new_result;
}

vector<duckdb::CypherPhysicalOperator *> *Planner::pTransformEopTopNSort(
    CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    /* Non-root - call single child */
    D_ASSERT(plan_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalLimit);
    D_ASSERT(plan_expr->operator[](0)->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalLimit);
    D_ASSERT(plan_expr->operator[](0)->operator[](0)->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalSort);
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(
            plan_expr->operator[](0)->operator[](0)->PdrgPexpr()->operator[](
                0));

    // get limit info
    bool has_limit = false;
    CExpression *limit_expr = plan_expr;
    CPhysicalLimit *limit_op = (CPhysicalLimit *)limit_expr->Pop();
    D_ASSERT(limit_expr->operator[](1)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);
    D_ASSERT(limit_expr->operator[](2)->Pop()->Eopid() ==
             COperator::EOperatorId::EopScalarConst);

    int64_t offset, limit;
    if (!limit_op->FHasCount()) {
        has_limit = false;
    }
    else {
        has_limit = true;
        CDatumInt8GPDB *offset_datum =
            (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](1)
                                    ->Pop())
                                   ->GetDatum());
        CDatumInt8GPDB *limit_datum =
            (CDatumInt8GPDB *)(((CScalarConst *)limit_expr->operator[](2)
                                    ->Pop())
                                   ->GetDatum());
        offset = offset_datum->Value();
        limit = limit_datum->Value();
    }

    // currently, second limit has no count info
    D_ASSERT(!((CPhysicalLimit *)plan_expr->operator[](0)->Pop())->FHasCount());

    // get sort info
    CExpression *sort_expr = plan_expr->operator[](0)->operator[](0);
    CPhysicalSort *proj_op = (CPhysicalSort *)sort_expr->Pop();

    const COrderSpec *pos = proj_op->Pos();

    CColRefArray *output_cols = sort_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*sort_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    duckdb::CypherPhysicalOperator *last_op = result->back();
    auto &last_op_result_types = last_op->GetTypes();

    vector<duckdb::BoundOrderByNode> orders;
    for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++) {
        const CColRef *col = pos->Pcr(ul);
        ULONG idx = outer_cols->IndexOf(col);

        unique_ptr<duckdb::Expression> order_expr =
            make_unique<duckdb::BoundReferenceExpression>(
                last_op_result_types[idx],
                plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp)->IndexOf(col));

        duckdb::OrderType order_type =
            IMDId::MDIdCompare(pos->GetMdIdSortOp(ul),
                               col->RetrieveType()->GetMdidForCmpType(
                                   IMDType::EcmptG)) == false
                ? duckdb::OrderType::ASCENDING
                : duckdb::OrderType::DESCENDING;

        duckdb::BoundOrderByNode order(
            order_type, pTranslateNullType(pos->Ent(ul)), move(order_expr));
        orders.push_back(move(order));
    }

    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(last_op->GetTypes());
    duckdb::CypherPhysicalOperator *op;
    if (has_limit) {
        op = new duckdb::PhysicalTopNSort(tmp_schema, move(orders), limit,
                                          offset);
    }
    else {
        op = new duckdb::PhysicalSort(tmp_schema, move(orders));
    }

    result->push_back(op);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result);
    pipelines.push_back(pipeline);

    if (generate_sfg) {
        // Generate for the previous pipeline
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
        pGenerateSchemaFlowGraph(*result);

        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    return new_result;
}

bool Planner::pIsIndexJoinOnPhysicalID(CExpression *plan_expr)
{
    // if id seek then true, else adjidxjoin

    D_ASSERT(plan_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalInnerIndexNLJoin ||
             plan_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin);

    CPhysicalInnerIndexNLJoin *proj_op =
        (CPhysicalInnerIndexNLJoin *)plan_expr->Pop();
    CExpression *pexprInner = (*plan_expr)[1];

    CExpression *inner_root = pexprInner;
    uint64_t idx_obj_id;
    while (true) {
        if (inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexScan ||
            inner_root->Pop()->Eopid() ==
                COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
            // IdxScan
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            idx_obj_id = (uint64_t)oid;
        }
        // reached to the bottom
        if (inner_root->Arity() == 0) {
            break;
        }
        else {
            inner_root =
                inner_root->operator[](0);  // pass first child in linear plan
        }
    }
    D_ASSERT(inner_root != pexprInner);

    // search catalog
    duckdb::Catalog &cat_instance = context->db->GetCatalog();
    duckdb::IndexCatalogEntry *index_cat =
        (duckdb::IndexCatalogEntry *)cat_instance.GetEntry(
            *context, DEFAULT_SCHEMA, idx_obj_id);
    if (index_cat->GetIndexType() == duckdb::IndexType::PHYSICAL_ID) {
        return true;
    }
    else {
        return false;
    }
}

bool Planner::pMatchExprPattern(CExpression *root,
                                vector<COperator::EOperatorId> &pattern,
                                uint64_t pattern_root_idx,
                                bool physical_op_only)
{

    D_ASSERT(pattern.size() > 0);
    D_ASSERT(pattern_root_idx < pattern.size());

    // conjunctive checks
    bool match = true;

    // recursively iterate
    // if depth shorter than pattern, also return false.
    CExpressionArray *children = root->PdrgPexpr();
    const ULONG children_size = children->Size();

    // construct recursive child_pattern_match
    for (int i = 0; i < children_size; i++) {
        CExpression *child_expr = children->operator[](i);
        // check pattern for child
        if (physical_op_only && !child_expr->Pop()->FPhysical()) {
            // dont care other than phyiscal operator
            continue;
        }
        if (pattern_root_idx + 1 < pattern.size()) {
            // more patterns to check
            match = match &&
                    pMatchExprPattern(child_expr, pattern, pattern_root_idx + 1,
                                      physical_op_only);
        }
    }
    // check pattern for root
    match = match && root->Pop()->Eopid() == pattern[pattern_root_idx];

    return match;
}

bool Planner::pIsUnionAllOpAccessExpression(CExpression *expr)
{
    // FIXME
    auto p1 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalTableScan,
    });
    auto p2 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalIndexScan,
    });
    auto p3 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalIndexOnlyScan,
    });
    auto p4 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalFilter,
        COperator::EOperatorId::EopPhysicalTableScan,
    });
    auto p5 = vector<COperator::EOperatorId>({
        COperator::EOperatorId::EopPhysicalSerialUnionAll,
        COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
        COperator::EOperatorId::EopPhysicalFilter,
        COperator::EOperatorId::EopPhysicalIndexScan,
    });
    return pMatchExprPattern(expr, p1, 0, true) ||
           pMatchExprPattern(expr, p2, 0, true) ||
           pMatchExprPattern(expr, p3, 0, true) ||
           pMatchExprPattern(expr, p4, 0, true) ||
           pMatchExprPattern(expr, p5, 0, true);
}

uint64_t Planner::pGetColIdxFromTable(OID table_oid, const CColRef *target_col)
{
    CMemoryPool *mp = this->memory_pool;

    CColRefTable *colref_table = (CColRefTable *)target_col;
    INT attr_no = colref_table->AttrNum();
    if (attr_no == (INT)-1) {
        return 0;
    }
    else {
        return (uint64_t)attr_no;
    }
}

void Planner::pGenerateFilterExprs(
    CColRefArray *outer_cols, duckdb::ExpressionType &exp_type,
    CExpression *filter_pred_expr,
    vector<unique_ptr<duckdb::Expression>> &filter_exprs)
{
    if (filter_pred_expr->operator[](0)->Pop()->Eopid() ==
            COperator::EOperatorId::EopScalarIdent &&
        filter_pred_expr->operator[](1)->Pop()->Eopid() ==
            COperator::EOperatorId::EopScalarIdent) {
        // compare two columns
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id()));
        CColRefTable *rhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](1)->Pop())
                ->Pcr()
                ->Id()));

        gpos::ULONG lhs_pos, rhs_pos;
        lhs_pos = outer_cols->IndexOf((CColRef *)lhs_colref);
        rhs_pos = outer_cols->IndexOf((CColRef *)rhs_colref);
        D_ASSERT((lhs_pos != gpos::ulong_max) && (rhs_pos != gpos::ulong_max));

        duckdb::LogicalType lhs_type, rhs_type;
        lhs_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(
                outer_cols->operator[](lhs_pos)->RetrieveType()->MDId())
                ->Oid());
        rhs_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(
                outer_cols->operator[](rhs_pos)->RetrieveType()->MDId())
                ->Oid());

        unique_ptr<duckdb::Expression> filter_expr;
        filter_expr = make_unique<duckdb::BoundComparisonExpression>(
            exp_type,
            make_unique<duckdb::BoundReferenceExpression>(lhs_type, lhs_pos),
            make_unique<duckdb::BoundReferenceExpression>(rhs_type, rhs_pos));
        filter_exprs.push_back(move(filter_expr));
    }
    else if (filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarIdent &&
             filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarConst) {
        // compare left column to const val
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id()));

        gpos::ULONG lhs_pos;
        duckdb::Value literal_val;
        lhs_pos = outer_cols->IndexOf((CColRef *)lhs_colref);
        D_ASSERT(lhs_pos != gpos::ulong_max);

        duckdb::LogicalType lhs_type;
        lhs_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(
                outer_cols->operator[](lhs_pos)->RetrieveType()->MDId())
                ->Oid());

        CDatumGenericGPDB *datum =
            (CDatumGenericGPDB
                 *)(((CScalarConst *)filter_pred_expr->operator[](1)->Pop())
                        ->GetDatum());
        literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
            CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
            datum->GetByteArrayValue(), (uint64_t)datum->Size());

        unique_ptr<duckdb::Expression> filter_expr;
        filter_expr = make_unique<duckdb::BoundComparisonExpression>(
            exp_type,
            make_unique<duckdb::BoundReferenceExpression>(lhs_type, lhs_pos),
            make_unique<duckdb::BoundConstantExpression>(literal_val));
        filter_exprs.push_back(move(filter_expr));
    }
    else if (filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarConst &&
             filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarIdent) {
        // compare right column to const val
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRefTable *rhs_colref = (CColRefTable *)(col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](1)->Pop())
                ->Pcr()
                ->Id()));

        gpos::ULONG rhs_pos;
        duckdb::Value literal_val;
        rhs_pos = outer_cols->IndexOf((CColRef *)rhs_colref);
        D_ASSERT(rhs_pos != gpos::ulong_max);

        duckdb::LogicalType rhs_type;
        rhs_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(
                outer_cols->operator[](rhs_pos)->RetrieveType()->MDId())
                ->Oid());

        CDatumGenericGPDB *datum =
            (CDatumGenericGPDB
                 *)(((CScalarConst *)filter_pred_expr->operator[](0)->Pop())
                        ->GetDatum());
        literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
            CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
            datum->GetByteArrayValue(), (uint64_t)datum->Size());

        unique_ptr<duckdb::Expression> filter_expr;
        filter_expr = make_unique<duckdb::BoundComparisonExpression>(
            exp_type, make_unique<duckdb::BoundConstantExpression>(literal_val),
            make_unique<duckdb::BoundReferenceExpression>(rhs_type, rhs_pos));
        filter_exprs.push_back(move(filter_expr));
    }
    else {
        // not implemented yet
        throw duckdb::NotImplementedException("");
        D_ASSERT(false);
    }
}

void Planner::pGenerateScanMapping(OID table_oid, CColRefArray *columns,
                                   vector<uint64_t> &out_mapping)
{
    columns->AddRef();
    D_ASSERT(out_mapping.size() == 0);  // assert empty

    for (uint64_t i = 0; i < columns->Size(); i++) {
        auto table_col_idx =
            pGetColIdxFromTable(table_oid, columns->operator[](i));
        out_mapping.push_back(table_col_idx);
    }
}

void Planner::pGenerateTypes(CColRefArray *columns,
                             vector<duckdb::LogicalType> &out_types)
{
    columns->AddRef();
    D_ASSERT(out_types.size() == 0);  // assert empty
    for (uint64_t i = 0; i < columns->Size(); i++) {
        CMDIdGPDB *type_mdid =
            CMDIdGPDB::CastMdid(columns->operator[](i)->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        out_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }
}

void Planner::pGenerateColumnNames(CColRefArray *columns,
                                   vector<string> &out_col_names)
{
    columns->AddRef();
    D_ASSERT(out_col_names.size() == 0);
    for (uint64_t i = 0; i < columns->Size(); i++) {
        out_col_names.push_back(pGetColNameFromColRef(columns->operator[](i)));
    }
}

void Planner::pGenerateSchemaFlowGraph(
    vector<duckdb::CypherPhysicalOperator *> &final_pipeline_ops)
{
    duckdb::SchemaFlowGraph sfg(final_pipeline_ops.size(),
                                pipeline_operator_types, num_schemas_of_childs,
                                pipeline_schemas, pipeline_union_schema);
    auto &num_schemas_of_childs_ = sfg.GetNumSchemasOfChilds();
    vector<vector<uint64_t>> flow_graph;
    flow_graph.resize(final_pipeline_ops.size());
    for (auto i = 0; i < flow_graph.size(); i++) {
        uint64_t num_total_child_schemas = 1;
        for (auto j = 0; j < num_schemas_of_childs_[i].size(); j++) {
            num_total_child_schemas *= num_schemas_of_childs_[i][j];
        }
        flow_graph[i].resize(num_total_child_schemas);
        std::cout << "lv " << i << " : " << num_total_child_schemas
                  << std::endl;
        for (auto j = 0; j < flow_graph[i].size(); j++) {
            flow_graph[i][j] = j;  // TODO
        }
    }

    sfg.SetFlowGraph(flow_graph);
    sfgs.push_back(std::move(sfg));
}

void Planner::pClearSchemaFlowGraph()
{
    pipeline_operator_types.clear();
    num_schemas_of_childs.clear();
    pipeline_schemas.clear();
    pipeline_union_schema.clear();
}

void Planner::pInitializeSchemaFlowGraph()
{
    pipeline_operator_types.clear();
    num_schemas_of_childs.clear();
    pipeline_schemas.clear();
    pipeline_union_schema.clear();
    sfgs.clear();
    generate_sfg = false;
}

void Planner::pGenerateMappingInfo(vector<duckdb::idx_t> &scan_cols_id,
                                   duckdb::PropertyKeyID_vector *key_ids,
                                   vector<duckdb::LogicalType> &global_types,
                                   vector<duckdb::LogicalType> &local_types,
                                   vector<uint64_t> &projection_mapping,
                                   vector<uint64_t> &scan_projection_mapping)
{
    D_ASSERT(scan_cols_id.size() == global_types.size());
    duckdb::idx_t i = 0, j = 0, output_idx = 0;
    size_t i_max = scan_cols_id.size();
    size_t j_max = key_ids->size();
    while (i < i_max && j < j_max) {
        if (scan_cols_id[i] == (*key_ids)[j]) {
            projection_mapping.push_back(output_idx);
            scan_projection_mapping.push_back(j);
            local_types.push_back(global_types[i]);

            i++;
            j++;
            output_idx++;
        }
        else if (scan_cols_id[i] > (*key_ids)[j]) {
            j++;
        }
        else {  // scan_cols_id[i] < (*key_ids)[j]
            i++;
        }
    }
}

bool Planner::pIsColumnarProjectionSimpleProject(CExpression *proj_expr)
{
    // check if all projetion expressions are CscalarIdent
    D_ASSERT(proj_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
    D_ASSERT(proj_expr->Arity() == 2);  // 0 prev 1 projlist

    bool result = true;
    ULONG proj_size = proj_expr->operator[](1)->Arity();
    for (ULONG proj_idx = 0; proj_idx < proj_size; proj_idx++) {
        result = result &&
                 // list -> idx'th element -> ident
                 (proj_expr->operator[](1)
                      ->
                      operator[](proj_idx)
                      ->
                      operator[](0)
                      ->Pop()
                      ->Eopid() == COperator::EOperatorId::EopScalarIdent);
    }
    return result;
}

void Planner::pTranslatePredicateToJoinCondition(
    CExpression *pred, vector<duckdb::JoinCondition> &out_conds,
    CColRefArray *lhs_cols, CColRefArray *rhs_cols)
{

    // split AND predicates into each JoinCondition
    // TODO what about OR condition in duckdb ?? -> IDK
    auto *op = pred->Pop();
    if (op->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)op;
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            // Split predicates
            pTranslatePredicateToJoinCondition(pred->operator[](0), out_conds,
                                               lhs_cols, rhs_cols);
            pTranslatePredicateToJoinCondition(pred->operator[](1), out_conds,
                                               lhs_cols, rhs_cols);
        }
        else if (boolop->Eboolop() ==
                 CScalarBoolOp::EBoolOperator::EboolopNot) {
            // NOT + EQUALS
            D_ASSERT(
                pred->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarCmp &&
                ((CScalarCmp *)(pred->operator[](0)->Pop()))->ParseCmpType() ==
                    IMDType::ECmpType::EcmptEq);
            duckdb::JoinCondition cond;
            unique_ptr<duckdb::Expression> lhs = pTransformScalarExpr(
                pred->operator[](0)->operator[](0), lhs_cols, rhs_cols);
            unique_ptr<duckdb::Expression> rhs = pTransformScalarExpr(
                pred->operator[](0)->operator[](1), lhs_cols, rhs_cols);
            if (lhs->return_type != rhs->return_type) {
                rhs = pGenScalarCast(move(rhs), lhs->return_type);
            }
            cond.left = move(lhs);
            cond.right = move(rhs);
            cond.comparison = duckdb::ExpressionType::COMPARE_NOTEQUAL;
            out_conds.push_back(move(cond));
        }
        else {
            D_ASSERT(false);
        }
    }
    else if (op->Eopid() == COperator::EOperatorId::EopScalarCmp) {
        CScalarCmp *cmpop = (CScalarCmp *)op;
        duckdb::JoinCondition cond;
        unique_ptr<duckdb::Expression> lhs =
            pTransformScalarExpr(pred->operator[](0), lhs_cols, rhs_cols);
        unique_ptr<duckdb::Expression> rhs =
            pTransformScalarExpr(pred->operator[](1), lhs_cols, rhs_cols);
        if (lhs->return_type != rhs->return_type) {
            rhs = pGenScalarCast(move(rhs), lhs->return_type);
        }
        cond.left = move(lhs);
        cond.right = move(rhs);
        if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptEq) {
            // EQUALS
            cond.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
        }
        else if (cmpop->ParseCmpType() == IMDType::ECmpType::EcmptNEq) {
            // NOT EQUALS
            cond.comparison = duckdb::ExpressionType::COMPARE_NOTEQUAL;
        }
        out_conds.push_back(move(cond));
    }
    else {
        D_ASSERT(false);
    }
    return;
}

bool Planner::pIsCartesianProduct(CExpression *expr)
{
    return expr->Pop()->Eopid() ==
               COperator::EOperatorId::EopPhysicalInnerNLJoin &&
           expr->operator[](2)->Pop()->Eopid() ==
               COperator::EOperatorId::EopScalarConst &&
           CUtils::FScalarConstTrue(expr->operator[](2));
}

bool Planner::pIsFilterPushdownAbleIntoScan(CExpression* selection_expr) {
	
	D_ASSERT( selection_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalFilter );
	CExpression* filter_expr = NULL;
	CExpression* filter_pred_expr = NULL;
	filter_expr = selection_expr;
	filter_pred_expr = filter_expr->operator[](1);

	auto ok = 
		filter_pred_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp
		&& (((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptEq ||
			((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptNEq ||
			((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptL ||
			((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptLEq ||
			((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptG ||
			((CScalarCmp*)(filter_pred_expr->Pop()))->ParseCmpType() == IMDType::ECmpType::EcmptGEq
		)
		&& filter_pred_expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent
		&& filter_pred_expr->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarConst;
	
	return ok;
}

duckdb::OrderByNullType Planner::pTranslateNullType(COrderSpec::ENullTreatment ent) {
	switch (ent) {
	case COrderSpec::ENullTreatment::EntAuto:
		return duckdb::OrderByNullType::ORDER_DEFAULT;
	case COrderSpec::ENullTreatment::EntFirst:
		return duckdb::OrderByNullType::NULLS_FIRST;
	case COrderSpec::ENullTreatment::EntLast:
		return duckdb::OrderByNullType::NULLS_LAST;
	case COrderSpec::ENullTreatment::EntSentinel:
		D_ASSERT(false);
	}
	return duckdb::OrderByNullType::ORDER_DEFAULT;
}

duckdb::JoinType Planner::pTranslateJoinType(COperator *op)
{

    switch (op->Eopid()) {
        case COperator::EOperatorId::EopPhysicalInnerNLJoin:
        case COperator::EOperatorId::EopPhysicalInnerIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalInnerHashJoin:
        case COperator::EOperatorId::EopPhysicalInnerMergeJoin: {
            return duckdb::JoinType::INNER;
        }
        case COperator::EOperatorId::EopPhysicalLeftOuterNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterIndexNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftOuterHashJoin: {
            return duckdb::JoinType::LEFT;
        }
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin:
        case COperator::EOperatorId::
            EopPhysicalCorrelatedLeftAntiSemiNLJoin: {  // correct?
            return duckdb::JoinType::ANTI;
        }
        case COperator::EOperatorId::EopPhysicalLeftSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin:
        case COperator::EOperatorId::
            EopPhysicalCorrelatedLeftSemiNLJoin: {  // correct?
            return duckdb::JoinType::SEMI;
        }
            // TODO where is FULL OUTER??????
    }
    D_ASSERT(false);
}

void Planner::pGetFilterAttrPosAndValue(CExpression *filter_pred_expr,
                                        gpos::ULONG &attr_pos,
                                        duckdb::Value &attr_value)
{
    CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
    CColRefTable *lhs_colref = (CColRefTable *)(col_factory->LookupColRef(
        ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())->Pcr()->Id()));
    gpos::INT lhs_attrnum = lhs_colref->AttrNum();
    attr_pos = lGetMDAccessor()
                   ->RetrieveRel(lhs_colref->GetMdidTable())
                   ->GetPosFromAttno(lhs_attrnum);
    CDatumGenericGPDB *datum =
        (CDatumGenericGPDB *)(((CScalarConst *)filter_pred_expr->operator[](1)
                                   ->Pop())
                                  ->GetDatum());
    attr_value = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
        CMDIdGPDB::CastMdid(datum->MDId())->Oid(), datum->GetByteArrayValue(),
        (uint64_t)datum->Size());
}

void Planner::pConvertLocalFilterExprToUnionAllFilterExpr(
    unique_ptr<duckdb::Expression> &expr, CColRefArray *cols,
    vector<ULONG> unionall_output_original_col_ids)
{
    switch (expr->expression_class) {
        case duckdb::ExpressionClass::BOUND_BETWEEN: {
            auto between_expr = (duckdb::BoundBetweenExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->input, cols, unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->lower, cols, unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                between_expr->upper, cols, unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_REF: {
            auto bound_ref_expr =
                (duckdb::BoundReferenceExpression *)expr.get();
            CColRef *col = cols->operator[](bound_ref_expr->index);
            for (uint64_t i = 0; i < unionall_output_original_col_ids.size();
                 i++) {
                if (unionall_output_original_col_ids[i] == col->Id()) {
                    bound_ref_expr->index = i;
                    break;
                }
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CASE: {
            auto bound_case_expr = (duckdb::BoundCaseExpression *)expr.get();
            for (auto &bound_case_check : bound_case_expr->case_checks) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    bound_case_check.when_expr, cols,
                    unionall_output_original_col_ids);
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    bound_case_check.then_expr, cols,
                    unionall_output_original_col_ids);
            }
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_case_expr->else_expr, cols,
                unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CAST: {
            auto bound_cast_expr = (duckdb::BoundCastExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_cast_expr->child, cols, unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_COMPARISON: {
            auto bound_comparison_expr =
                (duckdb::BoundComparisonExpression *)expr.get();
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_comparison_expr->left, cols,
                unionall_output_original_col_ids);
            pConvertLocalFilterExprToUnionAllFilterExpr(
                bound_comparison_expr->right, cols,
                unionall_output_original_col_ids);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
            auto bound_conjunction_expr =
                (duckdb::BoundConjunctionExpression *)expr.get();
            for (auto &child : bound_conjunction_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONSTANT:
            break;
        case duckdb::ExpressionClass::BOUND_FUNCTION: {
            auto bound_function_expr =
                (duckdb::BoundFunctionExpression *)expr.get();
            for (auto &child : bound_function_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_OPERATOR: {
            auto bound_operator_expr =
                (duckdb::BoundOperatorExpression *)expr.get();
            for (auto &child : bound_operator_expr->children) {
                pConvertLocalFilterExprToUnionAllFilterExpr(
                    child, cols, unionall_output_original_col_ids);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_PARAMETER:
            break;
        default:
            throw InternalException(
                "Attempting to execute expression of unknown type!");
    }
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pBuildSchemaflowGraphForBinaryJoin(CExpression *plan_expr,
                                            duckdb::CypherPhysicalOperator *op,
                                            duckdb::Schema &output_schema)
{
    /**
	 * Join is a binary operator, which needs two pipelines.
	 * If we need to generate schema flow graph, we need to create pipeline one-by-one.
	 * We first generate rhs pipeline and schema flow graph.
	 * We then clear the schema flow graph data structures.
	 * We finally generate lhs pipeline
	*/
    // Step 1. rhs pipline
    vector<duckdb::CypherPhysicalOperator *> *rhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](1));
    rhs_result->push_back(op);
    auto pipeline = new duckdb::CypherPipeline(*rhs_result);
    pipelines.push_back(pipeline);

    // Step 1. schema flow graph
    size_t rhs_num_schemas = 0;
    vector<duckdb::Schema> rhs_schemas;  // We need to change this.
    if (generate_sfg) {
        // Store previous schema flow graph data structures for lhs build
        rhs_num_schemas = pipeline_schemas.back().size();
        rhs_schemas = pipeline_schemas.back();
        // Generate rhs schema flow graph
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        duckdb::Schema prev_union_schema = pipeline_union_schema.back();
        rhs_num_schemas = prev_local_schemas.size();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(prev_union_schema);
        pGenerateSchemaFlowGraph(*rhs_result);
        pClearSchemaFlowGraph();  // Step 2
    }

    // Step 3. lhs pipeline
    vector<duckdb::CypherPhysicalOperator *> *lhs_result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
    lhs_result->push_back(op);

    // Step 3. schema flow graph
    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        num_schemas_of_childs.push_back(
            {prev_local_schemas.size(), rhs_num_schemas});
        // Generate cartensian prouct of schemas (prev_local_schema x rhs_schemas)
        vector<duckdb::Schema> lhs_schemas;
        for (auto &prev_local_schema : prev_local_schemas) {
            for (auto &rhs_schema : rhs_schemas) {
                duckdb::Schema tmp_schema;
                tmp_schema.setStoredTypes(prev_local_schema.getStoredTypes());
                tmp_schema.appendStoredTypes(rhs_schema.getStoredTypes());
                lhs_schemas.push_back(tmp_schema);
            }
        }
        pipeline_schemas.push_back(lhs_schemas);
        pipeline_union_schema.push_back(output_schema);
    }

    return lhs_result;
}

void Planner::pGetColumnsDuckDBType(CColRefArray *columns,
                                    vector<duckdb::LogicalType> &output_types)
{
    for (ULONG col_idx = 0; col_idx < columns->Size(); col_idx++) {
        CColRef *col = (*columns)[col_idx];

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        output_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    }
}

void Planner::pGetProjectionExprs(
    vector<duckdb::LogicalType> output_types, vector<duckdb::idx_t> &ref_idxs,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    for (auto idx : ref_idxs) {
        out_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            output_types[idx], idx));
    }
}

void Planner::pRemoveColumnsFromSchemas(vector<duckdb::Schema> &in_schemas,
                                        vector<duckdb::idx_t> &ref_idxs,
                                        vector<duckdb::Schema> &out_schemas)
{
    for (auto &schema : in_schemas) {
        duckdb::Schema new_schema;
        auto stored_types = schema.getStoredTypes();
        for (auto idx : ref_idxs) {
            new_schema.appendStoredTypes({stored_types[idx]});
        }
        out_schemas.push_back(new_schema);
    }
}

}  // namespace s62
