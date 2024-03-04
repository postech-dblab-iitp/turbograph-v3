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
    duckdb::CypherPhysicalOperator::operator_version = 0;
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
        projection_mappings.push_back(std::vector<uint8_t>());
        for (uint8_t log_idx = 0; log_idx < logical_plan_output_colrefs.size();
             log_idx++) {
            for (uint8_t phy_idx = 0;
                 phy_idx < physical_plan_output_colrefs.size(); phy_idx++) {
                if (logical_plan_output_colrefs[log_idx]->Id() ==
                    physical_plan_output_colrefs[phy_idx]->Id()) {
                    projection_mappings[0].push_back(phy_idx);
                }
            }
        }
        op = new duckdb::PhysicalProduceResults(final_output_schema,
                                                projection_mappings);
    }

    final_pipeline_ops.push_back(op);
    D_ASSERT(final_pipeline_ops.size() > 0);

    pBuildSchemaFlowGraphForUnaryOperator(final_output_schema);
    pGenerateSchemaFlowGraph(final_pipeline_ops);

    auto final_pipeline =
        new duckdb::CypherPipeline(final_pipeline_ops, pipelines.size());

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
        case COperator::EOperatorId::EopPhysicalLeftOuterHashJoin: {
            result = pTransformEopPhysicalHashJoinToHashJoin(plan_expr);
            break;
        }
        case COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin: {
            result = 
                pTransformEopPhysicalNLJoinToBlockwiseNLJoin(plan_expr, false);
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
                    GPOS_ASSERT(false);
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
            auto topnsort_p = vector<COperator::EOperatorId>(
                {COperator::EOperatorId::EopPhysicalLimit,
                 COperator::EOperatorId::EopPhysicalLimit,
                 COperator::EOperatorId::EopPhysicalSort});
            if (pMatchExprPattern(plan_expr, topnsort_p, 0, true)) {
                result = pTransformEopTopNSort(plan_expr);
            }
            else {
                result = pTransformEopLimit(plan_expr);
            }
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
            ULONG col_id = scan_cols->operator[](i)->Id();
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
    generate_sfg = true;

    if (!do_filter_pushdown) {
        op = new duckdb::PhysicalNodeScan(tmp_schema, oids,
                                          output_projection_mapping,
                                          scan_projection_mapping);
    }
    else {
        if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            IMDType::ECmpType::EcmptEq) {
            op = new duckdb::PhysicalNodeScan(
                tmp_schema, oids, output_projection_mapping, scan_types,
                scan_projection_mapping, pred_attr_pos, literal_val);
        }
        else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                 IMDType::ECmpType::EcmptL) {
            op = new duckdb::PhysicalNodeScan(
                tmp_schema, oids, output_projection_mapping, scan_types,
                scan_projection_mapping, pred_attr_pos,
                duckdb::Value::MinimumValue(literal_val.type()), literal_val,
                true, false);
        }
        else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                 IMDType::ECmpType::EcmptLEq) {
            op = new duckdb::PhysicalNodeScan(
                tmp_schema, oids, output_projection_mapping, scan_types,
                scan_projection_mapping, pred_attr_pos,
                duckdb::Value::MinimumValue(literal_val.type()), literal_val,
                true, true);
        }
        else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                 IMDType::ECmpType::EcmptG) {
            op = new duckdb::PhysicalNodeScan(
                tmp_schema, oids, output_projection_mapping, scan_types,
                scan_projection_mapping, pred_attr_pos, literal_val,
                duckdb::Value::MaximumValue(literal_val.type()), false, true);
        }
        else if (((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                 IMDType::ECmpType::EcmptGEq) {
            op = new duckdb::PhysicalNodeScan(
                tmp_schema, oids, output_projection_mapping, scan_types,
                scan_projection_mapping, pred_attr_pos, literal_val,
                duckdb::Value::MaximumValue(literal_val.type()), true, true);
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
                INT type_mod = ident_op->Pcr()->TypeModifier();
                auto duckdb_type =
                    pConvertTypeOidToLogicalType(type_oid, type_mod);
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
            auto cmp_type =
                ((CScalarCmp *)(repr_filter_pred_expr->Pop()))->ParseCmpType();
            auto num_vals = literal_vals.size();
            switch (cmp_type) {
                case IMDType::ECmpType::EcmptEq:
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss, literal_vals);
                    break;
                case IMDType::ECmpType::EcmptL:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {duckdb::Value::MinimumValue(
                                 literal_vals[i].type()),
                             literal_vals[i], true, false});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptLEq:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {duckdb::Value::MinimumValue(
                                 literal_vals[i].type()),
                             literal_vals[i], true, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptG:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {literal_vals[i],
                             duckdb::Value::MaximumValue(
                                 literal_vals[i].type()),
                             false, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
                    break;
                case IMDType::ECmpType::EcmptGEq:
                    for (int i = 0; i < num_vals; i++)
                        range_filter_values.push_back(
                            {literal_vals[i],
                             duckdb::Value::MaximumValue(
                                 literal_vals[i].type()),
                             true, true});
                    op = new duckdb::PhysicalNodeScan(
                        local_schemas, global_schema, oids, projection_mapping,
                        scan_projection_mapping, pred_attr_poss,
                        range_filter_values);
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
            pGetColumnsDuckDBType(unionall_output_cols, proj_op_output_types);
            proj_op_output_union_schema.setStoredTypes(proj_op_output_types);

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
    /**
     * If edge proeprty in output, create IdSeek operator.
     * In this case, AdjIdxJoin only output non-edge property columns, including edge ID column.
     * Then, IdSeek will output edge property columns, removing edge ID column, if necessary.
    */

    CMemoryPool *mp = this->memory_pool;
    vector<duckdb::CypherPhysicalOperator *> *result =
        pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

    // ORCA data structures
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *outer_cols =
        (*plan_expr)[0]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *inner_cols =
        (*plan_expr)[1]->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *adj_inner_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *adj_output_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *seek_inner_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *seek_output_cols = output_cols;
    CColRefArray *idxscan_pred_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *idxscan_cols = NULL;
    CExpression *filter_expr = NULL;

    // DuckDB data structures
    size_t num_outer_schemas = pGetNumOuterSchemas();
    duckdb::idx_t outer_join_key_col_idx;
    duckdb::idx_t edge_id_col_idx;
    uint64_t adjidx_obj_id;
    vector<uint64_t> seek_obj_ids;
    vector<vector<uint32_t>> inner_col_maps_adj(1);
    vector<vector<uint32_t>> outer_col_maps_adj(1);
    vector<vector<uint32_t>> inner_col_maps_seek(1);
    vector<vector<uint32_t>> outer_col_maps_seek(1);
    vector<duckdb::LogicalType> output_types_adj;
    vector<duckdb::LogicalType> output_types_seek;
    vector<vector<uint64_t>> scan_projection_mappings_seek(1);
    vector<vector<uint64_t>> output_projection_mappings_seek(1);
    vector<vector<duckdb::LogicalType>> scan_types_seek(1);
    unique_ptr<duckdb::Expression> filter_duckdb_expr;
    duckdb::Schema schema_adj;
    duckdb::Schema schema_seek;
    duckdb::Schema schema_proj;
    size_t ID_COL_SIZE = 1;

    // Flags
    bool is_edge_prop_in_output = pIsPropertyInCols(inner_cols);
    bool is_edge_id_in_inner_cols = pIsIDColInCols(inner_cols);
    bool is_filter_exist = pIsFilterExist(plan_expr->operator[](1));
    bool filter_after_adj =
        is_filter_exist && !pIsEdgePropertyInFilter(plan_expr->operator[](1));
    bool filter_in_seek = is_filter_exist && !filter_after_adj;
    bool generate_seek = is_edge_prop_in_output || filter_in_seek;

    // Get filter
    if (is_filter_exist)
        filter_expr = pFindFilterExpr(plan_expr->operator[](1));

    // Calculate join key columns index
    auto idxscan_epxr = pFindIndexScanExpr(plan_expr->operator[](1));
    D_ASSERT(idxscan_epxr != NULL);
    idxscan_cols = idxscan_epxr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    outer_join_key_col_idx =
        pGetColIndexInPred(idxscan_epxr->operator[](0), outer_cols);
    D_ASSERT(outer_join_key_col_idx != gpos::ulong_max);

    // Calculate adj_inner_cols and adj_output_cols and seek_inner_cols
    /**
     * 1) AdjIdxJoin + Filter + Seek
     *  - AdjIdxJoin output: outer_cols + inner_cols (w/o edge properties) + filter only columns + edge ID
     *  - Filter output: outer_cols + inner_cols (w/o edge properties) + filter only columns
     *  - Seek inner: edge properties
     *  - Seek output: output_cols
     * 2) AdjIdxJoin + Filter + [Projection]
     *  - AdjIdxJoin inner: inner_cols + filter only columns
     *  - AdjIdxJoin output: outer_cols + inner_cols + filter only columns
     *  - Filter output: output_cols
     * 3) AdjIdxJoin + Seek
     *  - AdjIdxJoin inner: inner_cols (w/o edge properties)
     *  - AdjIdxJoin output: outer_cols + inner_cols (w/o edge properties) + edge ID
     *  - Seek inner: edge properties + filter only columns (if filter exists)
     *  - Seek output: output_cols
     * 4) AdjIdxJoin
     *  - AdjIdxJoin inner: inner cols
     *  - AdjIdxJoin output: output cols
    */
    if (filter_after_adj && generate_seek) {
        D_ASSERT(filter_expr != NULL);
        adj_output_cols->AppendArray(outer_cols);
        pSeperatePropertyNonPropertyCols(inner_cols, seek_inner_cols,
                                         adj_inner_cols);
        pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                              adj_inner_cols);
        pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                              adj_output_cols);
    }
    else if (filter_after_adj && !generate_seek) {
        D_ASSERT(filter_expr != NULL);
        adj_output_cols->AppendArray(outer_cols);
        adj_inner_cols->AppendArray(inner_cols);
        pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                              adj_inner_cols);
        pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                              adj_output_cols);
    }
    else if (!filter_after_adj && generate_seek) {
        adj_output_cols->AppendArray(outer_cols);
        pSeperatePropertyNonPropertyCols(inner_cols, seek_inner_cols,
                                         adj_inner_cols);
        adj_output_cols->AppendArray(adj_inner_cols);
        if (filter_in_seek) {
            D_ASSERT(filter_expr != NULL);
            pAppendFilterOnlyCols(filter_expr, idxscan_cols, inner_cols,
                                  seek_inner_cols);
        }
    }
    else {
        adj_output_cols->AppendArray(output_cols);
        adj_inner_cols->AppendArray(inner_cols);
    }

    D_ASSERT(adj_output_cols->Size() > 0);
    D_ASSERT(adj_inner_cols->Size() > 0);

    // Construct inner_col_maps_adj
    if (!generate_seek) {
        pConstructColMapping(adj_inner_cols, adj_output_cols,
                             inner_col_maps_adj[0]);
    }
    else {
        for (ULONG col_idx = 0; col_idx < adj_inner_cols->Size(); col_idx++) {
            // inner cols appended to outer cols
            inner_col_maps_adj[0].push_back(outer_cols->Size() + col_idx);
        }
        if (!is_edge_id_in_inner_cols) {
            // edge ID column for seek
            inner_col_maps_adj[0].push_back(outer_cols->Size() +
                                            adj_inner_cols->Size());
            edge_id_col_idx = outer_cols->Size() + adj_inner_cols->Size();
        }
        else {
            auto edge_id_col_ref = pGetIDColInCols(inner_cols);
            D_ASSERT(edge_id_col_ref != NULL);
            edge_id_col_idx = adj_output_cols->IndexOf(edge_id_col_ref);
        }
    }
    D_ASSERT(inner_col_maps_adj[0].size() > 0);

    // Construct outer_cols_maps_adj
    pConstructColMapping(outer_cols, adj_output_cols, outer_col_maps_adj[0]);

    // Construct AdjIdxJoin schema
    pGetDuckDBTypesFromColRefs(adj_output_cols, output_types_adj);
    if (generate_seek && !is_edge_id_in_inner_cols) {  // Append ID Column
        output_types_adj.push_back(duckdb::LogicalType::ID);
    }
    schema_adj.setStoredTypes(output_types_adj);

    // Construct adjidx_obj_id
    CPhysicalIndexScan *idxscan_op = (CPhysicalIndexScan *)idxscan_epxr->Pop();
    CMDIdGPDB *index_mdid =
        CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
    adjidx_obj_id = index_mdid->Oid();

    // Construct adjacency index
    bool load_eid = generate_seek || is_edge_id_in_inner_cols;
    bool load_eid_temp = load_eid && !is_edge_id_in_inner_cols;
    duckdb::CypherPhysicalOperator *duckdb_adjidx_op =
        new duckdb::PhysicalAdjIdxJoin(
            schema_adj, adjidx_obj_id,
            is_left_outer ? duckdb::JoinType::LEFT : duckdb::JoinType::INNER,
            outer_join_key_col_idx, load_eid, outer_col_maps_adj[0],
            inner_col_maps_adj[0], false /* do filter pushdown */,
            0 /* unused outer_pos */, 0 /* unused inner_pos */, load_eid_temp);

    /**
     * TOOD: this code assumes that the edge table is single schema.
     * Extend this code to handle multiple schemas.
     * 
     * TODO: is pipeline_schemas necessary?
     * In the current logic, we intialize the chunk with UNION schema
     * and use col map to invalid each vector.
     * Therefore, pipeline schema is not actually used.
    */
    pBuildSchemaFlowGraphForUnaryOperator(schema_adj);
    result->push_back(duckdb_adjidx_op);

    // System col only filter will be processed after adj
    if (filter_after_adj) {
        D_ASSERT(!filter_in_seek);
        vector<unique_ptr<duckdb::Expression>> filter_duckdb_exprs;
        pGetFilterDuckDBExprs(filter_expr, adj_output_cols, nullptr,
                              adj_output_cols->Size(), filter_duckdb_exprs);
        duckdb::CypherPhysicalOperator *duckdb_filter_op =
            new duckdb::PhysicalFilter(schema_adj, move(filter_duckdb_exprs));
        result->push_back(duckdb_filter_op);
        pBuildSchemaFlowGraphForUnaryOperator(schema_adj);

        // Construct projection
        if (!generate_seek) {
            if (output_cols->Size() != adj_output_cols->Size()) {
                vector<duckdb::LogicalType> output_types_proj;
                vector<unique_ptr<duckdb::Expression>> proj_exprs;
                pGetDuckDBTypesFromColRefs(output_cols, output_types_proj);
                schema_proj.setStoredTypes(output_types_proj);
                pGetProjectionExprs(adj_output_cols, output_cols,
                                    output_types_proj, proj_exprs);
                if (proj_exprs.size() != 0) {
                    duckdb::CypherPhysicalOperator *duckdb_proj_op =
                        new duckdb::PhysicalProjection(schema_proj,
                                                       move(proj_exprs));
                    result->push_back(duckdb_proj_op);
                    pBuildSchemaFlowGraphForUnaryOperator(schema_proj);
                }
            }
            return result;
        }
    }

    // Construct IdSeek
    if (generate_seek) {
        // Construct inner col map
        pConstructColMapping(seek_inner_cols, seek_output_cols,
                             inner_col_maps_seek[0]);
        D_ASSERT(inner_col_maps_seek[0].size() > 0);

        // Construct union_inner_col_map_seek
        vector<uint32_t> union_inner_col_map_seek;
        union_inner_col_map_seek = inner_col_maps_seek[0];

        // Construct outer col map
        pConstructColMapping(adj_output_cols, seek_output_cols,
                             outer_col_maps_seek[0]);
        if (!is_edge_id_in_inner_cols) {
            outer_col_maps_seek[0].push_back(
                std::numeric_limits<uint32_t>::max());
        }  // Remove ID COlumn
        D_ASSERT(outer_col_maps_seek[0].size() > 0);

        // Construct scan_projection_mappings_seek and output_projection_mappings_seek and scan_types
        for (ULONG col_idx = 0; col_idx < seek_inner_cols->Size(); col_idx++) {
            CColRef *col = seek_inner_cols->operator[](col_idx);
            CColRefTable *colref_table = (CColRefTable *)col;
            CMDIdGPDB *type_mdid =
                CMDIdGPDB::CastMdid(colref_table->RetrieveType()->MDId());
            scan_projection_mappings_seek[0].push_back(colref_table->AttrNum());
            output_projection_mappings_seek[0].push_back(
                col_idx);  // not actually used
            scan_types_seek[0].push_back(pConvertTypeOidToLogicalType(
                type_mdid->Oid(), col->TypeModifier()));
        }

        // Get OIDs
        CColRefArray *idxscan_output_cols =
            idxscan_epxr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
        pGetObjetIdsForColRefs(idxscan_output_cols, seek_obj_ids);

        // Construct seek scheam
        pGetDuckDBTypesFromColRefs(seek_output_cols, output_types_seek);
        schema_seek.setStoredTypes(output_types_seek);

        // Construct IdSeek Operator
        duckdb::CypherPhysicalOperator *duckdb_idseek_op;
        if (!filter_in_seek) {
            duckdb_idseek_op = new duckdb::PhysicalIdSeek(
                schema_seek, edge_id_col_idx, seek_obj_ids,
                output_projection_mappings_seek /* not used */,
                outer_col_maps_seek, inner_col_maps_seek,
                union_inner_col_map_seek, scan_projection_mappings_seek,
                scan_types_seek, false /* is output UNION Schema */);
        }
        else {
            // Get filter_exprs
            vector<unique_ptr<duckdb::Expression>> filter_duckdb_exprs;
            pGetFilterDuckDBExprs(filter_expr, adj_output_cols, seek_inner_cols,
                                  adj_output_cols->Size() + ID_COL_SIZE,
                                  filter_duckdb_exprs);
            // Construct IdSeek Operator for filter
            duckdb_idseek_op = new duckdb::PhysicalIdSeek(
                schema_seek, edge_id_col_idx, seek_obj_ids,
                output_projection_mappings_seek /* not used */,
                outer_col_maps_seek, inner_col_maps_seek,
                union_inner_col_map_seek, scan_projection_mappings_seek,
                scan_types_seek, filter_duckdb_exprs,
                false /* is output UNION Schema */);
        }

        // Construct schema flow graph
        pPushCartesianProductSchema(schema_seek, scan_types_seek[0]);

        // Pushback
        result->push_back(duckdb_idseek_op);
    }

    // Release
    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    adj_inner_cols->Release();
    adj_output_cols->Release();
    seek_inner_cols->Release();

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
        INT type_mod = col->TypeModifier();
        types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

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

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *idxscan_cols = nullptr;

    unordered_map<ULONG, uint64_t> id_map;
    vector<vector<duckdb::LogicalType>> scan_types;
    vector<vector<uint32_t>> outer_col_maps;
    vector<vector<uint32_t>> inner_col_maps;
    vector<vector<uint64_t>> scan_projection_mapping;
    vector<vector<uint64_t>> output_projection_mapping;

    scan_types.push_back(std::vector<duckdb::LogicalType>());
    scan_projection_mapping.push_back(std::vector<uint64_t>());
    output_projection_mapping.push_back(std::vector<uint64_t>());
    inner_col_maps.push_back(std::vector<uint32_t>());

    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        CColRefTable *colref_table = (CColRefTable *)col;
        ULONG col_id = col->Id();
        id_map.insert(std::make_pair(col_id, col_idx));

        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = col->TypeModifier();
        types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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
    size_t num_outer_schemas = pGetNumOuterSchemas();

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan) {
            // IdxScan
            idxscan_expr = inner_root;
            idxscan_cols = inner_root->Prpp()->PcrsRequired()->Pdrgpcr(mp);
            CPhysicalIndexScan *idxscan_op =
                (CPhysicalIndexScan *)inner_root->Pop();
            CMDIdGPDB *index_mdid =
                CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
            gpos::ULONG oid = index_mdid->Oid();
            idx_obj_id = (uint64_t)oid;

            // check complex comparison
            bool is_complex_comparison = false;
            if (idxscan_expr->Arity() > 0) {
                CExpression *scalar_expr_child = idxscan_expr->operator[](0);
                if (scalar_expr_child->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarBoolOp) {
                    has_filter = true;
                    is_complex_comparison = true;
                }
            }

            // Traverse down until we met ScalarCmp (We assume binary tree of BoolOp)
            CExpression *scalar_cmp_expr = NULL;
            CExpression *cursor = idxscan_expr;
            while (true) {
                cursor = cursor->operator[](0);
                if (cursor->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarCmp) {
                    scalar_cmp_expr = cursor;
                    break;
                }
            }
            D_ASSERT(scalar_cmp_expr->Pop()->Eopid() ==
                     COperator::EOperatorId::EopScalarCmp);

            // Generate filter expression for
            if (is_complex_comparison) {
                /**
                 * We consider the case where ._id column have multiple CMP with other ._id cols.
                 * Since we don't load ._id column, we consider this as a AND of CMP of other .id cols.
                */

                unique_ptr<duckdb::Expression> filter_duckdb_expr;
                auto org_filter_pred_expr = idxscan_expr->operator[](0);
                D_ASSERT(org_filter_pred_expr->Pop()->Eopid() ==
                         COperator::EOperatorId::EopScalarBoolOp);

                // Create modified filter_pred_expr
                CExpression *filter_pred_expr;
                vector<uint64_t> outer_filter_cols_idx;
                for (ULONG col_idx = 0; col_idx < org_filter_pred_expr->Arity();
                     col_idx++) {
                    CExpression *child_cmp_expr =
                        org_filter_pred_expr->operator[](col_idx);
                    for (ULONG child_col_idx = 0;
                         child_col_idx < child_cmp_expr->Arity();
                         child_col_idx++) {
                        CScalarIdent *sc_ident = (CScalarIdent *)child_cmp_expr
                                                     ->
                                                     operator[](child_col_idx)
                                                     ->Pop();
                        const CColRef *col = sc_ident->Pcr();
                        if (idxscan_cols->IndexOf(col) == gpos::ulong_max) {
                            auto col_idx = outer_cols->IndexOf(col);
                            D_ASSERT(col_idx != gpos::ulong_max);
                            outer_filter_cols_idx.push_back(col_idx);
                        }
                    }
                }
                D_ASSERT(outer_filter_cols_idx.size() != 1);

                CExpressionArray *cnf_exprs = GPOS_NEW(mp) CExpressionArray(mp);
                for (ULONG i = 0; i < outer_filter_cols_idx.size() - 1; i++) {
                    CColRef *lpcr =
                        outer_cols->operator[](outer_filter_cols_idx[i]);
                    CColRef *rpcr =
                        outer_cols->operator[](outer_filter_cols_idx[i + 1]);
                    cnf_exprs->Append(CUtils::PexprScalarEqCmp(mp, lpcr, rpcr));
                }

                if (cnf_exprs->Size() == 1) {
                    filter_pred_expr = cnf_exprs->operator[](0);
                }
                else {
                    filter_pred_expr = CUtils::PexprScalarBoolOp(
                        mp, CScalarBoolOp::EBoolOperator::EboolopAnd,
                        cnf_exprs);
                }

                filter_duckdb_expr =
                    pTransformScalarExpr(filter_pred_expr, outer_cols,
                                         nullptr);  // only outer cols exist
                filter_exprs.push_back(std::move(filter_duckdb_expr));
                filter_pred_expr->Release();
            }

            // Get JoinColumnID (We assume binary tree of BoolOp)
            for (uint32_t i = 0; i < scalar_cmp_expr->Arity(); i++) {
                CScalarIdent *sc_ident =
                    (CScalarIdent *)(scalar_cmp_expr->operator[](i)->Pop());
                sccmp_colids.push_back(sc_ident->Pcr()->Id());
            }

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
                if (idx ==
                    gpos::
                        ulong_max) {  // if not found, it is ID column and to be replaced with IdSeek's column
                    if (!project_physical_id_column) {
                        continue;
                    }
                    else {
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

                pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);
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
            do_filter_pushdown = false;
            filter_expr = inner_root;
            filter_pred_expr = filter_expr->operator[](1);
            // D_ASSERT(filter_expr->operator[](0)->Pop()->Eopid() ==
            //          COperator::EOperatorId::EopPhysicalIndexScan);
            idxscan_cols =
                filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(mp);

            /**
             * TODO: revive filter pushdown
            */

            // Get filter only columns in idxscan_cols
            vector<ULONG> inner_filter_only_cols_idx;
            pGetFilterOnlyInnerColsIdx(
                filter_pred_expr, idxscan_cols /* all inner cols */,
                inner_cols /* output inner cols */, inner_filter_only_cols_idx);

            // Get required cols for seek (inner cols + filter only cols)
            CColRefArray *inner_required_cols = GPOS_NEW(mp) CColRefArray(mp);
            for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
                inner_required_cols->Append(inner_cols->operator[](col_idx));
            }
            for (auto col_idx : inner_filter_only_cols_idx) {
                CColRef *col = idxscan_cols->operator[](col_idx);
                CColRefTable *colreftbl = (CColRefTable *)col;
                // register as required column (since we use in filter)
                inner_required_cols->Append(col);
                // register as seek column
                inner_col_maps[0].push_back(
                    std::numeric_limits<uint32_t>::max());
                scan_projection_mapping[0].push_back(colreftbl->AttrNum());
                CMDIdGPDB *type_mdid =
                    CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
                scan_types[0].push_back(pConvertTypeOidToLogicalType(
                    type_mdid->Oid(), col->TypeModifier()));
            }

            unique_ptr<duckdb::Expression> filter_duckdb_expr;
            filter_duckdb_expr = pTransformScalarExpr(
                filter_pred_expr, outer_cols, inner_required_cols);
            pShiftFilterPredInnerColumnIndices(filter_duckdb_expr,
                                               outer_cols->Size());
            filter_exprs.push_back(std::move(filter_duckdb_expr));
        }
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalComputeScalarColumnar) {
            size_t num_filter_only_cols = 0;
            bool load_system_col = false;

            // generate inner_col_maps, scan_projection_mapping, output_projection_mapping, scan_types
            for (ULONG i = 0; i < inner_cols->Size(); i++) {
                CColRef *col = inner_cols->operator[](i);
                CColRefTable *colreftbl = (CColRefTable *)col;
                INT attr_no = colreftbl->AttrNum();
                ULONG col_id = colreftbl->Id();
                auto it = id_map.find(col_id);
                if (it != id_map.end()) {
                    inner_col_maps[0].push_back(it->second);
                    if (attr_no == INT(-1))
                        load_system_col = true;
                }

                // generate scan_projection_mapping
                if ((attr_no == (INT)-1)) {
                    if (load_system_col) {
                        scan_projection_mapping[0].push_back(0);
                        scan_types[0].push_back(duckdb::LogicalType::ID);
                    }
                }
                else {
                    scan_projection_mapping[0].push_back(attr_no);
                    CMDIdGPDB *type_mdid =
                        CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
                    scan_types[0].push_back(pConvertTypeOidToLogicalType(
                        type_mdid->Oid(), col->TypeModifier()));
                }
            }

            // Construct outer mapping info
            for (auto i = 0; i < num_outer_schemas; i++) {
                outer_col_maps.push_back(std::vector<uint32_t>());
                for (ULONG col_idx = 0; col_idx < outer_cols->Size();
                     col_idx++) {
                    CColRef *col = outer_cols->operator[](col_idx);
                    ULONG col_id = col->Id();
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

    D_ASSERT(inner_root != pexprInner);

    D_ASSERT(idxscan_expr != NULL);
    CColRefSet *inner_output_cols = pexprInner->Prpp()->PcrsRequired();
    CColRefSet *idxscan_output_cols = idxscan_expr->Prpp()->PcrsRequired();

    CPhysicalIndexScan *idxscan_op = (CPhysicalIndexScan *)idxscan_expr->Pop();
    CMDIdGPDB *table_mdid = CMDIdGPDB::CastMdid(idxscan_op->Ptabdesc()->MDId());
    OID table_obj_id = table_mdid->Oid();
    oids.push_back(table_obj_id);

    // Construct sid_col_idx
    for (auto i = 0; i < num_outer_schemas; i++) {
        bool sid_col_idx_found = false;
        for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
            CColRef *col = outer_cols->operator[](col_idx);
            ULONG col_id = col->Id();
            // match _tid
            auto it =
                std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
            if (it != sccmp_colids.end()) {
                D_ASSERT(!sid_col_idx_found);
                sid_col_idx = col_idx;
                sid_col_idx_found = true;
            }
        }
        D_ASSERT(sid_col_idx_found);
    }

    gpos::ULONG pred_attr_pos, pred_pos;
    duckdb::Value literal_val;
    duckdb::LogicalType pred_attr_type;
    if (has_filter && do_filter_pushdown) {
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRef *lhs_colref_o = col_factory->LookupColRef(
            ((CScalarIdent *)filter_pred_expr->operator[](0)->Pop())
                ->Pcr()
                ->Id());
        CColRefTable *lhs_colref = (CColRefTable *)lhs_colref_o;
        gpos::INT lhs_attrnum = lhs_colref->AttrNum();
        pred_attr_pos = lGetMDAccessor()
                            ->RetrieveRel(lhs_colref->GetMdidTable())
                            ->GetPosFromAttno(lhs_attrnum);
        pred_attr_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(lhs_colref->RetrieveType()->MDId())->Oid(),
            lhs_colref_o->TypeModifier());
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

    /* Generate schema flow graph for IdSeek */
    /* Note: to prevent destruction of inner_col_maps due to move, call this before PhysicalIdSeek */
    pBuildSchemaFlowGraphForBinaryOperator(tmp_schema, inner_col_maps.size());
    vector<uint32_t> union_inner_col_map = inner_col_maps[0];
    if (!do_filter_pushdown) {
        if (has_filter) {
            filter_exprs[0]->Print();
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_maps, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, filter_exprs, false);
            result->push_back(op);
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
        D_ASSERT(false);
        duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
            tmp_schema, sid_col_idx, oids, output_projection_mapping,
            outer_col_maps, inner_col_maps, union_inner_col_map,
            scan_projection_mapping, scan_types, false);
        result->push_back(op);
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    idxscan_cols->Release();

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

    if (pIsJoinRhsOutputPhysicalIdOnly(plan_expr)) {
        pTransformEopPhysicalInnerIndexNLJoinToProjectionForUnionAllInner(
            plan_expr, result);
    }
    else {
        if (drvd_prop_plan->Pos()->UlSortColumns() > 0) {
            pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(
                plan_expr, result);
        }
        else {
            pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(
                plan_expr, result);
        }
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
        INT type_mod = col->TypeModifier();
        types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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
                        auto it = id_map.find(proj_col->Id());
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
                            INT type_mod = proj_elem->Pcr()->TypeModifier();
                            scan_type.push_back(pConvertTypeOidToLogicalType(
                                type_oid, type_mod));
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
                        GPOS_ASSERT(false);
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
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    if (!do_filter_pushdown) {
        if (has_filter) {
            GPOS_ASSERT(false);
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
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

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
        INT type_mod = col->TypeModifier();
        types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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
    bool has_filter_only_column = false;
    size_t num_filter_only_col = 0;

    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    CExpression *idxscan_expr = NULL;
    vector<unique_ptr<duckdb::Expression>> filter_pred_duckdb_exprs;
    vector<vector<ULONG>> inner_col_ids;

    while (true) {
        if (inner_root->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalSerialUnionAll) {
            bool load_system_col = false;
            for (uint32_t i = 0; i < inner_root->Arity();
                 i++) {  // for each idx(only)scan expression
                // TODO currently support this pattern type only
                D_ASSERT(
                    inner_root->operator[](i)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                        COperator::EOperatorId::EopPhysicalIndexScan ||
                    inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                        COperator::EOperatorId::EopPhysicalFilter);
                D_ASSERT(
                    inner_root->operator[](i)->operator[](1)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopScalarProjectList);

                if (inner_root->operator[](i)->operator[](0)->Pop()->Eopid() ==
                    COperator::EOperatorId::EopPhysicalFilter) {
                    has_filter = true;
                }

                CExpression *unionall_expr = inner_root;
                CExpression *inner_idxscan_expr =
                    !has_filter
                        ? inner_root->operator[](i)->operator[](0)
                        : inner_root->operator[](i)->operator[](0)->operator[](
                              0);
                CExpression *projectlist_expr =
                    inner_root->operator[](i)->operator[](1);

                // Get JoinColumnID
                if (i == 0) {
                    for (uint32_t j = 0;
                         j < inner_idxscan_expr->operator[](0)->Arity(); j++) {
                        CScalarIdent *sc_ident =
                            (CScalarIdent *)(inner_idxscan_expr->operator[](0)
                                                 ->
                                                 operator[](j)
                                                 ->Pop());
                        sccmp_colids.push_back(sc_ident->Pcr()->Id());
                    }
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

                inner_col_maps.push_back(std::vector<uint32_t>());

                // projection mapping (output to scan table mapping)
                vector<uint64_t> output_ident_mapping;

                // for filter
                vector<ULONG> inner_col_id;
                auto scalarident_pattern = vector<COperator::EOperatorId>(
                    {COperator::EOperatorId::EopScalarProjectElement,
                     COperator::EOperatorId::EopScalarIdent});

                if (i == 0) {
                    for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                        D_ASSERT(
                            projectlist_expr->operator[](j)->Pop()->Eopid() ==
                            COperator::EOperatorId::EopScalarProjectElement);
                        CExpression *proj_elem_expr =
                            projectlist_expr->operator[](j);
                        CScalarProjectElement *proj_elem =
                            (CScalarProjectElement
                                 *)(projectlist_expr->operator[](j)->Pop());
                        CColRefTable *proj_col =
                            (CColRefTable *)proj_elem->Pcr();
                        auto it = id_map.find(proj_col->Id());

                        if (it != id_map.end()) {
                            union_inner_col_map.push_back(it->second);
                            if (proj_col->AttrNum() == INT(-1))
                                load_system_col = true;
                        }
                        else {
                            if (proj_col->AttrNum() != INT(-1)) {
                                /**
                                 * Note: Filter-only columns are appended to the output columns.
                                 * IdSeek finds out filter-only columns using inner_col_map
                                 * and then appends them to the output columns to generate temp chunk.
                                */
                                union_inner_col_map.push_back(
                                    output_cols->Size() + num_filter_only_col);
                                num_filter_only_col++;
                                has_filter_only_column = true;
                            }
                        }
                    }
                }

                // Construct innter mapping, scan projection mapping, scan type infos
                for (uint32_t j = 0; j < projectlist_expr->Arity(); j++) {
                    D_ASSERT(projectlist_expr->operator[](j)->Pop()->Eopid() ==
                             COperator::EOperatorId::EopScalarProjectElement);
                    CExpression *proj_elem_expr =
                        projectlist_expr->operator[](j);
                    CScalarProjectElement *proj_elem =
                        (CScalarProjectElement
                             *)(projectlist_expr->operator[](j)->Pop());
                    CColRefTable *proj_col = (CColRefTable *)proj_elem->Pcr();
                    CScalarIdent *ident_op =
                        (CScalarIdent *)proj_elem_expr->PdrgPexpr()
                            ->
                            operator[](0)
                            ->Pop();

                    if (projectlist_expr->operator[](j)
                            ->
                            operator[](0)
                            ->Pop()
                            ->Eopid() ==
                        COperator::EOperatorId::EopScalarIdent) {
                        // non-null column
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

                                if (pMatchExprPattern(proj_elem_expr,
                                                      scalarident_pattern)) {
                                    inner_col_id.push_back(
                                        ident_op->Pcr()->Id());
                                }
                                else {
                                    inner_col_id.push_back(
                                        std::numeric_limits<ULONG>::max());
                                }
                            }
                        }
                        else {
                            scan_ident_mapping.push_back(attr_no);
                            output_ident_mapping.push_back(j);
                            CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(
                                proj_col->RetrieveType()->MDId());
                            OID type_oid = type_mdid->Oid();
                            INT type_mod = proj_elem->Pcr()->TypeModifier();
                            scan_type.push_back(pConvertTypeOidToLogicalType(
                                type_oid, type_mod));

                            if (pMatchExprPattern(proj_elem_expr,
                                                  scalarident_pattern)) {
                                inner_col_id.push_back(ident_op->Pcr()->Id());
                            }
                            else {
                                inner_col_id.push_back(
                                    std::numeric_limits<ULONG>::max());
                            }
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
                        GPOS_ASSERT(false);
                        throw duckdb::InvalidInputException(
                            "Project element types other than ident & const is "
                            "not desired");
                    }
                }

                scan_projection_mapping.push_back(scan_ident_mapping);
                scan_types.push_back(std::move(scan_type));
                output_projection_mapping.push_back(output_ident_mapping);
                inner_col_ids.push_back(inner_col_id);
            }

            // Construct outer mapping info
            auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
            duckdb::idx_t num_outer_schemas = 1;
            for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
                num_outer_schemas *= num_schemas_of_childs_prev[i];
            }
            for (auto i = 0; i < num_outer_schemas; i++) {
                outer_col_maps.push_back(std::vector<uint32_t>());
                bool sid_col_idx_found = false;
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
        else if (inner_root->Pop()->Eopid() ==
                 COperator::EOperatorId::EopPhysicalFilter) {
            has_filter = true;
            do_filter_pushdown = false;
            filter_expr = inner_root;
            filter_pred_expr = filter_expr->operator[](1);
            CColRefArray *idxscan_cols =
                filter_expr->operator[](0)->Prpp()->PcrsRequired()->Pdrgpcr(mp);
            auto filter_duckdb_expr = pTransformScalarExpr(
                filter_pred_expr, outer_cols, idxscan_cols);
            pShiftFilterPredInnerColumnIndices(filter_duckdb_expr,
                                               outer_cols->Size());
            filter_pred_duckdb_exprs.push_back(std::move(filter_duckdb_expr));
            idxscan_cols->Release();
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
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    /* Generate operator and push */
    duckdb::Schema tmp_schema;
    tmp_schema.setStoredTypes(types);

    /**
     * TODO: this code is currently wrong. It should be fixed.
     * IdSeek should generate multiple schemas.
     * However, this code now only generates one schema (union schema).
     * Instead, it uses tmp_schema given to the PhysicalIdSeek to initialize.
    */

    if (generate_sfg) {
        vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
        auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
        duckdb::idx_t num_total_schemas_prev = 1;
        for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
            num_total_schemas_prev *= num_schemas_of_childs_prev[i];
        }
        pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
        num_schemas_of_childs.push_back(
            {num_total_schemas_prev, inner_col_maps.size()});
        // num_schemas_of_childs.push_back({prev_local_schemas.size()});
        pipeline_schemas.push_back(prev_local_schemas);
        pipeline_union_schema.push_back(tmp_schema);
    }

    if (!do_filter_pushdown) {
        if (has_filter) {
            duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalIdSeek(
                tmp_schema, sid_col_idx, oids, output_projection_mapping,
                outer_col_maps, inner_col_maps, union_inner_col_map,
                scan_projection_mapping, scan_types, filter_pred_duckdb_exprs,
                false);
            result->push_back(op);
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
        GPOS_ASSERT(false);
        throw NotImplementedException("InnerIdxNLJoin for Filter case");
    }

    output_cols->Release();
    outer_cols->Release();
    inner_cols->Release();
    outer_inner_cols->Release();
}

void Planner::pTransformEopPhysicalInnerIndexNLJoinToProjectionForUnionAllInner(
    CExpression *plan_expr, vector<duckdb::CypherPhysicalOperator *> *result)
{
    CMemoryPool *mp = this->memory_pool;

    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprOuter = (*plan_expr)[0];
    CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CExpression *pexprInner = (*plan_expr)[1];
    CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // ID column only (n._id)
    vector<duckdb::LogicalType> proj_output_types;
    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = col->TypeModifier();
        proj_output_types.push_back(
            pConvertTypeOidToLogicalType(type_oid, type_mod));
    }

    // generate projection expressions (I don't assume the _tid is always the last)
    vector<unique_ptr<duckdb::Expression>> proj_exprs(output_cols->Size());
    vector<bool> outer_cols_is_id_col(outer_cols->Size(), true);
    duckdb::idx_t output_id_col_idx = gpos::ulong_max;

    // projection expressions for non _id columns
    for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
         output_col_idx++) {
        auto outer_col_idx =
            outer_cols->IndexOf(output_cols->operator[](output_col_idx));
        if (outer_col_idx != gpos::ulong_max) {  // non _id column
            proj_exprs[output_col_idx] =
                make_unique<duckdb::BoundReferenceExpression>(
                    proj_output_types[output_col_idx], (int)outer_col_idx);
            outer_cols_is_id_col[outer_col_idx] = false;
        }
        else {  // _id column
            output_id_col_idx = output_col_idx;
        }
    }

    // projection expressions for _id column
    if (output_id_col_idx != gpos::ulong_max) {
        for (size_t outer_col_idx = 0;
             outer_col_idx < outer_cols_is_id_col.size(); outer_col_idx++) {
            if (outer_cols_is_id_col[outer_col_idx]) {
                proj_exprs[output_id_col_idx] =
                    (make_unique<duckdb::BoundReferenceExpression>(
                        duckdb::LogicalType(duckdb::LogicalTypeId::ID),
                        (int)outer_col_idx));
            }
        }
    }

    // generate schema
    duckdb::Schema proj_schema;
    proj_schema.setStoredTypes(proj_output_types);

    // define op
    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
    result->push_back(op);

    // generate schema flow graph
    pBuildSchemaFlowGraphForUnaryOperator(proj_schema);
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalHashJoinToHashJoin(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;

    // Don't need to explicitly convert to LeftAntiJoin, LeftSemiJoin, LeftMarkJoin
    CPhysicalInnerHashJoin *expr_op =
        (CPhysicalInnerHashJoin *)plan_expr->Pop();
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *hash_output_cols = GPOS_NEW(mp) CColRefArray(mp);
    CColRefArray *filter_output_cols = NULL;
    CColRefArray *proj_output_cols = output_cols;

    // Obtain left and right cols
    CExpression *pexprLeft = (*plan_expr)[0];
    CColRefArray *left_cols;
    CColRefArray *right_cols;
    if (pexprLeft->Pop()->Eopid() ==
        COperator::EOperatorId::
            EopPhysicalSerialUnionAll) {  // TODO: correctness check (is it okay to call PdrgpcrOutput?)
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprLeft->Pop();
        left_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        left_cols = pexprLeft->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }
    CExpression *pexprRight = (*plan_expr)[1];
    if (pexprRight->Pop()->Eopid() ==
        COperator::EOperatorId::
            EopPhysicalSerialUnionAll) {  // TODO: correctness check (is it okay to call PdrgpcrOutput?)
        CPhysicalSerialUnionAll *unionall_op =
            (CPhysicalSerialUnionAll *)pexprRight->Pop();
        right_cols = unionall_op->PdrgpcrOutput();
    }
    else {
        right_cols = pexprRight->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    }

    vector<duckdb::JoinCondition> join_conds;
    pTranslatePredicateToJoinCondition(plan_expr->operator[](2), join_conds,
                                       left_cols, right_cols); // Shifting is not needed!
    hash_output_cols = output_cols;

    // Construct col map, types and etc
    vector<duckdb::LogicalType> hash_output_types;
    vector<uint32_t> left_col_map;
    vector<uint32_t> right_col_map;

    pGetDuckDBTypesFromColRefs(hash_output_cols, hash_output_types);
    pConstructColMapping(left_cols, hash_output_cols, left_col_map);
    pConstructColMapping(right_cols, hash_output_cols, right_col_map);

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
            INT type_mod = cols_build_list->operator[](col_idx)->TypeModifier();
            duckdb::LogicalType col_type =
                pConvertTypeOidToLogicalType(type_oid, type_mod);
            right_build_types.push_back(col_type);
        }
        D_ASSERT(right_build_map.size() == right_build_types.size());
    }
    else if (join_type == duckdb::JoinType::ANTI) {
        // do nothing
    }
    else if (join_type == duckdb::JoinType::SEMI) {
        // do nothing
    }
    else {
        D_ASSERT(false);
    }

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(hash_output_types);

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalHashJoin(
        schema, move(join_conds), join_type, left_col_map, right_col_map,
        right_build_types, right_build_map);

    // Release
    output_cols->Release();
    left_cols->Release();
    right_cols->Release();
    hash_output_cols->Release();

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalMergeJoinToMergeJoin(CExpression *plan_expr)
{
    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerMergeJoin *expr_op =
        (CPhysicalInnerMergeJoin *)plan_expr->Pop();
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
        INT type_mod = col->TypeModifier();
        duckdb::LogicalType col_type =
            pConvertTypeOidToLogicalType(type_oid, type_mod);
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
        INT type_mod = left_cols->operator[](col_idx)->TypeModifier();
        lhs_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }
    for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
        OID type_oid =
            CMDIdGPDB::CastMdid(
                right_cols->operator[](col_idx)->RetrieveType()->MDId())
                ->Oid();
        INT type_mod = right_cols->operator[](col_idx)->TypeModifier();
        rhs_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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
    // CMemoryPool *mp = this->memory_pool;

    // D_ASSERT(plan_expr->Arity() == 3);

    // /* Non-root - call LHS first for inner materialization */
    // vector<duckdb::CypherPhysicalOperator *> *lhs_result =
    //     pTraverseTransformPhysicalPlan(
    //         plan_expr->PdrgPexpr()->operator[](0));  // outer
    // vector<duckdb::CypherPhysicalOperator *> *rhs_result =
    //     pTraverseTransformPhysicalPlan(
    //         plan_expr->PdrgPexpr()->operator[](1));  // inner

    // /* finish rhs pipeline */
    // CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    // CExpression *pexprOuter = (*plan_expr)[0];
    // CColRefArray *outer_cols = pexprOuter->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    // CExpression *pexprInner = (*plan_expr)[1];
    // CColRefArray *inner_cols = pexprInner->Prpp()->PcrsRequired()->Pdrgpcr(mp);

    // vector<duckdb::LogicalType> types;
    // vector<uint32_t> outer_col_map;
    // vector<uint32_t> inner_col_map;

    // for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
    //     CColRef *col = output_cols->operator[](col_idx);
    //     OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
    //     duckdb::LogicalType col_type = pConvertTypeOidToLogicalType(type_oid);
    //     types.push_back(col_type);
    // }
    // for (ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++) {
    //     outer_col_map.push_back(
    //         output_cols->IndexOf(outer_cols->operator[](col_idx)));
    // }
    // for (ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
    //     inner_col_map.push_back(
    //         output_cols->IndexOf(inner_cols->operator[](col_idx)));
    // }

    // // define op
    // duckdb::Schema tmp_schema;
    // tmp_schema.setStoredTypes(types);
    // duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalCrossProduct(
    //     tmp_schema, outer_col_map, inner_col_map);

    // // finish rhs pipeline
    // rhs_result->push_back(op);
    // auto pipeline = new duckdb::CypherPipeline(*rhs_result, pipelines.size());
    // pipelines.push_back(pipeline);

    // // return lhs pipeline
    // lhs_result->push_back(op);
    // return lhs_result;
    CMemoryPool *mp = this->memory_pool;
    D_ASSERT(plan_expr->Arity() == 3);

    CPhysicalInnerNLJoin *expr_op = (CPhysicalInnerNLJoin *)plan_expr->Pop();
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
        INT type_mod = col->TypeModifier();
        duckdb::LogicalType col_type =
            pConvertTypeOidToLogicalType(type_oid, type_mod);
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

    // define op
    duckdb::Schema schema;
    schema.setStoredTypes(types);

    // Calculate lhs and rhs types
    // vector<duckdb::LogicalType> lhs_types;
    // vector<duckdb::LogicalType> rhs_types;
    // for (ULONG col_idx = 0; col_idx < left_cols->Size(); col_idx++) {
    //     OID type_oid =
    //         CMDIdGPDB::CastMdid(
    //             left_cols->operator[](col_idx)->RetrieveType()->MDId())
    //             ->Oid();
    //     lhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    // }
    // for (ULONG col_idx = 0; col_idx < right_cols->Size(); col_idx++) {
    //     OID type_oid =
    //         CMDIdGPDB::CastMdid(
    //             right_cols->operator[](col_idx)->RetrieveType()->MDId())
    //             ->Oid();
    //     rhs_types.push_back(pConvertTypeOidToLogicalType(type_oid));
    // }

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalCrossProduct(schema, left_col_map, right_col_map);

    return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
}

vector<duckdb::CypherPhysicalOperator *> *
Planner::pTransformEopPhysicalNLJoinToBlockwiseNLJoin(CExpression *plan_expr,
                                                      bool is_correlated)
{
    CMemoryPool *mp = this->memory_pool;

    D_ASSERT(plan_expr->Arity() == 3);

    /* Non-root - call left child */
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
        INT type_mod = col->TypeModifier();
        duckdb::LogicalType col_type =
            pConvertTypeOidToLogicalType(type_oid, type_mod);
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

    duckdb::JoinType join_type = pTranslateJoinType(plan_expr->Pop());
    D_ASSERT(join_type != duckdb::JoinType::RIGHT);

    CExpression *join_pred = (*plan_expr)[2];
    if (plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalLeftAntiSemiHashJoin) {
        // Temporal code for anti hash join handling
        join_pred = CUtils::PexprNegate(mp, join_pred);
    }

    auto join_condition_expr = pTransformScalarExpr(
        join_pred, outer_cols, inner_cols);  // left - right

    duckdb::CypherPhysicalOperator *op = new duckdb::PhysicalBlockwiseNLJoin(
        schema, move(join_condition_expr), join_type, outer_col_map,
        inner_col_map);

    if (is_correlated) {
        // unsupported yet
        D_ASSERT(false);
        // // finish lhs pipeline
        // lhs_result->push_back(op);
        // auto pipeline =
        //     new duckdb::CypherPipeline(*lhs_result, pipelines.size());
        // pipelines.push_back(pipeline);

        // // return rhs pipeline
        // rhs_result->push_back(op);
        // return rhs_result;
    }
    else {
        return pBuildSchemaflowGraphForBinaryJoin(plan_expr, op, schema);
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

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalTop(tmp_schema, limit, offset);
    result->push_back(op);

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
            GPOS_ASSERT(false);
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

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

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
    vector<string> output_column_names_proj;
    // vector<duckdb::LogicalType> groups_type;
    // vector<ULONG> groups_idx;
    // vector<ULONG> proj_mapping;
    vector<uint64_t> output_projection_mapping;

    CPhysicalAgg *agg_op = (CPhysicalAgg *)plan_expr->Pop();
    CExpression *pexprProjRelational = (*plan_expr)[0];  // Prev op
    CColRefArray *output_cols = plan_expr->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *child_cols =
        pexprProjRelational->Prpp()->PcrsRequired()->Pdrgpcr(mp);
    CColRefArray *interm_output_cols =
        plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
    CExpression *pexprProjList = (*plan_expr)[1];  // Projection list
    const CColRefArray *grouping_cols = agg_op->PdrgpcrGroupingCols();
    CColRefSet *grouping_col_set = GPOS_NEW(mp) CColRefSet(mp, grouping_cols);
    CColRefArray *grouping_cols_sorted = grouping_col_set->Pdrgpcr(mp);

    // used for pre-projection
    vector<unique_ptr<duckdb::Expression>> proj_exprs;

    // used for post-projection
    vector<unique_ptr<duckdb::Expression>> post_proj_exprs;

    // get agg groups
    uint64_t num_outputs_in_grouping_col = 0;
    for (ULONG group_col_idx = 0; group_col_idx < grouping_cols_sorted->Size();
         group_col_idx++) {
        CColRef *col = grouping_cols_sorted->operator[](group_col_idx);
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        INT type_mod = col->TypeModifier();
        duckdb::LogicalType col_type =
            pConvertTypeOidToLogicalType(type_oid, type_mod);
        ULONG child_idx = child_cols->IndexOf(col);
        agg_groups.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_idx));
        proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            col_type, child_idx));
        proj_types.push_back(col_type);
        output_column_names_proj.push_back(pGetColNameFromColRef(col));
        if (output_cols->IndexOf(col) != gpos::ulong_max) {
            output_projection_mapping.push_back(num_outputs_in_grouping_col++);
        } else {
            output_projection_mapping.push_back(std::numeric_limits<uint32_t>::max());
        }
    }

    // get output columns
    for (ULONG output_col_idx = 0; output_col_idx < output_cols->Size();
         output_col_idx++) {
        CColRef *col = output_cols->operator[](output_col_idx);
        if (grouping_cols->IndexOf(col) == gpos::ulong_max)
            continue;
        // output_projection_mapping;
        OID type_oid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId())->Oid();
        INT type_mod = col->TypeModifier();
        duckdb::LogicalType col_type =
            pConvertTypeOidToLogicalType(type_oid, type_mod);
        types.push_back(col_type);
        output_column_names.push_back(pGetColNameFromColRef(col));
        // proj_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
        //     col_type, child_cols->IndexOf(col)));
        // agg_groups.push_back(make_unique<duckdb::BoundReferenceExpression>(
        //     col_type, child_cols->IndexOf(col)));
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
        output_column_names_proj.push_back(pGetColNameFromColRef(
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
        proj_schema.setStoredColumnNames(output_column_names_proj);
        pBuildSchemaFlowGraphForUnaryOperator(proj_schema);
        duckdb::CypherPhysicalOperator *proj_op =
            new duckdb::PhysicalProjection(proj_schema, move(proj_exprs));
        result->push_back(proj_op);
    }

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op;
    if (agg_groups.empty()) {
        op = new duckdb::PhysicalHashAggregate(tmp_schema, output_projection_mapping, move(agg_exprs));
    }
    else {
        op = new duckdb::PhysicalHashAggregate(tmp_schema, output_projection_mapping, move(agg_exprs),
                                               move(agg_groups));
    }

    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);

    // finish pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);

    // new pipeline
    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

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

    // generate schema flow graph for the filter
    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

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

            pBuildSchemaFlowGraphForUnaryOperator(output_schema);
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

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op =
        new duckdb::PhysicalSort(tmp_schema, move(orders));
    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);

    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

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

    pBuildSchemaFlowGraphForUnaryOperator(tmp_schema);

    duckdb::CypherPhysicalOperator *op;
    if (has_limit) {
        op = new duckdb::PhysicalTopNSort(tmp_schema, move(orders), limit,
                                          offset);
    }
    else {
        op = new duckdb::PhysicalSort(tmp_schema, move(orders));
    }

    result->push_back(op);
    pGenerateSchemaFlowGraph(*result);

    // break pipeline
    auto pipeline = new duckdb::CypherPipeline(*result, pipelines.size());
    pipelines.push_back(pipeline);

    auto new_result = new vector<duckdb::CypherPhysicalOperator *>();
    new_result->push_back(op);

    if (generate_sfg) {
        // Set for the current pipeline. We consider after group by, schema is merged.
        pClearSchemaFlowGraph();
        pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
        num_schemas_of_childs.push_back({1});
        pipeline_schemas.push_back({tmp_schema});
        pipeline_union_schema.push_back(tmp_schema);
    }

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
                ->Oid(),
            outer_cols->operator[](lhs_pos)->TypeModifier());
        rhs_type = pConvertTypeOidToLogicalType(
            CMDIdGPDB::CastMdid(
                outer_cols->operator[](rhs_pos)->RetrieveType()->MDId())
                ->Oid(),
            outer_cols->operator[](rhs_pos)->TypeModifier());

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
                ->Oid(),
            outer_cols->operator[](lhs_pos)->TypeModifier());

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
                ->Oid(),
            outer_cols->operator[](rhs_pos)->TypeModifier());

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
        GPOS_ASSERT(false);
        throw duckdb::NotImplementedException("pGenerateFilterExprs");
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
        INT type_mod = columns->operator[](i)->TypeModifier();
        out_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
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
    if (!generate_sfg)
        return;
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

void Planner::pBuildSchemaFlowGraphForUnaryOperator(
    duckdb::Schema &output_schema)
{
    if (!generate_sfg)
        return;
    vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
    auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
    duckdb::idx_t num_total_schemas_prev = 1;
    for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
        num_total_schemas_prev *= num_schemas_of_childs_prev[i];
    }
    // if (num_schemas_of_childs_prev.size() == 1) {
    //     D_ASSERT(num_total_schemas_prev == prev_local_schemas.size());
    // }
    pipeline_operator_types.push_back(duckdb::OperatorType::UNARY);
    num_schemas_of_childs.push_back({num_total_schemas_prev});
    pipeline_schemas.push_back(prev_local_schemas);  // TODO useless..
    pipeline_union_schema.push_back(output_schema);
}

void Planner::pBuildSchemaFlowGraphForBinaryOperator(
    duckdb::Schema &output_schema, size_t num_rhs_schemas)
{
    if (!generate_sfg)
        return;
    vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
    auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
    duckdb::idx_t num_total_schemas_prev = 1;
    for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
        num_total_schemas_prev *= num_schemas_of_childs_prev[i];
    }
    pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
    num_schemas_of_childs.push_back({num_total_schemas_prev, num_rhs_schemas});
    pipeline_schemas.push_back(prev_local_schemas);
    pipeline_union_schema.push_back(output_schema);
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
    // TODO what about OR condition in duckdb ?? -> IDK
    auto *op = pred->Pop();
    if (op->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)op;
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            D_ASSERT(pred->Arity() == 2);
            // Split predicates
            pTranslatePredicateToJoinCondition(pred->operator[](0), out_conds,
                                               lhs_cols, rhs_cols);
            pTranslatePredicateToJoinCondition(pred->operator[](1), out_conds,
                                               lhs_cols, rhs_cols);
        }
        else if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
            D_ASSERT(false);
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
        else {
            D_ASSERT(false);
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

bool Planner::pIsFilterPushdownAbleIntoScan(CExpression *selection_expr)
{

    D_ASSERT(selection_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalFilter);
    CExpression *filter_expr = NULL;
    CExpression *filter_pred_expr = NULL;
    filter_expr = selection_expr;
    filter_pred_expr = filter_expr->operator[](1);

    auto ok = filter_pred_expr->Pop()->Eopid() ==
                  COperator::EOperatorId::EopScalarCmp 
                  &&
              ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
                   IMDType::ECmpType::EcmptEq
            //    ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            //        IMDType::ECmpType::EcmptNEq ||
            //    ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            //        IMDType::ECmpType::EcmptL ||
            //    ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            //        IMDType::ECmpType::EcmptLEq ||
            //    ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            //        IMDType::ECmpType::EcmptG ||
            //    ((CScalarCmp *)(filter_pred_expr->Pop()))->ParseCmpType() ==
            //        IMDType::ECmpType::EcmptGEq) 
            &&
              filter_pred_expr->operator[](0)->Pop()->Eopid() ==
                  COperator::EOperatorId::EopScalarIdent &&
              filter_pred_expr->operator[](1)->Pop()->Eopid() ==
                  COperator::EOperatorId::EopScalarConst;

    return ok;
}

duckdb::OrderByNullType Planner::pTranslateNullType(
    COrderSpec::ENullTreatment ent)
{
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
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftAntiSemiNLJoin: {
            return duckdb::JoinType::ANTI;
        }
        case COperator::EOperatorId::EopPhysicalLeftSemiNLJoin:
        case COperator::EOperatorId::EopPhysicalLeftSemiHashJoin:
        case COperator::EOperatorId::EopPhysicalCorrelatedLeftSemiNLJoin: {
            return duckdb::JoinType::SEMI;
        }
            // TODO where is FULL OUTER??????
    }
    D_ASSERT(false);
}


CExpression* Planner::pPredToDNF(CExpression *pred) {
    CMemoryPool *mp = this->memory_pool;

    if (pred->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp) {
        CScalarBoolOp *boolop = (CScalarBoolOp *)pred->Pop();
        if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopAnd) {
            // Recursively convert children to DNF
            CExpression* leftDNF = pPredToDNF(pred->operator[](0));
            CExpression* rightDNF = pPredToDNF(pred->operator[](1));

            // Apply distributive law if one child is an OR
            CScalarBoolOp *left_op = (CScalarBoolOp *)leftDNF->Pop();
            CScalarBoolOp *right_op = (CScalarBoolOp *)rightDNF->Pop();
            if (left_op->Eopid() == COperator::EOperatorId::EopScalarBoolOp &&
                left_op->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
                // (A OR B) AND C => (A AND C) OR (B AND C)
                return pDistributeANDOverOR(leftDNF, rightDNF);
            } else if (right_op->Eopid() == COperator::EOperatorId::EopScalarBoolOp &&
                       right_op->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
                // A AND (B OR C) => (A AND B) OR (A AND C)
                return pDistributeANDOverOR(rightDNF, leftDNF);
            } else {
                // If neither child is an OR, just combine the DNF children with AND
                CExpressionArray* newDNF = GPOS_NEW(mp) CExpressionArray(mp);
                newDNF->Append(leftDNF);
                newDNF->Append(rightDNF);
                return CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopAnd, newDNF);
            }
        } else if (boolop->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr) {
            // Recursively convert children to DNF
            CExpression* leftDNF = pPredToDNF(pred->operator[](0));
            CExpression* rightDNF = pPredToDNF(pred->operator[](1));
            CExpressionArray* newDNF = GPOS_NEW(mp) CExpressionArray(mp);
            newDNF->Append(leftDNF);
            newDNF->Append(rightDNF);

            // Combine the DNF children with OR
            return CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopOr, newDNF);
        }
    } else {
        return pred;
    }
}
CExpression* Planner::pDistributeANDOverOR(CExpression *a, CExpression *b) {
    CMemoryPool *mp = this->memory_pool;
    // Ensure that 'a' is the OR expression and 'b' is the one to distribute
    // If 'a' is not an OR expression, we should swap 'a' and 'b'
    if (!(a->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp &&
        ((CScalarBoolOp *)(a->Pop()))->Eboolop() == CScalarBoolOp::EBoolOperator::EboolopOr)) {
        std::swap(a, b);
    }

    // Now 'a' is (B OR C) and 'b' is A in the (A AND (B OR C)) structure
    CExpression* left = a->operator[](0);  // B
    CExpression* right = a->operator[](1); // C

    // Create (A AND B)
    CExpressionArray* left_array = GPOS_NEW(mp) CExpressionArray(mp);
    left_array->Append(b);
    left_array->Append(left);
    auto left_and = CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopAnd, left_array);

    // Create (A AND C)
    CExpressionArray* right_array = GPOS_NEW(mp) CExpressionArray(mp);
    right_array->Append(b);
    right_array->Append(right);
    auto right_and = CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopAnd, right_array);

    // Create ((A AND B) OR (A AND C))
    CExpressionArray* or_array = GPOS_NEW(mp) CExpressionArray(mp);
    or_array->Append(left_and);
    or_array->Append(right_and);
    return CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopOr, or_array);
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
            GPOS_ASSERT(false);
            throw InternalException(
                "Attempting to execute expression of unknown type!");
    }
}

void Planner::pShiftFilterPredInnerColumnIndices(
    unique_ptr<duckdb::Expression> &expr, size_t outer_size)
{
    switch (expr->expression_class) {
        case duckdb::ExpressionClass::BOUND_BETWEEN: {
            auto between_expr = (duckdb::BoundBetweenExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(between_expr->input, outer_size);
            pShiftFilterPredInnerColumnIndices(between_expr->lower, outer_size);
            pShiftFilterPredInnerColumnIndices(between_expr->upper, outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_REF: {
            /**
             * IdSeek will create temp chunk 
             * which is a concat of outer and inner chunk
             * e.g., oc1, oc2, ...., ic1, ic2, ....
             * Therefore, inner column index should be added by outer column size
            */
            auto bound_ref_expr =
                (duckdb::BoundReferenceExpression *)expr.get();
            if (bound_ref_expr->is_inner) {
                bound_ref_expr->index = bound_ref_expr->index + outer_size;
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CASE: {
            auto bound_case_expr = (duckdb::BoundCaseExpression *)expr.get();
            for (auto &bound_case_check : bound_case_expr->case_checks) {
                pShiftFilterPredInnerColumnIndices(bound_case_check.when_expr,
                                                   outer_size);
                pShiftFilterPredInnerColumnIndices(bound_case_check.then_expr,
                                                   outer_size);
            }
            pShiftFilterPredInnerColumnIndices(bound_case_expr->else_expr,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CAST: {
            auto bound_cast_expr = (duckdb::BoundCastExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(bound_cast_expr->child,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_COMPARISON: {
            auto bound_comparison_expr =
                (duckdb::BoundComparisonExpression *)expr.get();
            pShiftFilterPredInnerColumnIndices(bound_comparison_expr->left,
                                               outer_size);
            pShiftFilterPredInnerColumnIndices(bound_comparison_expr->right,
                                               outer_size);
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
            auto bound_conjunction_expr =
                (duckdb::BoundConjunctionExpression *)expr.get();
            for (auto &child : bound_conjunction_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_CONSTANT:
            break;
        case duckdb::ExpressionClass::BOUND_FUNCTION: {
            auto bound_function_expr =
                (duckdb::BoundFunctionExpression *)expr.get();
            for (auto &child : bound_function_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_OPERATOR: {
            auto bound_operator_expr =
                (duckdb::BoundOperatorExpression *)expr.get();
            for (auto &child : bound_operator_expr->children) {
                pShiftFilterPredInnerColumnIndices(child, outer_size);
            }
            break;
        }
        case duckdb::ExpressionClass::BOUND_PARAMETER:
            break;
        default:
            GPOS_ASSERT(false);
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
        vector<duckdb::Schema> out_schemas;
        pGenerateCartesianProductSchema(prev_local_schemas, rhs_schemas,
                                        out_schemas);
        pipeline_schemas.push_back(out_schemas);
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
        INT type_mod = col->TypeModifier();
        output_types.push_back(
            pConvertTypeOidToLogicalType(type_oid, type_mod));
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

bool Planner::pIsColEdgeProperty(const CColRef *colref)
{
    const CColRefTable *colref_table = (const CColRefTable *)colref;
    const CName &col_name = colref_table->Name();
    wchar_t *full_col_name, *col_only_name, *pt;
    full_col_name = new wchar_t[std::wcslen(col_name.Pstr()->GetBuffer()) + 1];
    std::wcscpy(full_col_name, col_name.Pstr()->GetBuffer());
    col_only_name = std::wcstok(full_col_name, L".", &pt);
    col_only_name = std::wcstok(NULL, L".", &pt);

    if (col_only_name == NULL) {
        return true;
    }
    else {
        return (std::wcsncmp(col_only_name, L"_sid", 4) != 0) &&
               (std::wcsncmp(col_only_name, L"_tid", 4) != 0) &&
               (std::wcsncmp(col_only_name, L"_id", 4) != 0);
    }
}

void Planner::pGenerateCartesianProductSchema(
    vector<duckdb::Schema> &lhs_schemas, vector<duckdb::Schema> &rhs_schemas,
    vector<duckdb::Schema> &out_schemas)
{
    /**
    * TODO: This code assumes that the rhs is simply appended to the lhs.
    * If the output columns are shuffled, this code will not work. 
    */
    for (auto &lhs_schema : lhs_schemas) {
        for (auto &rhs_schema : rhs_schemas) {
            duckdb::Schema tmp_schema;
            tmp_schema.setStoredTypes(lhs_schema.getStoredTypes());
            tmp_schema.appendStoredTypes(rhs_schema.getStoredTypes());
            out_schemas.push_back(tmp_schema);
        }
    }
}

bool Planner::pIsJoinRhsOutputPhysicalIdOnly(CExpression *plan_expr)
{
    /* 
     * Example query: 
     * MATCH (m:Comment)-[roc:REPLY_OF_COMMENT]->(n:Comment) RETURN m.id AS messageId
     * 
     * The pattern:
     * CPhysicalInnerIndexNLJoin (plan_expr)
     * |--(some pattern)
     * |--CPhysicalSerialUnionAll (optional)
     * |  |--CPhysicalComputeScalarColumnar
     * |  |  |--CPhysicalIndexScan
     * |  |  |  |--CScalarCmp
     * |  |  |  |  |--CScalarIdent "n._id"
     * |  |  |  |  |--CScalarIdent "_tid"
     * |  |  |--CScalarProjectList
     * |  |  |  |--CScalarProjectElement "n._id"
     * |  |  |  |  |--CScalarIdent "n._id"
    */

    /* UNION ALL Case */
    vector<COperator::EOperatorId> p1 = vector<COperator::EOperatorId>(
        {COperator::EOperatorId::EopPhysicalSerialUnionAll,
         COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
         COperator::EOperatorId::EopPhysicalIndexScan});

    if (pMatchExprPattern(plan_expr->operator[](1), p1, 0, true)) {
        CExpression *compute_scalar_expr =
            plan_expr->operator[](1)->operator[](0);
        CExpression *index_scan_expr = compute_scalar_expr->operator[](0);
        CExpression *cmp_expr = index_scan_expr->operator[](0);
        CExpression *project_list_expr = compute_scalar_expr->operator[](1);

        // Check if two ident's name contains w_id_col_name and w_tid_col_name
        if (cmp_expr->operator[](0)->Pop()->Eopid() ==
                COperator::EOperatorId::EopScalarIdent &&
            cmp_expr->operator[](1)->Pop()->Eopid() ==
                COperator::EOperatorId::EopScalarIdent) {
            CScalarIdent *lhs_ident =
                (CScalarIdent *)cmp_expr->operator[](0)->Pop();
            CScalarIdent *rhs_ident =
                (CScalarIdent *)cmp_expr->operator[](1)->Pop();

            if ((pCmpColName(lhs_ident->Pcr(), w_id_col_name) &&
                 pCmpColName(rhs_ident->Pcr(), w_tid_col_name)) ||
                (pCmpColName(lhs_ident->Pcr(), w_tid_col_name) &&
                 pCmpColName(rhs_ident->Pcr(), w_id_col_name))) {
                // Check project element is one, and is ._id
                if (project_list_expr->Arity() == 1 &&
                    project_list_expr->operator[](0)->Pop()->Eopid() ==
                        COperator::EOperatorId::EopScalarProjectElement) {
                    CScalarProjectElement *project_element =
                        (CScalarProjectElement *)project_list_expr
                            ->
                            operator[](0)
                            ->Pop();
                    if (pCmpColName(project_element->Pcr(), w_id_col_name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /* No UNION ALL Case */
    vector<COperator::EOperatorId> p2 = vector<COperator::EOperatorId>(
        {COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
         COperator::EOperatorId::EopPhysicalIndexScan});

    if (pMatchExprPattern(plan_expr->operator[](1), p2, 0, true)) {
        D_ASSERT(false);
    }

    return false;
}

bool Planner::pCmpColName(const CColRef *colref, const WCHAR *col_name)
{
    if (std::wcslen(colref->Name().Pstr()->GetBuffer()) <
        std::wcslen(col_name)) {
        return false;
    }
    return std::wcsncmp(colref->Name().Pstr()->GetBuffer() +
                            std::wcslen(colref->Name().Pstr()->GetBuffer()) -
                            std::wcslen(col_name),
                        col_name, std::wcslen(col_name)) == 0;
}

void Planner::pGetFilterOnlyInnerColsIdx(CExpression *expr,
                                         CColRefArray *inner_cols,
                                         CColRefArray *output_cols,
                                         vector<ULONG> &inner_cols_idx)
{
    switch (expr->Pop()->Eopid()) {
        case COperator::EopScalarIdent: {
            CScalarIdent *ident = (CScalarIdent *)expr->Pop();
            // check is filter only
            if (inner_cols->IndexOf(ident->Pcr()) != gpos::ulong_max &&
                output_cols->IndexOf(ident->Pcr()) == gpos::ulong_max) {
                inner_cols_idx.push_back(inner_cols->IndexOf(ident->Pcr()));
            }
            break;
        }
        case COperator::EopScalarConst:
        case COperator::EopScalarBoolOp:
        case COperator::EopScalarAggFunc:
        case COperator::EopScalarFunc:
        case COperator::EopScalarCmp:
        case COperator::EopScalarSwitch: {
            for (ULONG child_idx = 0; child_idx < expr->Arity(); child_idx++) {
                pGetFilterOnlyInnerColsIdx(expr->operator[](child_idx),
                                           inner_cols, output_cols,
                                           inner_cols_idx);
            }
            break;
        }
        default:
            GPOS_ASSERT(false);  // NOT implemented yet
    }
}

CExpression *Planner::pFindFilterExpr(CExpression *plan_expr)
{
    if (plan_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        return plan_expr;
    }
    for (ULONG child_idx = 0; child_idx < plan_expr->Arity(); child_idx++) {
        CExpression *result = pFindFilterExpr(plan_expr->operator[](child_idx));
        if (result != NULL) {
            return result;
        }
    }
    return NULL;
}

bool Planner::pIsFilterExist(CExpression *plan_expr)
{
    auto filter_expr = pFindFilterExpr(plan_expr);
    return filter_expr != NULL;
}

bool Planner::pIsEdgePropertyInFilter(CExpression *plan_expr)
{
    auto filter_expr = pFindFilterExpr(plan_expr);
    if (filter_expr == NULL) {
        return false;
    }

    bool is_edge_property_found = false;
    auto filter_pred_expr = filter_expr->operator[](1);

    vector<CExpression *> nodes_to_visit;
    nodes_to_visit.push_back(filter_pred_expr);
    while (!nodes_to_visit.empty()) {
        CExpression *current_node = nodes_to_visit.back();
        nodes_to_visit.pop_back();
        switch (current_node->Pop()->Eopid()) {
            case COperator::EOperatorId::EopScalarIdent: {
                CScalarIdent *ident = (CScalarIdent *)current_node->Pop();
                if (pIsColEdgeProperty(ident->Pcr())) {
                    is_edge_property_found = true;
                    break;
                }
                break;
            }
            default: {
                for (ULONG child_idx = 0; child_idx < current_node->Arity();
                     child_idx++) {
                    nodes_to_visit.push_back(
                        current_node->operator[](child_idx));
                }
                break;
            }
        }
        if (is_edge_property_found) {
            break;
        }
    }

    return is_edge_property_found;
}

bool Planner::pIsPropertyInCols(CColRefArray *cols)
{
    bool is_property_found = false;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        if (pIsColEdgeProperty((*cols)[col_idx])) {
            is_property_found = true;
            break;
        }
    }
    return is_property_found;
}

bool Planner::pIsIDColInCols(CColRefArray *cols)
{
    bool is_edge_id_found = false;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *col = cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)col;
        if (colref_table->AttrNum() == -1) {
            is_edge_id_found = true;
            break;
        }
    }
    return is_edge_id_found;
}

CColRef *Planner::pGetIDColInCols(CColRefArray *cols)
{
    CColRef *id_col = NULL;
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *col = cols->operator[](col_idx);
        CColRefTable *colref_table = (CColRefTable *)col;
        if (colref_table->AttrNum() == -1) {
            id_col = col;
            break;
        }
    }
    return id_col;
}

size_t Planner::pGetNumOuterSchemas()
{
    auto &num_schemas_of_childs_prev = num_schemas_of_childs.back();
    size_t num_outer_schemas = 1;
    for (auto i = 0; i < num_schemas_of_childs_prev.size(); i++) {
        num_outer_schemas *= num_schemas_of_childs_prev[i];
    }
    return num_outer_schemas;
}

CExpression *Planner::pFindIndexScanExpr(CExpression *plan_expr)
{
    if (plan_expr->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexScan ||
        plan_expr->Pop()->Eopid() ==
            COperator::EOperatorId::EopPhysicalIndexOnlyScan) {
        return plan_expr;
    }
    for (ULONG child_idx = 0; child_idx < plan_expr->Arity(); child_idx++) {
        CExpression *result =
            pFindIndexScanExpr(plan_expr->operator[](child_idx));
        if (result != NULL) {
            return result;
        }
    }
    return NULL;
}

duckdb::idx_t Planner::pGetColIndexInPred(CExpression *pred,
                                          CColRefArray *colrefs)
{
    duckdb::idx_t idx = gpos::ulong_max;
    D_ASSERT(pred->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp);
    for (ULONG child_idx = 0; child_idx < pred->Arity(); child_idx++) {
        CExpression *child = pred->operator[](child_idx);
        if (child->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent) {
            CScalarIdent *ident = (CScalarIdent *)child->Pop();
            idx = colrefs->IndexOf(ident->Pcr());
            if (idx != gpos::ulong_max) {
                break;
            }
        }
    }
    return idx;
}

void Planner::pGetDuckDBTypesFromColRefs(CColRefArray *colrefs,
                                         vector<duckdb::LogicalType> &out_types)
{
    for (ULONG col_idx = 0; col_idx < colrefs->Size(); col_idx++) {
        CColRef *colref = colrefs->operator[](col_idx);
        CMDIdGPDB *type_mdid =
            CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId());
        OID type_oid = type_mdid->Oid();
        INT type_mod = colref->TypeModifier();
        out_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
    }
}

void Planner::pGetObjetIdsForColRefs(CColRefArray *cols,
                                     vector<uint64_t> &out_oids)
{
    // TODO: This function is quite weird, since only returns the first value
    // Check jhko's intended behavior
    for (ULONG col_idx = 0; col_idx < cols->Size(); col_idx++) {
        CColRef *colref = cols->operator[](col_idx);
        OID table_obj_id =
            CMDIdGPDB::CastMdid(((CColRefTable *)colref)->GetMdidTable())
                ->Oid();
        if (col_idx == 0) {
            out_oids.push_back((uint64_t)table_obj_id);
        }
    }
}

void Planner::pPushCartesianProductSchema(
    duckdb::Schema &out_schema, vector<duckdb::LogicalType> &rhs_types)
{
    duckdb::Schema rhs_schema;
    rhs_schema.setStoredTypes(rhs_types);
    vector<duckdb::Schema> prev_local_schemas = pipeline_schemas.back();
    pipeline_operator_types.push_back(duckdb::OperatorType::BINARY);
    num_schemas_of_childs.push_back({pGetNumOuterSchemas(), 1});
    vector<duckdb::Schema> out_schemas;
    vector<duckdb::Schema> rhs_schemas = {rhs_schema};
    pGenerateCartesianProductSchema(prev_local_schemas, rhs_schemas,
                                    out_schemas);
    pipeline_schemas.push_back(out_schemas);
    pipeline_union_schema.push_back(out_schema);
}

void Planner::pConstructColMapping(CColRefArray *in_cols,
                                   CColRefArray *out_cols,
                                   vector<uint32_t> &out_mapping)
{
    for (ULONG col_idx = 0; col_idx < in_cols->Size(); col_idx++) {
        CColRef *col = in_cols->operator[](col_idx);
        auto idx = out_cols->IndexOf(col);
        if (idx != gpos::ulong_max) {
            out_mapping.push_back(idx);
        }
        else {
            out_mapping.push_back(std::numeric_limits<uint32_t>::max());
        }
    }
}

void Planner::pAppendFilterOnlyCols(CExpression *filter_expr,
                                    CColRefArray *input_cols,
                                    CColRefArray *output_cols,
                                    CColRefArray *result_cols)
{
    D_ASSERT(filter_expr->Pop()->Eopid() ==
             COperator::EOperatorId::EopPhysicalFilter);
    vector<ULONG> filter_only_cols_idx;
    auto filter_pred_expr = filter_expr->operator[](1);
    pGetFilterOnlyInnerColsIdx(
        filter_pred_expr, input_cols /* all scanned cols */,
        output_cols /* output inner cols */, filter_only_cols_idx);
    for (auto col_idx : filter_only_cols_idx) {
        result_cols->Append(input_cols->operator[](col_idx));
    }
}

void Planner::pGetFilterDuckDBExprs(
    CExpression *filter_or_pred_expr, CColRefArray *outer_cols,
    CColRefArray *inner_cols, size_t index_shifting_size,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    CExpression *filter_pred_expr;
    if (filter_or_pred_expr->Pop()->Eopid() ==
        COperator::EOperatorId::EopPhysicalFilter) {
        filter_pred_expr = filter_or_pred_expr->operator[](1);
    }
    else if (filter_or_pred_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarBoolOp ||
             filter_or_pred_expr->Pop()->Eopid() ==
                 COperator::EOperatorId::EopScalarCmp) {
        filter_pred_expr = filter_or_pred_expr;
    }
    unique_ptr<duckdb::Expression> filter_duckdb_expr;
    filter_duckdb_expr =
        pTransformScalarExpr(filter_pred_expr, outer_cols, inner_cols);
    if (index_shifting_size > 0)
        pShiftFilterPredInnerColumnIndices(filter_duckdb_expr,
                                           index_shifting_size);
    out_exprs.push_back(std::move(filter_duckdb_expr));
}

void Planner::pSeperatePropertyNonPropertyCols(CColRefArray *input_cols,
                                               CColRefArray *property_cols,
                                               CColRefArray *non_property_cols)
{
    for (ULONG col_idx = 0; col_idx < input_cols->Size(); col_idx++) {
        CColRef *col = input_cols->operator[](col_idx);
        if (!pIsColEdgeProperty(col)) {
            non_property_cols->Append(col);
        }
        else {
            property_cols->Append(col);
        }
    }
}

void Planner::pGetProjectionExprs(
    CColRefArray *input_cols, CColRefArray *output_cols,
    vector<duckdb::LogicalType> output_types,
    vector<unique_ptr<duckdb::Expression>> &out_exprs)
{
    for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
        CColRef *col = (*output_cols)[col_idx];
        ULONG idx = input_cols->IndexOf(col);
        D_ASSERT(idx != gpos::ulong_max);
        out_exprs.push_back(make_unique<duckdb::BoundReferenceExpression>(
            output_types[col_idx], (int)idx));
    }
}

}  // namespace s62
