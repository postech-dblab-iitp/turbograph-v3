#include "planner.hpp"


#include <string>
#include <limits>
#include <bits/stdc++.h>

#include "kuzu/common/expression_type.h"


namespace s62 {

CExpression *Planner::lExprScalarExpression(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
	// if expression appears in previous plan, generate ScalarIdent expression	
	CExpression *expr = lTryGenerateScalarIdent(expression, prev_plan);
	if (expr != NULL) { /* found */ return expr; }

	auto expr_type = expression->expressionType;
	switch(expr_type) {
	case OR:
	case XOR:
	case AND:
	case NOT:
		return lExprScalarBoolOp(expression, prev_plan, required_type);
	case EQUALS:
	case NOT_EQUALS:
	case GREATER_THAN:
    case GREATER_THAN_EQUALS:
	case LESS_THAN:
	case LESS_THAN_EQUALS:
		return lExprScalarComparisonExpr(expression, prev_plan, required_type);
	case PROPERTY:
		return lExprScalarPropertyExpr(expression, prev_plan, required_type);
	case LITERAL:
		return lExprScalarLiteralExpr(expression, prev_plan, required_type);
	case AGGREGATE_FUNCTION:
		return lExprScalarAggFuncExpr(expression, prev_plan, required_type);
	case IS_NULL:
	case IS_NOT_NULL:
		return lExprScalarNullOp(expression, prev_plan, required_type);
	case FUNCTION:
		return lExprScalarFuncExpr(expression, prev_plan, required_type);
	case CASE_ELSE:
		return lExprScalarCaseElseExpr(expression, prev_plan, required_type);
	case EXISTENTIAL_SUBQUERY:
		return lExprScalarExistentialSubqueryExpr(expression, prev_plan, required_type);
	case PARAMETER:
		return lExprScalarParamExpr(expression, prev_plan, required_type);
	case LIST_COMPREHENSION:
		return lExprScalarListComprehensionExpr(expression, prev_plan, required_type);
	case PATTERN_COMPREHENSION:
		// return lExprScalarPatternComprehensionExpr(expression, prev_plan, required_type);
	case FILTER:
		return lExprScalarFilterExpr(expression, prev_plan, required_type);
	case ID_IN_COLL:
		return lExprScalarIdInCollExpr(expression, prev_plan, required_type);
	case SHORTEST_PATH:
		return lExprScalarShortestPathExpr(expression, prev_plan, required_type);
	default:
		D_ASSERT(false);
	}
}

CExpression *Planner::lExprScalarBoolOp(kuzu::binder::Expression* expression, LogicalPlan* prev_plan, DataTypeID required_type) {

	CMemoryPool* mp = this->memory_pool;
	ScalarFunctionExpression* bool_expr = (ScalarFunctionExpression*) expression;
	auto children = bool_expr->getChildren();

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	vector<CExpression*> child_exprs;
	for(int idx = 0; idx < children.size(); idx++) {
		child_exprs.push_back(lExprScalarExpression(children[idx].get(), prev_plan, required_type));
	}

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for(auto* child: child_exprs) {
		pdrgpexprChildren->Append(child);
	}

	// PexprScalarBoolOp
	CScalarBoolOp::EBoolOperator op_type;
	switch(bool_expr->expressionType) {
		case ExpressionType::NOT:
			op_type = CScalarBoolOp::EBoolOperator::EboolopNot; break;
		case ExpressionType::AND:
			op_type = CScalarBoolOp::EBoolOperator::EboolopAnd; break;
		case ExpressionType::OR:
			op_type = CScalarBoolOp::EBoolOperator::EboolopOr; break;
		default:
			D_ASSERT(false);
	}

	return CUtils::PexprScalarBoolOp(mp, op_type, pdrgpexprChildren);

}

CExpression *Planner::lExprScalarNullOp(kuzu::binder::Expression* expression, LogicalPlan* prev_plan, DataTypeID required_type) {

	CMemoryPool* mp = this->memory_pool;
	ScalarFunctionExpression* bool_expr = (ScalarFunctionExpression*) expression;
	auto children = bool_expr->getChildren();

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CExpression* child_expr =  lExprScalarExpression(children[0].get(), prev_plan, required_type);

	// PexprScalarBoolOp
	CScalarBoolOp::EBoolOperator op_type;
	switch(bool_expr->expressionType) {
		case ExpressionType::IS_NULL:
			return CUtils::PexprIsNull(mp, child_expr);
		case ExpressionType::IS_NOT_NULL:
			return CUtils::PexprIsNotNull(mp, child_expr);
		default:
			D_ASSERT(false);
	}
}

CExpression *Planner::lExprScalarComparisonExpr(kuzu::binder::Expression* expression, LogicalPlan* prev_plan, DataTypeID required_type) {

	CMemoryPool *mp = this->memory_pool;
	ScalarFunctionExpression* comp_expr = (ScalarFunctionExpression*) expression;
	D_ASSERT( comp_expr->getNumChildren() == 2);	// S62 not sure how kuzu generates comparison expression, now assume 2
	auto children = comp_expr->getChildren();
	// lhs, rhs
	CExpression *lhs_scalar_expr;
	CExpression *rhs_scalar_expr;
    if (children[0]->expressionType == ExpressionType::LITERAL &&
        children[1]->expressionType != ExpressionType::LITERAL) {
        rhs_scalar_expr =
            lExprScalarExpression(children[1].get(), prev_plan, required_type);
        DataTypeID target_type_id =
            (DataTypeID)(((CMDIdGPDB *)(CScalar::PopConvert(
                                            rhs_scalar_expr->Pop())
                                            ->MdidType()))
                             ->Oid() -
                         LOGICAL_TYPE_BASE_ID);
        lhs_scalar_expr =
            lExprScalarExpression(children[0].get(), prev_plan, target_type_id);
    }
    else if (children[0]->expressionType != ExpressionType::LITERAL &&
             children[1]->expressionType == ExpressionType::LITERAL) {
        lhs_scalar_expr =
            lExprScalarExpression(children[0].get(), prev_plan, required_type);
        DataTypeID target_type_id =
            (DataTypeID)(((CMDIdGPDB *)(CScalar::PopConvert(
                                            lhs_scalar_expr->Pop())
                                            ->MdidType()))
                             ->Oid() -
                         LOGICAL_TYPE_BASE_ID);
        rhs_scalar_expr =
            lExprScalarExpression(children[1].get(), prev_plan, target_type_id);
    }
    else {
        lhs_scalar_expr =
            lExprScalarExpression(children[0].get(), prev_plan, required_type);
        rhs_scalar_expr =
            lExprScalarExpression(children[1].get(), prev_plan, required_type);
    }

    CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// map comparison type
	IMDType::ECmpType cmp_type;
	if (lhs_scalar_expr->Pop()->Eopid() == COperator::EopScalarConst &&
        rhs_scalar_expr->Pop()->Eopid() == COperator::EopScalarIdent) {
        // swap
        CExpression *tmp = lhs_scalar_expr;
        lhs_scalar_expr = rhs_scalar_expr;
        rhs_scalar_expr = tmp;

		// reverse comp type
		switch (comp_expr->expressionType) {
			case ExpressionType::EQUALS: 
				cmp_type = IMDType::ECmpType::EcmptEq; break;
			case ExpressionType::NOT_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptNEq; break;
			case ExpressionType::GREATER_THAN:
				cmp_type = IMDType::ECmpType::EcmptL; break;
			case ExpressionType::GREATER_THAN_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptLEq; break;
			case ExpressionType::LESS_THAN:
				cmp_type = IMDType::ECmpType::EcmptG; break;
			case ExpressionType::LESS_THAN_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptGEq; break;
			default:
				D_ASSERT(false);
		}
    } else {
		switch (comp_expr->expressionType) {
			case ExpressionType::EQUALS: 
				cmp_type = IMDType::ECmpType::EcmptEq; break;
			case ExpressionType::NOT_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptNEq; break;
			case ExpressionType::GREATER_THAN:
				cmp_type = IMDType::ECmpType::EcmptG; break;
			case ExpressionType::GREATER_THAN_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptGEq; break;
			case ExpressionType::LESS_THAN:
				cmp_type = IMDType::ECmpType::EcmptL; break;
			case ExpressionType::LESS_THAN_EQUALS:
				cmp_type = IMDType::ECmpType::EcmptLEq; break;
			default:
				D_ASSERT(false);
		}
	}

	IMDId *left_mdid = CScalar::PopConvert(lhs_scalar_expr->Pop())->MdidType();
	IMDId *right_mdid = CScalar::PopConvert(rhs_scalar_expr->Pop())->MdidType();

	IMDId* func_mdid = 
		CMDAccessorUtils::GetScCmpMdIdConsiderCasts(md_accessor, left_mdid, right_mdid, cmp_type);	// test if function exists
	D_ASSERT(func_mdid != NULL);	// function must be found // TODO need to raise exception

	return CUtils::PexprScalarCmp(mp, lhs_scalar_expr, rhs_scalar_expr, cmp_type);
	
	// return corresponding expression
}

CExpression* Planner::lExprScalarCmpEq(CExpression* left_expr, CExpression* right_expr) {

	CMemoryPool* mp = this->memory_pool;
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDType::ECmpType cmp_type = IMDType::ECmpType::EcmptEq;

	IMDId *left_mdid = CScalar::PopConvert(left_expr->Pop())->MdidType();
	IMDId *right_mdid = CScalar::PopConvert(right_expr->Pop())->MdidType();
	IMDId* func_mdid = 
		CMDAccessorUtils::GetScCmpMdIdConsiderCasts(md_accessor, left_mdid, right_mdid, cmp_type);
	D_ASSERT(func_mdid != NULL);	// function must be found // TODO need to raise exception

	return CUtils::PexprScalarCmp(mp, left_expr, right_expr, cmp_type);
}

CExpression *Planner::lTryGenerateScalarIdent(kuzu::binder::Expression* expression, LogicalPlan* prev_plan) {

	// used to handle already processed kuzu expression as ScalarIdent.
	// if this function did not exist, our compiler will try evaluating expressions again and again

	// normal column
	CMemoryPool* mp = this->memory_pool;
	CColRef* target_colref;

	target_colref = prev_plan->getSchema()->getColRefOfKey(expression->getUniqueName(), "");
	if(target_colref == NULL) {
		target_colref = prev_plan->getSchema()->getColRefOfKey(expression->getAlias(), "");
	}

	if((target_colref == NULL) && l_is_outer_plan_registered) {
		D_ASSERT(l_registered_outer_plan != nullptr);
		target_colref = l_registered_outer_plan->getSchema()->getColRefOfKey(expression->getUniqueName(), "");
		if(target_colref == NULL) {
			target_colref = l_registered_outer_plan->getSchema()->getColRefOfKey(expression->getAlias(), "");
		}
	}

	// if target not found, then pass
	if( target_colref == NULL) {
		return NULL;
	}
	GPOS_ASSERT(target_colref != NULL);

	// record alias to mapping
	if( expression->hasAlias() ) {
		property_col_to_output_col_names_mapping[target_colref] = expression->getAlias();
	}

	CExpression* ident_expr = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
	return ident_expr;
}

CExpression *Planner::lExprScalarPropertyExpr(
    kuzu::binder::Expression *expression, LogicalPlan *prev_plan,
    DataTypeID required_type)
{
    CMemoryPool *mp = this->memory_pool;

    PropertyExpression *prop_expr = (PropertyExpression *)expression;
    string k1 = "";
    string k2 = "";

    CColRef *target_colref;

    // try first with property
    k1 = prop_expr->getVariableName();
    k2 = prop_expr->getPropertyName();
    target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);

    // fallback to alias
    if (target_colref == NULL && prop_expr->hasAlias()) {
        k1 = prop_expr->getAlias();
        k2 = "";
        target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
    }

    // fallback to outer
    if (target_colref == NULL && l_is_outer_plan_registered) {
        GPOS_ASSERT(l_registered_outer_plan != nullptr);
        k1 = prop_expr->getVariableName();
        k2 = prop_expr->getPropertyName();
        target_colref =
            l_registered_outer_plan->getSchema()->getColRefOfKey(k1, k2);

        // fallback to alias
        if (target_colref == NULL && prop_expr->hasAlias()) {
            k1 = prop_expr->getAlias();
            k2 = "";
            target_colref =
                l_registered_outer_plan->getSchema()->getColRefOfKey(k1, k2);
        }
    }
    if (target_colref == NULL) {
        GPOS_ASSERT(target_colref != NULL);
    }

    // record alias to mapping
    if (prop_expr->hasAlias()) {
        property_col_to_output_col_names_mapping[target_colref] =
            prop_expr->getAlias();
    }

    CExpression *ident_expr = GPOS_NEW(mp)
        CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));

    return ident_expr;
}

CExpression *Planner::lExprScalarPropertyExpr(string k1, string k2, LogicalPlan *prev_plan) {

	CMemoryPool *mp = this->memory_pool;

	CColRef *target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
	if (target_colref == NULL) {
		std::cout << k1 << ", " << k2 << std::endl;
	}
	GPOS_ASSERT(target_colref != NULL);

	CExpression *ident_expr = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
	
	return ident_expr;
}

CExpression *Planner::lExprScalarLiteralExpr(
    kuzu::binder::Expression *expression, LogicalPlan *prev_plan,
    DataTypeID required_type)
{

    CMemoryPool *mp = this->memory_pool;

    LiteralExpression *lit_expr = (LiteralExpression *)expression;
    DataType type;
    if (required_type == DataTypeID::INVALID) {
        type = lit_expr->literal.get()->dataType;
    }
    else {
		// TODO we need to check if literal type can be casted to required type
        type = DataType(required_type);
    }

    CExpression *pexpr = nullptr;
    uint32_t literal_type_id = LOGICAL_TYPE_BASE_ID + (OID)type.typeID;
    INT type_modifier = lit_expr->literal->dataType.typeID == DataTypeID::LIST
                            ? lit_expr->literal->dataType.childType->typeID
                            : -1;
    CMDIdGPDB *type_mdid =
        GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, literal_type_id, 1, 0);
    type_mdid->AddRef();

    void *serialized_literal = NULL;
    uint64_t serialized_literal_length = 0;
    if (!lit_expr->isNull()) {
        DatumSerDes::SerializeKUZULiteralIntoOrcaByteArray(
            literal_type_id, lit_expr->literal.get(), serialized_literal,
            serialized_literal_length);
        D_ASSERT(serialized_literal != NULL && serialized_literal_length != 0);
    }

	LINT lint_val = 0;
	CDouble double_val = 0.0;
	// this should be aligned with CMDTypeGenericGPDB::HasByte2IntMapping
	if (type.typeID == DataTypeID::NODE_ID) {
		lint_val = lit_expr->literal.get()->val.uint64Val;
	} else if (type.typeID == DataTypeID::UBIGINT) {
		lint_val = lit_expr->literal.get()->val.uint64Val;
	} else if (type.typeID == DataTypeID::INT64) {
		lint_val = lit_expr->literal.get()->val.int64Val;
	}
	
	// this should be aligned with CMDTypeGenericGPDB::HasByte2DoubleMapping
	if (type.typeID == DataTypeID::DOUBLE) {
		double_val = lit_expr->literal.get()->val.doubleVal;
	}
    IDatumGeneric *datum = (IDatumGeneric *)(GPOS_NEW(mp) CDatumGenericGPDB(
        mp, (IMDId *)type_mdid, type_modifier, serialized_literal,
        serialized_literal_length, lit_expr->isNull(), lint_val, double_val));
    datum->AddRef();
    pexpr = GPOS_NEW(mp)
        CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *)datum));
    pexpr->AddRef();

    D_ASSERT(pexpr != nullptr);
    return pexpr;
}

CExpression *Planner::lExprScalarAggFuncExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
	CMemoryPool* mp = this->memory_pool;
	
	AggregateFunctionExpression* aggfunc_expr = (AggregateFunctionExpression*) expression;
	kuzu::binder::expression_vector children = aggfunc_expr->getChildren();

	std::string func_name = (aggfunc_expr)->getRawFuncName();
	std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);	// to lower case
	D_ASSERT(func_name != "");

	// refer expression_type.h
	bool child_exists = children.size() > 0;
	CColRef *child_colref = nullptr;
	D_ASSERT(children.size()<=1); 	// not sure yet

	CExpressionArray *child_exprs = GPOS_NEW(mp) CExpressionArray(mp);
	vector<duckdb::LogicalType> child_types;
	for (auto i = 0; i < children.size(); i++) {
		CExpression *child_expr = lExprScalarExpression(children[i].get(), prev_plan, required_type);
		child_exprs->Append(child_expr);

		OID type_oid = pGetTypeIdFromScalar(child_expr);
		INT type_mod = pGetTypeModFromScalar(child_expr);
		child_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
	}
	// refer expression_type.h for kuzu function names
	duckdb::idx_t func_mdid_id = context->db->GetCatalogWrapper().GetAggFuncMdId(*context, func_name, child_types);
	// no assert?

	IMDId* func_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
	func_mdid->AddRef();
	const IMDAggregate *pmdagg = lGetMDAccessor()->RetrieveAgg(func_mdid);
	IMDId *agg_mdid = pmdagg->MDId();
	agg_mdid->AddRef();
	CWStringConst *str = GPOS_NEW(mp)
		CWStringConst(mp, pmdagg->Mdname().GetMDName()->GetBuffer());

	/**
	 * This code identifies the return type for a function in duckdb. 
	 * The original implementation inaccurately determines the return type, 
	 * as it relies on the physical planner to generate the duckdb type without full information,
	 * since the translation from kuzu to Orca types leads to incomplete typing information in Orca.
	 * For instance, the return type for hash aggregation is determined using BindDecimalMultiply in
	 * physical planning, but during Orca processing, Orca does not about this type info.
	*/
	duckdb::AggregateFunctionCatalogEntry *agg_func_cat;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetAggFuncAndIdx(*context, func_mdid_id, agg_func_cat, function_idx);
	auto function = agg_func_cat->functions.get()->functions[function_idx];	
	vector<unique_ptr<duckdb::Expression>> duckdb_childs;
	for (auto i = 0; i < children.size(); i++) {
		duckdb_childs.push_back(move(lExprScalarExpressionDuckDB(children[i].get())));
	}
	if (function.bind) {
		auto bind_info = function.bind(*context, function, duckdb_childs);
	}
	INT type_mod = lGetTypeModFromType(function.return_type);

	CScalarAggFunc *popScAggFunc = CUtils::PopAggFunc(mp, agg_mdid, type_mod, str, aggfunc_expr->isDistinct() /*is_distinct*/,
				EaggfuncstageGlobal /*eaggfuncstage*/, false /*fSplit*/, NULL, EaggfunckindNormal);
	// CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp); // empty child
	CExpression *pexpr = GPOS_NEW(mp)
		CExpression(mp, popScAggFunc, CUtils::PexprAggFuncArgs(mp, child_exprs));
	pexpr->AddRef();
	return pexpr;
}

CExpression *Planner::lExprScalarFuncExpr(Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
	CMemoryPool *mp = this->memory_pool;
	
	ScalarFunctionExpression *scalarfunc_expr = (ScalarFunctionExpression *)expression;
	kuzu::binder::expression_vector children = scalarfunc_expr->getChildren();

	std::string func_name = (scalarfunc_expr)->getRawFuncName();
	if (lIsCastingFunction(func_name)) { return lExprScalarCastExpr(expression, prev_plan); }
	std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);	// to lower case
	D_ASSERT(func_name != "");

	// refer expression_type.h
	bool child_exists = children.size() > 0;
	CExpressionArray *child_exprs = GPOS_NEW(mp) CExpressionArray(mp);
	vector<duckdb::LogicalType> child_types;
	for (auto i = 0; i < children.size(); i++) {
		CExpression *child_expr = lExprScalarExpression(children[i].get(), prev_plan, required_type);
		child_exprs->Append(child_expr);

		OID type_oid = pGetTypeIdFromScalar(child_expr);
		INT type_mod = pGetTypeModFromScalar(child_expr);
		child_types.push_back(pConvertTypeOidToLogicalType(type_oid, type_mod));
	}

	// refer expression_type.h for kuzu function names
	duckdb::idx_t func_mdid_id = context->db->GetCatalogWrapper().GetScalarFuncMdId(*context, func_name, child_types);

	IMDId* func_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
	func_mdid->AddRef();
	const IMDFunction *pmdscalar = lGetMDAccessor()->RetrieveFunc(func_mdid);
	IMDId *scalarfunc_mdid = pmdscalar->MDId();
	scalarfunc_mdid->AddRef();
	CWStringConst *str = GPOS_NEW(mp)
		CWStringConst(mp, pmdscalar->Mdname().GetMDName()->GetBuffer());
	IMDId *mdid_return_type = pmdscalar->GetResultTypeMdid();

	// Get type mode
	duckdb::ScalarFunctionCatalogEntry *func_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetScalarFuncAndIdx(*context, func_mdid_id, func_catalog_entry, function_idx);
	auto function = func_catalog_entry->functions.get()->functions[function_idx];
	vector<unique_ptr<duckdb::Expression>> duckdb_childs;
	for (auto i = 0; i < children.size(); i++) {
		duckdb_childs.push_back(move(lExprScalarExpressionDuckDB(children[i].get())));
	}
	if (function.bind) {
		auto bind_info = function.bind(*context, function, duckdb_childs);
	}
	INT type_mod = lGetTypeModFromType(function.return_type);

	COperator *pop = GPOS_NEW(mp) CScalarFunc(
			mp, scalarfunc_mdid, mdid_return_type, type_mod, str);
	CExpression *pexpr;

	if (child_exists) {
		pexpr = GPOS_NEW(mp)
			CExpression(mp, pop, child_exprs);
	} else {
		pexpr = GPOS_NEW(mp)
			CExpression(mp, pop);
	}
	pexpr->AddRef();
	return pexpr;
}

CExpression *Planner::lExprScalarCaseElseExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
	CMemoryPool* mp = this->memory_pool;

	CaseExpression *case_expr = (CaseExpression *)expression;

	size_t numCaseAlts = case_expr->getNumCaseAlternatives();
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	// get return data type
	DataType return_type = case_expr->getDataType();
	uint32_t return_type_id = LOGICAL_TYPE_BASE_ID + (OID)return_type.typeID;
	CMDIdGPDB* return_type_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, return_type_id, 1, 0);
	return_type_mdid->AddRef();

	// generate expression for CaseAlternatives
	for (size_t idx = 0; idx < numCaseAlts; idx++) {
		auto casealt_expr = case_expr->getCaseAlternative(idx);
		auto when_expr = casealt_expr->whenExpression;
		auto then_expr = casealt_expr->thenExpression;

		CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

		// convert when & then expr
		CExpression *c_when_expr = lExprScalarExpression(when_expr.get(), prev_plan);
		CExpression *c_then_expr = lExprScalarExpression(then_expr.get(), prev_plan, return_type.typeID);
		
		pdrgpexprChildren->Append(c_when_expr);
		pdrgpexprChildren->Append(c_then_expr);

		CExpression *c_casealt_expr  = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarSwitchCase(mp), pdrgpexprChildren);
		pdrgpexpr->Append(c_casealt_expr);
	}

	// generate expression for Else
	auto else_expr = case_expr->getElseExpression();
	CExpression *else_expr_tmp = lExprScalarExpression(else_expr.get(), prev_plan, return_type.typeID);
	pdrgpexpr->Append(else_expr_tmp);

	CExpression *result_expr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarSwitch(mp, return_type_mdid), pdrgpexpr);

	return result_expr;
}

CExpression *Planner::lExprScalarExistentialSubqueryExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {

	CMemoryPool* mp = this->memory_pool;

	ExistentialSubqueryExpression *subquery_expr = (ExistentialSubqueryExpression *)expression;

	auto queryGraphCollection = subquery_expr->getQueryGraphCollection();
    expression_vector predicates = subquery_expr->hasWhereExpression() ?	
                          subquery_expr->getWhereExpression()->splitOnAND() :	// CNF form
                          expression_vector{};

	// call match - always correlated existential
	LogicalPlan* inner_plan = lPlanRegularMatchFromSubquery(*queryGraphCollection, prev_plan /* outer plan*/);
	// TODO edge isomorphism?

	// call selection; but now allow access of outer query
	l_is_outer_plan_registered = true;
	l_registered_outer_plan = prev_plan;
	if( predicates.size() > 0 ) {
		inner_plan = lPlanSelection(std::move(predicates), inner_plan);
	}
	l_registered_outer_plan = nullptr;
	l_is_outer_plan_registered = false;

	//generate subquery expression
	auto pexprSubqueryExistential = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarSubqueryExists(mp), inner_plan->getPlanExpr());
	
	return pexprSubqueryExistential;
}

CExpression *Planner::lExprScalarCastExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan) {
	CMemoryPool* mp = this->memory_pool;
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	ScalarFunctionExpression *scalarfunc_expr = (ScalarFunctionExpression *)expression;
	
	// Get child expr
	kuzu::binder::expression_vector children = scalarfunc_expr->getChildren();
	D_ASSERT(children.size() == 1);
	CExpression* child_expr = lExprScalarExpression(children[0].get(), prev_plan);

	// Get child type mdid
	DataType child_type = children[0]->getDataType();
	uint32_t child_type_id = LOGICAL_TYPE_BASE_ID + (OID)child_type.typeID;
	CMDIdGPDB* child_type_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, child_type_id, 1, 0);
	child_type_mdid->AddRef();

	// Get return type mdid
	DataType return_type = scalarfunc_expr->getDataType();
	uint32_t return_type_id = LOGICAL_TYPE_BASE_ID + (OID)return_type.typeID;
	CMDIdGPDB* return_type_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, return_type_id, 1, 0);
	return_type_mdid->AddRef();

	// Get function mdid
	auto cast_func = md_accessor->Pmdcast((IMDId*) child_type_mdid, (IMDId*) return_type_mdid);
	IMDId* cast_func_mdid = cast_func->GetCastFuncMdId();

	// Generate cast expression
	CExpression *result_expr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarCast(mp, (IMDId*) return_type_mdid, cast_func_mdid, false), child_expr);

	return result_expr;
}

CExpression *Planner::lExprScalarParamExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
	ParameterExpression *param_expr = (ParameterExpression *)expression;
	auto param_data_type = param_expr->getLiteral()->dataType;
	auto param_data_type_id = param_data_type.typeID;
	auto default_literal_exp = make_shared<LiteralExpression>(param_data_type, make_unique<Literal>(0));
	return lExprScalarLiteralExpr(default_literal_exp.get(), prev_plan, required_type);
}

CExpression *Planner::lExprScalarListComprehensionExpr(
    kuzu::binder::Expression *expression, LogicalPlan *prev_plan,
    DataTypeID required_type)
{
    CMemoryPool *mp = this->memory_pool;
    ListComprehensionExpression *list_comp_expr =
        (ListComprehensionExpression *)expression;

    CExpression *filter_expr = lExprScalarFilterExpr(
        list_comp_expr->getFilterExpression().get(), prev_plan, required_type);
	
	if (!list_comp_expr->getExpr()) {
		throw NotImplementedException("List comprehension expression not implemented");
	}

	return filter_expr;
}

CExpression *Planner::lExprScalarPatternComprehensionExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type)
{
	D_ASSERT(false);
}

CExpression *Planner::lExprScalarFilterExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type)
{
	CMemoryPool *mp = this->memory_pool;
    FilterExpression *filter_expr =
        (FilterExpression *)expression;
	
	CExpression *id_in_coll_expr = lExprScalarIdInCollExpr(
		filter_expr->getIdInCollExpression().get(), prev_plan, required_type);
	
	if (!filter_expr->getWhereExpression()) {
		throw NotImplementedException("Filter expression not implemented");
	}

	return id_in_coll_expr;
}

CExpression *Planner::lExprScalarIdInCollExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type)
{
	CMemoryPool *mp = this->memory_pool;
    IdInCollExpression *id_in_coll_expr =
        (IdInCollExpression *)expression;
	
	CExpression *coll_expr = lExprScalarExpression(
		id_in_coll_expr->getExpr().get(), prev_plan, required_type);
	
	string var_name = id_in_coll_expr->getVariableName();
	// OID type_oid = pGetTypeIdFromScalar(coll_expr);
	// INT type_mod = pGetTypeModFromScalar(coll_expr);

	std::wstring w_col_name = L"";
	// w_col_name.assign(col_name.begin(), col_name.end());
	// const CWStringConst col_name_str(w_col_name.c_str());
	// CName col_cname(&col_name_str);
	// CColRef *new_colref = col_factory->PcrCreate(
	// 	lGetMDAccessor()->RetrieveType(scalar_op->MdidType()),
	// 	scalar_op->TypeModifier(), col_cname);
	auto target_colref = prev_plan->getSchema()->getColRefOfKey(var_name, "");
	D_ASSERT(target_colref != NULL);

	CExpression *ident_expr = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
    return ident_expr;
}

CExpression *Planner::lExprScalarShortestPathExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type) {
    CMemoryPool *mp = this->memory_pool;
	PathExpression *path_expr = (PathExpression *)expression;
	string k1 = path_expr->getUniqueName();
    auto target_colref = prev_plan->getSchema()->getColRefOfKey(k1, "");
	D_ASSERT(target_colref != NULL);
	CExpression *ident_expr = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
    return ident_expr;
}

bool Planner::lIsCastingFunction(std::string &func_name)
{
    if (func_name == CAST_TO_DOUBLE_FUNC_NAME ||
        func_name == CAST_TO_FLOAT_FUNC_NAME ||
        func_name == CAST_TO_INT64_FUNC_NAME) {
        return true;
    }
    else {
        return false;
    }
}

INT Planner::lGetTypeModFromType(duckdb::LogicalType type) {
	INT mod = 0;
	if (type.id() == duckdb::LogicalTypeId::DECIMAL) {
		uint16_t width_scale = duckdb::DecimalType::GetWidth(type);
		width_scale = width_scale << 8 | duckdb::DecimalType::GetScale(type);
		mod = width_scale;
	} else if (type.id() == duckdb::LogicalTypeId::LIST) {
		// TODO we cannot handle cases when nesting lv >= 4 yet
		if (duckdb::ListType::GetChildType(type).id() == duckdb::LogicalTypeId::LIST) {
			INT child_mod = lGetTypeModFromType(duckdb::ListType::GetChildType(type));
			mod = (INT)duckdb::LogicalTypeId::LIST | child_mod << 8;
		} else {
			// TODO we do not consider LIST(DECIMAL) yet
			mod = (INT)duckdb::ListType::GetChildType(type).id();
		}
	}
	return mod;
}

}