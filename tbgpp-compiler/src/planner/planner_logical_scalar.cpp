#include "planner.hpp"


#include <string>
#include <limits>
#include<bits/stdc++.h>


#include "kuzu/common/expression_type.h"


namespace s62 {

CExpression* Planner::lExprScalarExpression(Expression* expression, LogicalPlan* prev_plan) {

	auto expr_type = expression->expressionType;
	if( isExpressionComparison(expr_type) ) {
		return lExprScalarComparisonExpr(expression, prev_plan);
	} else if( PROPERTY == expr_type) {
		return lExprScalarPropertyExpr(expression, prev_plan);	// property access need to access previous plan
	} else if ( isExpressionLiteral(expr_type) ) {
		return lExprScalarLiteralExpr(expression, prev_plan);
	} else if ( isExpressionAggregate(expr_type) ) {			// must first check aggfunc over func
		return lExprScalarAggFuncExpr(expression, prev_plan);
	} else if (isExpressionCaseElse(expr_type)) {
		return lExprScalarCaseElseExpr(expression, prev_plan);
	} else {
		D_ASSERT(false);	// TODO Not yet
	}
}

CExpression* Planner::lExprScalarComparisonExpr(Expression* expression, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;
	ScalarFunctionExpression* comp_expr = (ScalarFunctionExpression*) expression;
	D_ASSERT( comp_expr->getNumChildren() == 2);	// S62 not sure how kuzu generates comparison expression, now assume 2
	auto children = comp_expr->getChildren();
	// lhs, rhs
	CExpression* lhs_scalar_expr = lExprScalarExpression(children[0].get(), prev_plan);
	CExpression* rhs_scalar_expr = lExprScalarExpression(children[1].get(), prev_plan);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// map comparison type
	IMDType::ECmpType cmp_type;
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

CExpression* Planner::lExprScalarPropertyExpr(Expression* expression, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;

	PropertyExpression* prop_expr = (PropertyExpression*) expression;
	string k1 = "";
	string k2 = "";

	CColRef* target_colref;

	// try first with property
	k1 = prop_expr->getVariableRawName();
	k2 = prop_expr->getPropertyName();
	target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
	
	// fallback to alias
	if( target_colref == NULL && prop_expr->hasAlias() ) {
		k1 = prop_expr->getAlias();
		k2 = ""; 
		target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
	}

	CExpression* ident_expr = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
	
	return ident_expr;
}

CExpression* Planner::lExprScalarPropertyExpr(string k1, string k2, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;

	CColRef* target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);

	CExpression* ident_expr = GPOS_NEW(mp)
			CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, target_colref));
	
	return ident_expr;
}

CExpression* Planner::lExprScalarLiteralExpr(Expression* expression, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;

	LiteralExpression* lit_expr = (LiteralExpression*) expression;
	DataType type = lit_expr->literal.get()->dataType;

	// D_ASSERT( !lit_expr->isNull() && "currently null not supported");

	CExpression* pexpr = nullptr;
	uint32_t literal_type_id = LOGICAL_TYPE_BASE_ID + (OID)type.typeID;
	CMDIdGPDB* type_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, literal_type_id, 1, 0);
	type_mdid->AddRef();
	
	void* serialized_literal = NULL;
	uint64_t serialized_literal_length = 0;
	if( !lit_expr->isNull() ) {
		DatumSerDes::SerializeKUZULiteralIntoOrcaByteArray(literal_type_id, lit_expr->literal.get(), serialized_literal, serialized_literal_length);
		D_ASSERT(serialized_literal != NULL && serialized_literal_length != 0);
	}

	IDatumGeneric *datum = (IDatumGeneric*) (GPOS_NEW(mp) CDatumGenericGPDB(mp, (IMDId*)type_mdid, 0, serialized_literal, serialized_literal_length, lit_expr->isNull(), (LINT)0, (CDouble)0.0));
	datum->AddRef();
	pexpr = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CScalarConst(mp, (IDatum *) datum));
	pexpr->AddRef();

	D_ASSERT(pexpr != nullptr);
	return pexpr;
}

CExpression * Planner::lExprScalarAggFuncExpr(Expression* expression, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;
	
	AggregateFunctionExpression* aggfunc_expr = (AggregateFunctionExpression*) expression;
	kuzu::binder::expression_vector children = aggfunc_expr->getChildren();

	std::string func_name = (expression)->getUniqueName();
	transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);	// to lower case
	D_ASSERT(func_name != "");

	// refer expression_type.h
	bool child_exists = children.size() > 0;
	CColRef* child_colref = nullptr;
	D_ASSERT(children.size()<=1); 	// not sure yet
	
	vector<CExpression*> child_exprs;
	vector<duckdb::LogicalType> child_types;
	if(child_exists)
		CExpression* child_expr = lExprScalarExpression(children[0].get(), prev_plan);
		D_ASSERT(child_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent );
		child_expr.push_back(child_expr);

		CColRef* colref = pGetColRefFromScalarIdent(child_expr);
		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId());
		OID type_oid = type_mdid->Oid();
		child_types.push_back(pConvertTypeOidToLogicalType(type_oid));
	}
	if(func_name == "count_star") {
		// catalog requires ANY input for count_star
		child_types.push_back(duckdb::LogicalType::ANY);
	}
	// refer expression_type.h for kuzu function names
	idx_t func_mdid_id = context->db->GetCatalogWrapper().GetAggFuncMdId(context, func_name, child_types);
	// no assert?

	IMDId* func_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
	func_mdid->AddRef();
	const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(func_mdid);
	IMDId *agg_mdid = pmdagg->MDId();
	agg_mdid->AddRef();
	CWStringConst *str = GPOS_NEW(mp)
		CWStringConst(mp, pmdagg->Mdname().GetMDName()->GetBuffer());

	// refer cutils.h
	if(child_exists) {
		return CUtils::PexprAggFunc(mp, agg_mdid, str, colref, aggfunc_expr->isDistinct(),
						EaggfuncstageGlobal /*fGlobal*/, false /*fSplit*/);
	} else {
		CScalarAggFunc *popScAggFunc = CUtils::PopAggFunc(mp, agg_mdid, str, aggfunc_expr->isDistinct() /*is_distinct*/,
				   EaggfuncstageGlobal /*eaggfuncstage*/, false /*fSplit*/, NULL, EaggfunckindNormal);
		CExpression *pexpr = GPOS_NEW(mp)
			CExpression(mp, popScAggFunc, PexprAggFuncArgs(mp, pdrgpexprChildren));
		pexpr->AddRef();
		return pexpr;
	}
	D_ASSERT(false);
}

CExpression *Planner::lExprScalarCaseElseExpr(Expression *expression, LogicalPlan *prev_plan) {
	CMemoryPool* mp = this->memory_pool;

	CaseExpression *case_expr = (CaseExpression *)expression;

	size_t numCaseAlts = case_expr->getNumCaseAlternatives();
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	// generate expression for CaseAlternatives
	for (size_t idx = 0; idx < numCaseAlts; idx++) {
		auto casealt_expr = case_expr->getCaseAlternative(idx);
		auto when_expr = casealt_expr->whenExpression;
		auto then_expr = casealt_expr->thenExpression;

		CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

		// convert when & then expr
		CExpression *c_when_expr = lExprScalarExpression(when_expr.get(), prev_plan);
		CExpression *c_then_expr = lExprScalarExpression(then_expr.get(), prev_plan);
		
		pdrgpexprChildren->Append(c_when_expr);
		pdrgpexprChildren->Append(c_then_expr);

		CExpression *c_casealt_expr  = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarSwitchCase(mp), pdrgpexprChildren);
		pdrgpexpr->Append(c_casealt_expr);
	}

	// generate expression for Else
	auto else_expr = case_expr->getElseExpression();
	CExpression *else_expr_tmp = lExprScalarExpression(else_expr.get(), prev_plan);
	pdrgpexpr->Append(else_expr_tmp);

	// get return data type
	DataType return_type = case_expr->getDataType();
	uint32_t return_type_id = LOGICAL_TYPE_BASE_ID + (OID)return_type.typeID;
	CMDIdGPDB* return_type_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, return_type_id, 1, 0);
	return_type_mdid->AddRef();

	CExpression *result_expr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CScalarSwitch(mp, return_type_mdid), pdrgpexpr);

	return result_expr;
}

}