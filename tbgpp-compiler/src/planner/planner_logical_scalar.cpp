#include "planner.hpp"


#include <string>
#include <limits>

namespace s62 {

CExpression* Planner::lExprScalarExpression(Expression* expression, LogicalPlan* prev_plan) {

	auto expr_type = expression->expressionType;
	if( isExpressionComparison(expr_type) ) {
		return lExprScalarComparisonExpr(expression, prev_plan);
	} else if( PROPERTY == expr_type) {
		return lExprScalarPropertyExpr(expression, prev_plan);	// property access need to access previous plan
	} else if ( isExpressionLiteral(expr_type) ) {
		return lExprScalarLiteralExpr(expression, prev_plan);
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
	string k1 = prop_expr->getVariableRawName();
	string k2 = prop_expr->getPropertyName();

	CColRef* target_colref;
	target_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);

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

	D_ASSERT( !lit_expr->isNull() && "currently null not supported");

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

}