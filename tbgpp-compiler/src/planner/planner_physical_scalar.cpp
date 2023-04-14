#include "planner.hpp"

#include <string>
#include <limits>

// locally used duckdb expressions
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"

#include "common/enums/join_type.hpp"

namespace s62 {

unique_ptr<duckdb::Expression> Planner::pTransformScalarExpr(CExpression * scalar_expr, CColRefArray* child_cols) {
	
	switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarIdent: return pTransformScalarIdent(scalar_expr, child_cols);
		case COperator::EopScalarConst: return pTransformScalarConst(scalar_expr, child_cols);
		case COperator::EopScalarCmp: return pTransformScalarCmp(scalar_expr, child_cols);
		case COperator::EopScalarBoolOp: return pTransformScalarBoolOp(scalar_expr, child_cols);
		case COperator::EopScalarAggFunc: return pTransformScalarAggFunc(scalar_expr, child_cols);
		case COperator::EopScalarSwitch: return pTransformScalarSwitch(scalar_expr, child_cols);
		default:
			D_ASSERT(false); // NOT implemented yet
	}
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarIdent(CExpression * scalar_expr, CColRefArray* child_cols) {
	
	CScalarIdent* ident_op = (CScalarIdent*)scalar_expr->Pop();

	ULONG child_index = child_cols->IndexOf(ident_op->Pcr());
	D_ASSERT(child_index != gpos::ulong_max);
	CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId() );
	OID type_oid = type_mdid->Oid();
	
	return make_unique<duckdb::BoundReferenceExpression>( pConvertTypeOidToLogicalTypeId(type_oid), (int)child_index );
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarConst(CExpression * scalar_expr, CColRefArray* child_cols) {

	CScalarConst* op = (CScalarConst*)scalar_expr->Pop();

	CDatumGenericGPDB *datum = (CDatumGenericGPDB*)(op->GetDatum());
	duckdb::Value literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
									CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
									datum->GetByteArrayValue(),
									(uint64_t) datum->Size());

	return make_unique<duckdb::BoundConstantExpression>(literal_val);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarCmp(CExpression * scalar_expr, CColRefArray* child_cols) {

	CScalarCmp* op = (CScalarCmp*)scalar_expr->Pop();
	return make_unique<duckdb::BoundComparisonExpression>(
		pTranslateCmpType(op->ParseCmpType()),
		std::move(pTransformScalarExpr(scalar_expr->operator[](0), child_cols)),	// lhs
		std::move(pTransformScalarExpr(scalar_expr->operator[](1), child_cols))	// rhs
	);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarBoolOp(CExpression * scalar_expr, CColRefArray* child_cols) {

	CScalarBoolOp* op = (CScalarBoolOp*)scalar_expr->Pop();
	auto op_type = pTranslateBoolOpType(op->Eboolop());

	if( op_type == duckdb::ExpressionType::OPERATOR_NOT) {
		// unary - NOT
		auto result = make_unique<duckdb::BoundOperatorExpression>(op_type, duckdb::LogicalType::BOOLEAN);
		result->children.push_back( std::move(pTransformScalarExpr(scalar_expr->operator[](0), child_cols)) );
		// TODO uncertain if this is right.s
		return std::move(result);
	} else {
		// binary
		return make_unique<duckdb::BoundConjunctionExpression>(op_type,
			std::move(pTransformScalarExpr(scalar_expr->operator[](0), child_cols)),	// lhs
			std::move(pTransformScalarExpr(scalar_expr->operator[](1), child_cols))		// rhs
		);
	}
	D_ASSERT(false);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarAggFunc(CExpression * scalar_expr, CColRefArray* child_cols) {

	CScalarAggFunc* op = (CScalarAggFunc*)scalar_expr->Pop();
	CExpression* aggargs_expr = scalar_expr->operator[](0);
	CScalarValuesList* aggargs = (CScalarValuesList*)(scalar_expr->operator[](0)->Pop());
		// TODO may need to expand four childs (aggargs, aggdirectargs, aggorder, aggdistinct)
	unique_ptr<duckdb::Expression> result;

	vector<unique_ptr<duckdb::Expression>> child;
	
	for( ULONG child_idx = 0; child_idx < aggargs_expr->Arity(); child_idx++ ) {
		child.push_back(pTransformScalarExpr(aggargs_expr->operator[](child_idx), child_cols));
	}
	D_ASSERT(child.size() <= 1);

	OID agg_func_id = CMDIdGPDB::CastMdid(op->MDId())->Oid();
	auto aggfunc_catalog_entry = context->db->GetCatalogWrapper().GetAggFunc(*context, agg_func_id);

	vector<duckdb::LogicalType> arguments;
	for(auto& ch: child) { arguments.push_back(ch.get()->return_type); }
	auto& functions = aggfunc_catalog_entry->functions.get()->functions;
	std::string error_string;

	duckdb::idx_t function_idx = duckdb::Function::BindFunction(std::string(aggfunc_catalog_entry->name), functions, arguments, error_string);
	D_ASSERT(function_idx != duckdb::idx_t(-1));

	// return duckdb::AggregateFunction::BindAggregateFunction(
	// 	*context, functions[function_idx], move(child), nullptr, op->IsDistinct(), nullptr
	// );	// TODO currently no support on GB having and aggregate orderbys

	return make_unique<duckdb::BoundAggregateExpression>(
		functions[function_idx], std::move(child), nullptr, nullptr, op->IsDistinct());
	
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarSwitch(CExpression *scalar_expr, CColRefArray *child_cols) {

	CScalarSwitch *op = (CScalarSwitch *)scalar_expr->Pop();

	uint32_t num_childs = scalar_expr->Arity();
	D_ASSERT(num_childs == 2); // currently support only one when/then
	unique_ptr<duckdb::Expression> e_when;
	unique_ptr<duckdb::Expression> e_then;
	unique_ptr<duckdb::Expression> e_else;

	// when/then
	for (uint32_t i = 0; i < num_childs - 1; i++) {
		CExpression *child_expr = scalar_expr->operator[](i);
		D_ASSERT(child_expr->Arity() == 2); // when & then

		CExpression *when_expr = child_expr->operator[](0);
		CExpression *then_expr = child_expr->operator[](1);

		e_when = std::move(pTransformScalarExpr(when_expr, child_cols));
		e_then = std::move(pTransformScalarExpr(then_expr, child_cols));
	}

	// else
	e_else = std::move(pTransformScalarExpr(scalar_expr->operator[](num_childs - 1), child_cols));

	return make_unique<duckdb::BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
}

duckdb::ExpressionType Planner::pTranslateCmpType(IMDType::ECmpType cmp_type) {
	switch(cmp_type) {
		case IMDType::ECmpType::EcmptEq: return duckdb::ExpressionType::COMPARE_EQUAL;
		case IMDType::ECmpType::EcmptNEq: return duckdb::ExpressionType::COMPARE_NOTEQUAL;
		case IMDType::ECmpType::EcmptL: return duckdb::ExpressionType::COMPARE_LESSTHAN;
		case IMDType::ECmpType::EcmptLEq: return duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
		case IMDType::ECmpType::EcmptG: return duckdb::ExpressionType::COMPARE_GREATERTHAN;
		case IMDType::ECmpType::EcmptGEq: return duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		case IMDType::ECmpType::EcmptIDF: return duckdb::ExpressionType::COMPARE_DISTINCT_FROM;
		default: D_ASSERT(false);
	}

}

duckdb::ExpressionType Planner::pTranslateBoolOpType(CScalarBoolOp::EBoolOperator op_type) {
	switch(op_type) {
		case CScalarBoolOp::EBoolOperator::EboolopAnd: return duckdb::ExpressionType::CONJUNCTION_AND;
		case CScalarBoolOp::EBoolOperator::EboolopOr: return duckdb::ExpressionType::CONJUNCTION_OR;
		case CScalarBoolOp::EBoolOperator::EboolopNot: return duckdb::ExpressionType::OPERATOR_NOT;
		default: D_ASSERT(false);
	}
}

CColRef* Planner::pGetColRefFromScalarIdent(CExpression* ident_expr) {
	D_ASSERT(ident_expr->Pop()->Eopid() == COperator::EopScalarIdent);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	return col_factory->LookupColRef(((CScalarIdent*)(ident_expr->Pop()))->Pcr()->Id());
}


}
