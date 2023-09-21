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
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"

#include "common/enums/join_type.hpp"

namespace s62 {

unique_ptr<duckdb::Expression> Planner::pTransformScalarExpr(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {
	
	switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarIdent: return pTransformScalarIdent(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarConst: return pTransformScalarConst(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarCmp: return pTransformScalarCmp(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarBoolOp: return pTransformScalarBoolOp(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarAggFunc: return pTransformScalarAggFunc(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarFunc: return pTransformScalarFunc(scalar_expr, child_cols, rhs_child_cols);
		case COperator::EopScalarSwitch: return pTransformScalarSwitch(scalar_expr, child_cols, rhs_child_cols);
		default:
			D_ASSERT(false); // NOT implemented yet
	}
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarIdent(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols) {
	
	CScalarIdent *ident_op = (CScalarIdent*)scalar_expr->Pop();

	// first find from LHS
	ULONG child_index = child_cols->IndexOf(ident_op->Pcr());
	// try finding from RHS; refer duckdb's mechanism on ColumnBindingResolver
	if (child_index == gpos::ulong_max && (rhs_child_cols != nullptr)) {
		child_index = rhs_child_cols->IndexOf(ident_op->Pcr());
		if (child_index != gpos::ulong_max) {	// index rules; LHS first, and then RHS next
			child_index += child_cols->Size();
		}
	}
	D_ASSERT(child_index != gpos::ulong_max);
	CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId());
	OID type_oid = type_mdid->Oid();
	
	return make_unique<duckdb::BoundReferenceExpression>(pConvertTypeOidToLogicalTypeId(type_oid), (int)child_index);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarConst(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {

	CScalarConst* op = (CScalarConst*)scalar_expr->Pop();

	CDatumGenericGPDB *datum = (CDatumGenericGPDB*)(op->GetDatum());

	// handle orca primitives exceptionally
	switch (datum->GetDatumType()) {
		case IMDType::EtiInt2:
		case IMDType::EtiInt4:
		case IMDType::EtiInt8:
			D_ASSERT(false);
			break;
		case IMDType::EtiBool:{
			CDatumBoolGPDB *datum_bool = (CDatumBoolGPDB*)datum;
			return make_unique<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN((int8_t) datum_bool->GetValue()));
		}
		case IMDType::EtiGeneric: {
			// our types
			duckdb::Value literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
									CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
									datum->GetByteArrayValue(),
									(uint64_t) datum->Size());
			return make_unique<duckdb::BoundConstantExpression>(literal_val);
		}
		default: {
			D_ASSERT(false);
		}
	}
	D_ASSERT(false);
	return nullptr;
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarCmp(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {

	CScalarCmp* op = (CScalarCmp*)scalar_expr->Pop();

	unique_ptr<duckdb::Expression> lhs = pTransformScalarExpr(scalar_expr->operator[](0), child_cols, rhs_child_cols);
	unique_ptr<duckdb::Expression> rhs = pTransformScalarExpr(scalar_expr->operator[](1), child_cols, rhs_child_cols);
	//try casting (rhs to lhs)
	if(lhs->return_type != rhs->return_type) {
		rhs = pGenScalarCast(move(rhs), lhs->return_type);
	}

	return make_unique<duckdb::BoundComparisonExpression>(
		pTranslateCmpType(op->ParseCmpType()),
		std::move(lhs),	// lhs
		std::move(rhs)	// rhs
	);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarBoolOp(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {

	CScalarBoolOp* op = (CScalarBoolOp*)scalar_expr->Pop();
	auto op_type = pTranslateBoolOpType(op->Eboolop());

	if (op_type == duckdb::ExpressionType::OPERATOR_NOT) {
		// unary - NOT
		auto result = make_unique<duckdb::BoundOperatorExpression>(op_type, duckdb::LogicalType::BOOLEAN);
		result->children.push_back(std::move(pTransformScalarExpr(scalar_expr->operator[](0), child_cols, rhs_child_cols)));
		// TODO uncertain if this is right.s
		return std::move(result);
	} else {
		// binary
		return make_unique<duckdb::BoundConjunctionExpression>(op_type,
			std::move(pTransformScalarExpr(scalar_expr->operator[](0), child_cols, rhs_child_cols)),	// lhs
			std::move(pTransformScalarExpr(scalar_expr->operator[](1), child_cols, rhs_child_cols))		// rhs
		);
	}
	D_ASSERT(false);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarAggFunc(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {

	CScalarAggFunc* op = (CScalarAggFunc*)scalar_expr->Pop();
	CExpression* aggargs_expr = scalar_expr->operator[](0);
	CScalarValuesList* aggargs = (CScalarValuesList*)(scalar_expr->operator[](0)->Pop());
		// TODO may need to expand four childs (aggargs, aggdirectargs, aggorder, aggdistinct)
	unique_ptr<duckdb::Expression> result;

	vector<unique_ptr<duckdb::Expression>> child;
	
	for( ULONG child_idx = 0; child_idx < aggargs_expr->Arity(); child_idx++ ) {
		child.push_back(pTransformScalarExpr(aggargs_expr->operator[](child_idx), child_cols, rhs_child_cols));
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

unique_ptr<duckdb::Expression> Planner::pTransformScalarFunc(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols) {
	CScalarFunc* op = (CScalarFunc*)scalar_expr->Pop();
	CExpressionArray* scalarfunc_exprs = scalar_expr->PdrgPexpr();

	vector<unique_ptr<duckdb::Expression>> child;
	for (ULONG child_idx = 0; child_idx < scalarfunc_exprs->Size(); child_idx++) {
		child.push_back(pTransformScalarExpr(scalarfunc_exprs->operator[](child_idx), child_cols, rhs_child_cols));
	}

	OID func_id = CMDIdGPDB::CastMdid(op->FuncMdId())->Oid();
	auto func_catalog_entry = context->db->GetCatalogWrapper().GetScalarFunc(*context, func_id);

	vector<duckdb::LogicalType> arguments;
	for(auto& ch: child) { arguments.push_back(ch.get()->return_type); }
	auto& functions = func_catalog_entry->functions.get()->functions;
	std::string error_string;

	duckdb::idx_t function_idx = duckdb::Function::BindFunction(std::string(func_catalog_entry->name), functions, arguments, error_string);
	D_ASSERT(function_idx != duckdb::idx_t(-1));
	auto function = functions[function_idx];

	return make_unique<duckdb::BoundFunctionExpression>(
		function.return_type, function, std::move(child), nullptr);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarSwitch(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray* rhs_child_cols) {

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

		e_when = std::move(pTransformScalarExpr(when_expr, child_cols, rhs_child_cols));
		e_then = std::move(pTransformScalarExpr(then_expr, child_cols, rhs_child_cols));
	}

	// else
	e_else = std::move(pTransformScalarExpr(scalar_expr->operator[](num_childs - 1), child_cols, rhs_child_cols));

	return make_unique<duckdb::BoundCaseExpression>(move(e_when), move(e_then), move(e_else));
}

unique_ptr<duckdb::Expression> Planner::pGenScalarCast(unique_ptr<duckdb::Expression> orig_expr, duckdb::LogicalType target_type) {
	return make_unique<duckdb::BoundCastExpression>(move(orig_expr), target_type, false /* try_cast */);
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
