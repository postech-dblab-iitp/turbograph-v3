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

unique_ptr<duckdb::Expression> Planner::pTransformScalarExpr(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {
	
	switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarIdent: return pTransformScalarIdent(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarConst: return pTransformScalarConst(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarCmp: return pTransformScalarCmp(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarBoolOp: return pTransformScalarBoolOp(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarAggFunc: return pTransformScalarAggFunc(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarFunc: return pTransformScalarFunc(scalar_expr, lhs_child_cols, rhs_child_cols);
		case COperator::EopScalarSwitch: return pTransformScalarSwitch(scalar_expr, lhs_child_cols, rhs_child_cols);
		default:
			GPOS_ASSERT(false); // NOT implemented yet
	}
}

void Planner::pGetAllScalarIdents(CExpression * scalar_expr, vector<uint32_t> &sccmp_colids) {
	switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarIdent: {
			CScalarIdent *sc_ident = (CScalarIdent *)(scalar_expr->Pop());
			sccmp_colids.push_back(sc_ident->Pcr()->Id());
			return;
		}
		case COperator::EopScalarConst: return;
		case COperator::EopScalarCmp: {
			pGetAllScalarIdents(scalar_expr->operator[](0), sccmp_colids);
			pGetAllScalarIdents(scalar_expr->operator[](1), sccmp_colids);
			return;
		}
		case COperator::EopScalarBoolOp: {
			for (ULONG child_idx = 0; child_idx < scalar_expr->Arity(); child_idx++) {
				pGetAllScalarIdents(scalar_expr->operator[](child_idx), sccmp_colids);
			}
		}
		case COperator::EopScalarAggFunc: return;
		case COperator::EopScalarFunc: return;
		case COperator::EopScalarSwitch: return;
		default:
			GPOS_ASSERT(!"[pGetAllScalarIdents] Not Implemeneted Yet"); // NOT implemented yet
	}
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarIdent(CExpression *scalar_expr, CColRefArray *lhs_child_cols, CColRefArray *rhs_child_cols) {
	CScalarIdent *ident_op = (CScalarIdent*)scalar_expr->Pop();

	// first find from LHS
	bool is_inner = false;
	ULONG child_index = lhs_child_cols->IndexOf(ident_op->Pcr());
	// try finding from RHS; refer duckdb's mechanism on ColumnBindingResolver
	if (child_index == gpos::ulong_max && (rhs_child_cols != nullptr)) {
		child_index = rhs_child_cols->IndexOf(ident_op->Pcr());
		is_inner = true;
	}

	if (child_index == gpos::ulong_max) {
	GPOS_ASSERT(child_index != gpos::ulong_max); // column reference not found in child columns
	}
	
	CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId());
	OID type_oid = type_mdid->Oid();
	INT type_mod = ident_op->Pcr()->TypeModifier();
	
	return make_unique<duckdb::BoundReferenceExpression>(pConvertTypeOidToLogicalType(type_oid, type_mod), (int)child_index, is_inner);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarIdent(CExpression *scalar_expr, CColRefArray *lhs_child_cols, ULONG child_index) {	
	CScalarIdent *ident_op = (CScalarIdent*)scalar_expr->Pop();

	GPOS_ASSERT(child_index != gpos::ulong_max);
	CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId());
	OID type_oid = type_mdid->Oid();
	INT type_mod = ident_op->Pcr()->TypeModifier();
	
	return make_unique<duckdb::BoundReferenceExpression>(pConvertTypeOidToLogicalType(type_oid, type_mod), (int)child_index);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarConst(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {

	CScalarConst* op = (CScalarConst*)scalar_expr->Pop();

	CDatumGenericGPDB *datum = (CDatumGenericGPDB*)(op->GetDatum());

	// handle orca primitives exceptionally
	switch (datum->GetDatumType()) {
		case IMDType::EtiInt2:
		case IMDType::EtiInt4:
		case IMDType::EtiInt8:
			GPOS_ASSERT(false);
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
			GPOS_ASSERT(false);
		}
	}
	GPOS_ASSERT(false);
	return nullptr;
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarCmp(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {

	CScalarCmp* op = (CScalarCmp*)scalar_expr->Pop();

	unique_ptr<duckdb::Expression> lhs = pTransformScalarExpr(scalar_expr->operator[](0), lhs_child_cols, rhs_child_cols);
	unique_ptr<duckdb::Expression> rhs = pTransformScalarExpr(scalar_expr->operator[](1), lhs_child_cols, rhs_child_cols);
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

unique_ptr<duckdb::Expression> Planner::pTransformScalarBoolOp(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {

	CScalarBoolOp* op = (CScalarBoolOp*)scalar_expr->Pop();
	auto op_type = pTranslateBoolOpType(op->Eboolop());

	if (op_type == duckdb::ExpressionType::OPERATOR_NOT) {
		// unary - NOT
		auto result = make_unique<duckdb::BoundOperatorExpression>(op_type, duckdb::LogicalType::BOOLEAN);
		result->children.push_back(std::move(pTransformScalarExpr(scalar_expr->operator[](0), lhs_child_cols, rhs_child_cols)));
		// TODO uncertain if this is right.s
		return std::move(result);
	} else if (op_type == duckdb::ExpressionType::CONJUNCTION_AND) {
		auto conjunction = make_unique<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND);
		for (ULONG child_idx = 0; child_idx < scalar_expr->Arity(); child_idx++) {
			conjunction->children.push_back(std::move(pTransformScalarExpr(scalar_expr->operator[](child_idx), lhs_child_cols, rhs_child_cols)));
		}
		return conjunction;
	} else if (op_type == duckdb::ExpressionType::CONJUNCTION_OR) {
		auto conjunction = make_unique<duckdb::BoundConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_OR);
		for (ULONG child_idx = 0; child_idx < scalar_expr->Arity(); child_idx++) {
			conjunction->children.push_back(std::move(pTransformScalarExpr(scalar_expr->operator[](child_idx), lhs_child_cols, rhs_child_cols)));
		}
		return conjunction;
	}
	GPOS_ASSERT(false);
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarAggFunc(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {

	CScalarAggFunc* op = (CScalarAggFunc*)scalar_expr->Pop();
	CExpression* aggargs_expr = scalar_expr->operator[](0);
	CScalarValuesList* aggargs = (CScalarValuesList*)(scalar_expr->operator[](0)->Pop());
		// TODO may need to expand four childs (aggargs, aggdirectargs, aggorder, aggdistinct)
	unique_ptr<duckdb::Expression> result;

	vector<unique_ptr<duckdb::Expression>> child;
	
	for( ULONG child_idx = 0; child_idx < aggargs_expr->Arity(); child_idx++ ) {
		child.push_back(pTransformScalarExpr(aggargs_expr->operator[](child_idx), lhs_child_cols, rhs_child_cols));
	}
	GPOS_ASSERT(child.size() <= 1);

	OID agg_func_id = CMDIdGPDB::CastMdid(op->MDId())->Oid();
	duckdb::AggregateFunctionCatalogEntry *aggfunc_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetAggFuncAndIdx(*context, agg_func_id, aggfunc_catalog_entry, function_idx);

	auto function = aggfunc_catalog_entry->functions.get()->functions[function_idx];
	unique_ptr<duckdb::FunctionData> bind_info;
	if (function.bind) {
		bind_info = function.bind(*context, function, child);
		child.resize(std::min(function.arguments.size(), child.size()));
	}

	// check if we need to add casts to the children
	function.CastToFunctionArguments(child);

	return make_unique<duckdb::BoundAggregateExpression>(
		std::move(function), std::move(child), nullptr, std::move(bind_info), op->IsDistinct()); // get function.return_type
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarAggFunc(CExpression * scalar_expr, CColRefArray* lhs_child_cols, duckdb::LogicalType child_ref_type, int child_ref_idx, CColRefArray* rhs_child_cols) {

	CScalarAggFunc* op = (CScalarAggFunc*)scalar_expr->Pop();
	CExpression* aggargs_expr = scalar_expr->operator[](0);
	CScalarValuesList* aggargs = (CScalarValuesList*)(scalar_expr->operator[](0)->Pop());
	unique_ptr<duckdb::Expression> result;

	vector<unique_ptr<duckdb::Expression>> child;
	child.push_back(make_unique<duckdb::BoundReferenceExpression>(child_ref_type, child_ref_idx));
	GPOS_ASSERT(child.size() <= 1);

	OID agg_func_id = CMDIdGPDB::CastMdid(op->MDId())->Oid();
	duckdb::AggregateFunctionCatalogEntry *aggfunc_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetAggFuncAndIdx(*context, agg_func_id, aggfunc_catalog_entry, function_idx);

	auto function = aggfunc_catalog_entry->functions.get()->functions[function_idx];
	unique_ptr<duckdb::FunctionData> bind_info;
	if (function.bind) {
		bind_info = function.bind(*context, function, child);
		child.resize(std::min(function.arguments.size(), child.size()));
	}

	// check if we need to add casts to the children
	function.CastToFunctionArguments(child);

	return make_unique<duckdb::BoundAggregateExpression>(
		std::move(function), std::move(child), nullptr, std::move(bind_info), op->IsDistinct());
}

unique_ptr<duckdb::Expression> Planner::pTransformScalarFunc(CExpression * scalar_expr, CColRefArray* lhs_child_cols, CColRefArray* rhs_child_cols) {
	CScalarFunc* op = (CScalarFunc*)scalar_expr->Pop();
	CExpressionArray* scalarfunc_exprs = scalar_expr->PdrgPexpr();

	vector<unique_ptr<duckdb::Expression>> child;
	for (ULONG child_idx = 0; child_idx < scalarfunc_exprs->Size(); child_idx++) {
		child.push_back(pTransformScalarExpr(scalarfunc_exprs->operator[](child_idx), lhs_child_cols, rhs_child_cols));
	}

	OID func_id = CMDIdGPDB::CastMdid(op->FuncMdId())->Oid();
	duckdb::ScalarFunctionCatalogEntry *func_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetScalarFuncAndIdx(*context, func_id, func_catalog_entry, function_idx);

	auto function = func_catalog_entry->functions.get()->functions[function_idx];
	unique_ptr<duckdb::FunctionData> bind_info;
	if (function.bind) {
		bind_info = function.bind(*context, function, child);
		child.resize(std::min(function.arguments.size(), child.size()));
	}

	// check if we need to add casts to the children
	function.CastToFunctionArguments(child);

	return make_unique<duckdb::BoundFunctionExpression>(
			function.return_type, function, std::move(child), std::move(bind_info));
}


unique_ptr<duckdb::Expression> Planner::pTransformScalarFunc(CExpression * scalar_expr, vector<unique_ptr<duckdb::Expression>>& child) {
	CScalarFunc* op = (CScalarFunc*)scalar_expr->Pop();
	CExpressionArray* scalarfunc_exprs = scalar_expr->PdrgPexpr();

	OID func_id = CMDIdGPDB::CastMdid(op->FuncMdId())->Oid();
	duckdb::ScalarFunctionCatalogEntry *func_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetScalarFuncAndIdx(*context, func_id, func_catalog_entry, function_idx);

	auto function = func_catalog_entry->functions.get()->functions[function_idx];
	unique_ptr<duckdb::FunctionData> bind_info;
	if (function.bind) {
		bind_info = function.bind(*context, function, child);
		child.resize(std::min(function.arguments.size(), child.size()));
	}

	// check if we need to add casts to the children
	function.CastToFunctionArguments(child);

	return make_unique<duckdb::BoundFunctionExpression>(
			function.return_type, function, std::move(child), std::move(bind_info));
}


unique_ptr<duckdb::Expression> Planner::pTransformScalarSwitch(CExpression *scalar_expr, CColRefArray *lhs_child_cols, CColRefArray* rhs_child_cols) {

	CScalarSwitch *op = (CScalarSwitch *)scalar_expr->Pop();

	uint32_t num_childs = scalar_expr->Arity();
	GPOS_ASSERT(num_childs == 2); // currently support only one when/then
	unique_ptr<duckdb::Expression> e_when;
	unique_ptr<duckdb::Expression> e_then;
	unique_ptr<duckdb::Expression> e_else;

	// when/then
	for (uint32_t i = 0; i < num_childs - 1; i++) {
		CExpression *child_expr = scalar_expr->operator[](i);
		GPOS_ASSERT(child_expr->Arity() == 2); // when & then

		CExpression *when_expr = child_expr->operator[](0);
		CExpression *then_expr = child_expr->operator[](1);

		e_when = std::move(pTransformScalarExpr(when_expr, lhs_child_cols, rhs_child_cols));
		e_then = std::move(pTransformScalarExpr(then_expr, lhs_child_cols, rhs_child_cols));
	}

	// else
	e_else = std::move(pTransformScalarExpr(scalar_expr->operator[](num_childs - 1), lhs_child_cols, rhs_child_cols));

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
		default: GPOS_ASSERT(false);
	}

}

duckdb::ExpressionType Planner::pTranslateBoolOpType(CScalarBoolOp::EBoolOperator op_type) {
	switch(op_type) {
		case CScalarBoolOp::EBoolOperator::EboolopAnd: return duckdb::ExpressionType::CONJUNCTION_AND;
		case CScalarBoolOp::EBoolOperator::EboolopOr: return duckdb::ExpressionType::CONJUNCTION_OR;
		case CScalarBoolOp::EBoolOperator::EboolopNot: return duckdb::ExpressionType::OPERATOR_NOT;
		default: GPOS_ASSERT(false);
	}
}

CColRef *Planner::pGetColRefFromScalarIdent(CExpression *ident_expr) {
	GPOS_ASSERT(ident_expr->Pop()->Eopid() == COperator::EopScalarIdent);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	return col_factory->LookupColRef(((CScalarIdent*)(ident_expr->Pop()))->Pcr()->Id());
}

OID Planner::pGetTypeIdFromScalar(CExpression *expr) {
	if (expr->Pop()->Eopid() == COperator::EopScalarIdent) {
		return pGetTypeIdFromScalarIdent(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarConst) {
		return pGetTypeIdFromScalarConst(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarFunc) {
		return pGetTypeIdFromScalarFunc(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarAggFunc) {
		return pGetTypeIdFromScalarAggFunc(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarSwitch) {
		return pGetTypeIdFromScalarSwitch(expr);
	} else {
		GPOS_ASSERT(false); // not implemented yet
	}
}

OID Planner::pGetTypeIdFromScalarIdent(CExpression *ident_expr) {
	GPOS_ASSERT(ident_expr->Pop()->Eopid() == COperator::EopScalarIdent);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRef *colref = col_factory->LookupColRef(((CScalarIdent*)(ident_expr->Pop()))->Pcr()->Id());
	CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId());
	return type_mdid->Oid();
}

OID Planner::pGetTypeIdFromScalarConst(CExpression *const_expr) {
	GPOS_ASSERT(const_expr->Pop()->Eopid() == COperator::EopScalarConst);
	CScalarConst *const_op = CScalarConst::PopConvert(const_expr->Pop());
	CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(const_op->MdidType());
	return type_mdid->Oid();
}

OID Planner::pGetTypeIdFromScalarFunc(CExpression *func_expr) {
	GPOS_ASSERT(func_expr->Pop()->Eopid() == COperator::EopScalarFunc);
	CScalarFunc *func_op = CScalarFunc::PopConvert(func_expr->Pop());
	CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(func_op->MdidType());
	return type_mdid->Oid();
}

OID Planner::pGetTypeIdFromScalarAggFunc(CExpression *agg_expr) {
	GPOS_ASSERT(agg_expr->Pop()->Eopid() == COperator::EopScalarAggFunc);
	CScalarAggFunc *aggfunc_op = CScalarAggFunc::PopConvert(agg_expr->Pop());
	CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(aggfunc_op->MdidType());
	return type_mdid->Oid();
}

OID Planner::pGetTypeIdFromScalarSwitch(CExpression *switch_expr) {
	GPOS_ASSERT(switch_expr->Pop()->Eopid() == COperator::EopScalarSwitch);
	CScalarSwitch *switch_op = CScalarSwitch::PopConvert(switch_expr->Pop());
	CMDIdGPDB *type_mdid = CMDIdGPDB::CastMdid(switch_op->MdidType());
	return type_mdid->Oid();
}

INT Planner::pGetTypeModFromScalar(CExpression *expr) {
	if (expr->Pop()->Eopid() == COperator::EopScalarIdent) {
		return pGetTypeModFromScalarIdent(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarConst) {
		return pGetTypeModFromScalarConst(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarFunc) {
		return pGetTypeModFromScalarFunc(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarAggFunc) {
		return pGetTypeModFromScalarAggFunc(expr);
	} else if (expr->Pop()->Eopid() == COperator::EopScalarSwitch) {
		return pGetTypeModFromScalarSwitch(expr);
	} else {
		GPOS_ASSERT(false); // not implemented yet
	}
}

INT Planner::pGetTypeModFromScalarIdent(CExpression *ident_expr) {
	GPOS_ASSERT(ident_expr->Pop()->Eopid() == COperator::EopScalarIdent);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRef *colref = col_factory->LookupColRef(((CScalarIdent*)(ident_expr->Pop()))->Pcr()->Id());
	return colref->TypeModifier();
}

INT Planner::pGetTypeModFromScalarConst(CExpression *const_expr) {
	GPOS_ASSERT(const_expr->Pop()->Eopid() == COperator::EopScalarConst);
	CScalarConst *const_op = CScalarConst::PopConvert(const_expr->Pop());
	return const_op->TypeModifier();
}

INT Planner::pGetTypeModFromScalarFunc(CExpression *func_expr) {
	GPOS_ASSERT(func_expr->Pop()->Eopid() == COperator::EopScalarFunc);
	CScalarFunc *func_op = CScalarFunc::PopConvert(func_expr->Pop());
	return func_op->TypeModifier();
}

INT Planner::pGetTypeModFromScalarAggFunc(CExpression *agg_expr) {
	GPOS_ASSERT(agg_expr->Pop()->Eopid() == COperator::EopScalarAggFunc);
	CScalarAggFunc *aggfunc_op = CScalarAggFunc::PopConvert(agg_expr->Pop());
	return aggfunc_op->TypeModifier();
}

INT Planner::pGetTypeModFromScalarSwitch(CExpression *switch_expr) {
	GPOS_ASSERT(switch_expr->Pop()->Eopid() == COperator::EopScalarSwitch);
	return -1;
}

/**
 * Special handling for post projection
*/

}
