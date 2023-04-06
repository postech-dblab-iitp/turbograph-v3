#include "planner.hpp"

#include <string>
#include <limits>

// locally used duckdb expressions
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"

#include "common/enums/join_type.hpp"

namespace s62 {

unique_ptr<duckdb::Expression> Planner::pTransformScalarExpr(CExpression * scalar_expr, CColRefArray* child_cols) {
	
	switch (scalar_expr->Pop()->Eopid()) {
		case COperator::EopScalarIdent: return pTransformScalarIdent(scalar_expr, child_cols);
		case COperator::EopScalarConst: return pTransformScalarConst(scalar_expr, child_cols);
		case COperator::EopScalarCmp: return pTransformScalarCmp(scalar_expr, child_cols);
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

}
