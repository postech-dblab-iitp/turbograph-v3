//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalShortestPathGet.cpp
//
//	@doc:
//		Implementation of basic table access
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalShortestPathGet.h"

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CDynamicPtrArray.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::CLogicalShortestPathGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalShortestPathGet::CLogicalShortestPathGet(CMemoryPool *mp)
	: CLogical(mp),
	  m_pnameAlias(NULL),
	  m_ptabdescArray(NULL),
	  m_pdrgpcrOutput(NULL),
	  m_pdrgpdrgpcrPart(NULL),
	  m_pcrsDist(NULL),
	  path_join_lower_bound(0),
	  path_join_upper_bound(0)

{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::CLogicalShortestPathGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalShortestPathGet::CLogicalShortestPathGet(CMemoryPool *mp, const CName *pnameAlias,
						 CTableDescriptorArray *ptabdescArray,
						 CColRefArray *pdrgpcrOutput,
						 INT path_join_lower_bound,
					  	 INT path_join_upper_bound
						 )
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdescArray(ptabdescArray),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pdrgpdrgpcrPart(NULL),
	  m_pcrsDist(NULL),
	  path_join_lower_bound(path_join_lower_bound),
	  path_join_upper_bound(path_join_upper_bound)

{
	GPOS_ASSERT(NULL != ptabdescArray);
	GPOS_ASSERT(NULL != pnameAlias);

	GPOS_ASSERT( m_ptabdescArray->Size() > 0 ); 	// at least one edge relation should be supplied
	GPOS_ASSERT( m_pdrgpcrOutput->Size() == 3 ); 	// list[rel], _sid, _tid

	m_pdrgpcrOutput->AddRef();
	
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::~CLogicalShortestPathGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalShortestPathGet::~CLogicalShortestPathGet()
{
	CRefCount::SafeRelease(m_ptabdescArray);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalShortestPathGet::HashValue() const
{
	// ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
	// 								   m_ptabdesc->MDId()->HashValue());
	// ulHash =
	// 	gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	// return ulHash;

	return gpos::CombineHashes(COperator::HashValue(),
	 								   CUtils::UlHashColArray(m_pdrgpcrOutput));
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalShortestPathGet::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	// CLogicalShortestPathGet *popGet = CLogicalShortestPathGet::PopConvert(pop);

	// return m_ptabdesc->MDId()->Equals(popGet->m_ptabdesc->MDId()) &&
	// 	   m_pdrgpcrOutput->Equals(popGet->PdrgpcrOutput());
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalShortestPathGet::PopCopyWithRemappedColumns(CMemoryPool *mp,
										UlongToColRefMap *colref_mapping,
										BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput = NULL;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}
	CName *pnameAlias = GPOS_NEW(mp) CName(mp, *m_pnameAlias);
	m_ptabdescArray->AddRef();

	pdrgpcrOutput->AddRef();
	m_pdrgpcrOutput->AddRef();

	return GPOS_NEW(mp) CLogicalShortestPathGet(mp, pnameAlias, m_ptabdescArray, pdrgpcrOutput, path_join_lower_bound, path_join_upper_bound);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPathGet::DeriveOutputColumns(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG i = 0; i < m_pdrgpcrOutput->Size(); i++)
	{
		// We want to limit the output columns to only those which are referenced in the query
		// We will know the entire list of columns which are referenced in the query only after
		// translating the entire DXL to an expression. Hence we should not limit the output columns
		// before we have processed the entire DXL.
		if ((*m_pdrgpcrOutput)[i]->GetUsage() == CColRef::EUsed ||
			(*m_pdrgpcrOutput)[i]->GetUsage() == CColRef::EUnknown)
		{
			pcrs->Include((*m_pdrgpcrOutput)[i]);
		} else {
			GPOS_ASSERT(false);
		}
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::DeriveNotNullColumns
//
//	@doc:
//		Derive not null output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPathGet::DeriveNotNullColumns(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const
{
	// get all output columns
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(exprhdl.DeriveOutputColumns());

	
	// all columns are non-null in pathget

	// filters out nullable columns
	// CColRefSetIter crsi(*exprhdl.DeriveOutputColumns());
	// while (crsi.Advance())
	// {
	// 	CColRefTable *pcrtable =
	// 		CColRefTable::PcrConvert(const_cast<CColRef *>(crsi.Pcr()));
	// 	if (pcrtable->IsNullable())
	// 	{
	// 		pcrs->Exclude(pcrtable);
	// 	}
	// }

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalShortestPathGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalShortestPathGet::DeriveKeyCollection(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
) const
{
	// const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();
	mp = mp;

	// return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);

	// no keys
	// CKeyCollection *pkc = GPOS_NEW(mp) CKeyCollection(mp);
	// pkc->AddRef();
	// return pkc;
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalShortestPathGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	// no direct xform, transformed only using index scan
	//(void) xform_set->ExchangeSet(CXform::ExfGet2TableScan);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalShortestPathGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  IStatisticsArray *  // not used
) const
{
	// requesting stats on distribution columns to estimate data skew

	CDouble rows(1.0);
	rows = CStatistics::DefaultRelationRows;

	IStatistics *pstatsTable =
		PstatsDeriveDummy(mp, exprhdl, rows);

	return pstatsTable;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPathGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalShortestPathGet::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	else
	{
		os << SzId() << " ";
		// alias of table as referenced in the query
		m_pnameAlias->OsPrint(os);

		// actual name of table in catalog and columns
		os << " Columns: [";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
		os << "] ";
	}

	return os;
}



// EOF
