//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalAllShortestPathGet.cpp
//
//	@doc:
//		Implementation of basic table access
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalAllShortestPathGet.h"

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
//		CLogicalAllShortestPathGet::CLogicalAllShortestPathGet
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalAllShortestPathGet::CLogicalAllShortestPathGet(CMemoryPool *mp)
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
//		CLogicalAllShortestPathGet::CLogicalAllShortestPathGet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalAllShortestPathGet::CLogicalAllShortestPathGet(CMemoryPool *mp, const CName *pnameAlias,
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
//		CLogicalAllShortestPathGet::~CLogicalAllShortestPathGet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalAllShortestPathGet::~CLogicalAllShortestPathGet()
{
	CRefCount::SafeRelease(m_ptabdescArray);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPathGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalAllShortestPathGet::HashValue() const
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
//		CLogicalAllShortestPathGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalAllShortestPathGet::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	// CLogicalAllShortestPathGet *popGet = CLogicalAllShortestPathGet::PopConvert(pop);

	// return m_ptabdesc->MDId()->Equals(popGet->m_ptabdesc->MDId()) &&
	// 	   m_pdrgpcrOutput->Equals(popGet->PdrgpcrOutput());
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPathGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalAllShortestPathGet::PopCopyWithRemappedColumns(CMemoryPool *mp,
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

	return GPOS_NEW(mp) CLogicalAllShortestPathGet(mp, pnameAlias, m_ptabdescArray, pdrgpcrOutput, path_join_lower_bound, path_join_upper_bound);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPathGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAllShortestPathGet::DeriveOutputColumns(CMemoryPool *mp,
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
//		CLogicalAllShortestPathGet::DeriveNotNullColumns
//
//	@doc:
//		Derive not null output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAllShortestPathGet::DeriveNotNullColumns(CMemoryPool *mp,
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
//		CLogicalAllShortestPathGet::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CLogicalAllShortestPathGet::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPathGet::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalAllShortestPathGet::DeriveKeyCollection(CMemoryPool *mp,
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
//		CLogicalAllShortestPathGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalAllShortestPathGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	// no direct xform, transformed only using index scan
	//(void) xform_set->ExchangeSet(CXform::ExfGet2TableScan);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPathGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalAllShortestPathGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
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
//		CLogicalAllShortestPathGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalAllShortestPathGet::OsPrint(IOstream &os) const
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
