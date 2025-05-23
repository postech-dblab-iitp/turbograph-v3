//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnDescriptor.h
//
//	@doc:
//		Abstraction of columns in tables, functions, external tables etc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CColumnDescriptor_H
#define GPOPT_CColumnDescriptor_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/metadata/CName.h"
#include "naucrates/md/IMDType.h"

namespace gpopt
{
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColumnDescriptor
//
//	@doc:
//		Metadata abstraction for columns that exist in the catalog;
//		Transient columns as computed during query execution do not have a
//		column descriptor;
//
//---------------------------------------------------------------------------
class CColumnDescriptor : public CRefCount,
						  public DbgPrintMixin<CColumnDescriptor>
{
private:
	// type information
	const IMDType *m_pmdtype;

	// type modifier
	const INT m_type_modifier;

	// name of column -- owned
	CName m_name;

	// attribute number
	INT m_iAttno;

	// does column allow null values?
	BOOL m_is_nullable;

	// width of the column, for instance  char(10) column has width 10
	ULONG m_width;

	// is the column a distribution col
	BOOL m_is_dist_col;

	ULONG m_prop_id;

	ULONG m_node_id;

public:
	// ctor
	CColumnDescriptor(CMemoryPool *mp, const IMDType *pmdtype,
					  INT type_modifier, const CName &name, INT attno,
					  BOOL is_nullable, ULONG ulWidth = gpos::ulong_max);

	// dtor
	virtual ~CColumnDescriptor();

	// return column name
	const CName &
	Name() const
	{
		return m_name;
	}

	// return metadata type
	const IMDType *
	RetrieveType() const
	{
		return m_pmdtype;
	}

	// type modifier
	INT
	TypeModifier() const
	{
		return m_type_modifier;
	}

	// return attribute number
	INT
	AttrNum() const
	{
		return m_iAttno;
	}

	// does column allow null values?
	BOOL
	IsNullable() const
	{
		return m_is_nullable;
	}

	// is this a system column
	virtual BOOL
	IsSystemColumn() const
	{
		return (0 > m_iAttno);
	}

	// width of the column
	virtual ULONG
	Width() const
	{
		return m_width;
	}

	// is this a distribution column
	BOOL
	IsDistCol() const
	{
		return m_is_dist_col;
	}

	// set this column as a distribution column
	void
	SetAsDistCol()
	{
		m_is_dist_col = true;
	}

	void
	SetPropId(ULONG prop_id)
	{
		m_prop_id = prop_id;
	}

	ULONG
	PropId() const
	{
		return m_prop_id;
	}

	void
	SetNodeId(ULONG node_id)
	{
		m_node_id = node_id;
	}

	ULONG
	NodeId() const
	{
		return m_node_id;
	}

	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CColumnDescriptor
}  // namespace gpopt

#endif	// !GPOPT_CColumnDescriptor_H

// EOF
