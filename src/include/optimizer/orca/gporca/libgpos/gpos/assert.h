//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		assert.h
//
//	@doc:
//		Macros for assertions in gpos
//
//		Use '&& "explanation"' in assert condition to provide additional
//		information about the failed condition.
//
//			GPOS_ASSERT(!FSpinlockHeld() && "Must not hold spinlock during allcoation");
//
//		There is no GPOS_ASSERT_STOP macro, instead use
//
//			GPOS_ASSERT(!"Should not get here because of xyz");
//
//		Avoid using GPOS_ASSERT(false);
//---------------------------------------------------------------------------
#ifndef GPOS_assert_H
#define GPOS_assert_H

// #ifndef USE_CMAKE
// #include "optimizer/orca/pg_config.h"
// #endif


// retail assert; available in all builds
#define GPOS_RTL_ASSERT(x)                                                 \
	((x) ? ((void) 0)                                                      \
		 : gpos::CException::Raise(__FILE__, __LINE__,                     \
								   gpos::CException::ExmaSystem,           \
								   gpos::CException::ExmiAssert, __FILE__, \
								   __LINE__, GPOS_WSZ_LIT(#x)))

#ifdef GPOS_DEBUG
// standard debug assert; maps to retail assert in debug builds only
#define GPOS_ASSERT(x) GPOS_RTL_ASSERT(x)
#else
#define GPOS_ASSERT(x) ;
#endif	// !GPOS_DEBUG

// implication assert
#define GPOS_ASSERT_IMP(x, y) GPOS_ASSERT(!(x) || (y))

// if-and-only-if assert
#define GPOS_ASSERT_IFF(x, y) GPOS_ASSERT((!(x) || (y)) && (!(y) || (x)))

// compile assert
#define GPOS_CPL_ASSERT(x) extern int assert_array[(x) ? 1 : -1]

// debug assert, with message
#define GPOS_ASSERT_MSG(x, msg) GPOS_ASSERT((x) && (msg))

// retail assert, with message
#define GPOS_RTL_ASSERT_MSG(x, msg) GPOS_RTL_ASSERT((x) && (msg))


#endif	// !GPOS_assert_H

// EOF
