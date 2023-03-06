#include <iostream>

#include "gpos/_api.h"
#include "naucrates/init.h"
#include "gpopt/init.h"

#include "unittest/gpopt/engine/CEngineTest.h"
#include "gpos/test/CUnittest.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "gpopt/operators/CLogicalGet.h"

#include "gpos/_api.h"
#include "gpos/common/CMainArgs.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CFSimulatorTestExt.h"
#include "gpos/test/CUnittest.h"
#include "gpos/types.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/init.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/base/CDistributionSpecStrictSingleton.h"

#include "planner/planner.hpp"

#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

#include "naucrates/traceflags/traceflags.h"



#include "mdprovider/MDProviderTBGPP.h"


void* mda_print(void* args) {

	std::cout << "mda_print() called" << std::endl;

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	amp.Detach();

	InitDXL();
	// _orcaInitXForm
	//CXformFactory::Pxff()->Shutdown();	// for allowing scan
	GPOS_RESULT eres = CXformFactory::Init();

	IMDProvider* provider;
	{
		gpmd::MDProviderTBGPP * pv = nullptr;
		CMemoryPool *mp = nullptr; 
		{
			CAutoMemoryPool amp;
			mp = amp.Pmp();
			pv = new (mp, __FILE__, __LINE__) gpmd::MDProviderTBGPP(mp);
			// detach safety
			(void) amp.Detach();
		}
		D_ASSERT( pv != nullptr );
		pv->AddRef();
		provider = pv ;
	}

	CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, provider);

	// now do whatever you want to do
	CMDIdGPDB rel_mdid(gpmd::IMDId::EMDIdType::EmdidRel, 0, 0, 0);

	std::cout << "RELMDID" << std::endl;
	std::cout << mda.RetrieveRel( (IMDId*) &rel_mdid)->ColumnCount() << std::endl;

	// shutdown
	CRefCount::SafeRelease(provider);
	CMDCache::Shutdown();

	return nullptr;
}

int main(int argc, char** argv) {

	gpos_exec_params params;
	int args;
	{
		params.func = mda_print;
		params.arg = &args;
		params.stack_start = &params;
		params.error_buffer = NULL;
		params.error_buffer_size = -1;
		params.abort_requested = NULL;
	}
	
	std::cout << "calling mda_print()" << std::endl;
	auto gpos_output_code = gpos_exec(&params);
	std::cout << "done" << std::endl;
	return 0;
}