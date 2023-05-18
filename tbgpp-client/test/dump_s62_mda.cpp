#include <iostream>


#include "cache/chunk_cache_manager.h"



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


#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"


#include "BTNode.h"

#include "naucrates/traceflags/traceflags.h"


#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"


#include "catalog/catalog_wrapper.hpp"
#include "tbgppdbwrappers.hpp"

#include "mdprovider/MDProviderTBGPP.h"

#include "naucrates/md/IMDIndex.h"


using namespace gpopt;

using namespace duckdb;

void print_depth(string data, int depth=0) {
	for( int i = 0; i<depth; i++ ) {
		std::cout << "  ";
	}
	std::cout << data;
	std::cout << std::endl;
}

void PrintCatalogEntryOid(std::shared_ptr<ClientContext> client, Catalog &cat) {
	vector<string> ps_cat_list = {"vps_Post:Message", "vps_Comment:Message", "vps_Forum", "vps_Person", "eps_HAS_CREATOR"};
	for (auto &ps_cat_name : ps_cat_list) {
		PropertySchemaCatalogEntry *ps_cat =
			(PropertySchemaCatalogEntry *)cat.GetEntry(*client.get(), CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, ps_cat_name);
		fprintf(stdout, "%s oid %ld\n", ps_cat_name.c_str(), ps_cat->GetOid());
	}
	
	vector<string> index_cat_list = {"REPLY_OF_COMMENT_fwd", "REPLY_OF_fwd", "CONTAINER_OF_bwd", "HAS_MODERATOR_fwd", "HAS_CREATOR_bwd", "POST_HAS_CREATOR_fwd", "HAS_CREATOR_fwd"};
	for (auto &index_cat_name : index_cat_list) {
		IndexCatalogEntry *index_cat =
			(IndexCatalogEntry *)cat.GetEntry(*client.get(), CatalogType::INDEX_ENTRY, DEFAULT_SCHEMA, index_cat_name);
		fprintf(stdout, "%s oid %ld, AdjColIdx = %ld\n", index_cat_name.c_str(), index_cat->GetOid(), index_cat->GetAdjColIdx());
	}

	vector<string> agg_function_catalog_list = {"count_star"};	// count_star oid=33, mdid=72162688
	for (auto &agg_function_cat_name : agg_function_catalog_list) {
		AggregateFunctionCatalogEntry *aggf_cat =
			(AggregateFunctionCatalogEntry *)cat.GetEntry(*client.get(), CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, agg_function_cat_name);
		fprintf(stdout, "%s oid=%ld \n", agg_function_cat_name.c_str(), aggf_cat->GetOid());	// Note that oid is different from mdid
	}

}

void* mda_print(void* args) {

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	amp.Detach();

	InitDXL();
	// _orcaInitXForm
	CXformFactory::Pxff()->Shutdown();	// for allowing scan
	GPOS_RESULT eres = CXformFactory::Init();
	CMDCache::Init();


	IMDProvider* provider;
	gpmd::MDProviderTBGPP * pv = nullptr;
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		pv = new (mp, __FILE__, __LINE__) gpmd::MDProviderTBGPP(mp);
		// detach safety
		(void) amp.Detach();
	}
	D_ASSERT( pv != nullptr );
	pv->AddRef();
	provider = pv;

	CMDAccessor _mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, provider);

	auto m_cost_model_params = GPOS_NEW(mp) CCostModelParamsGPDB(mp);
	gpdbcost::CCostModelGPDB* pcm = GPOS_NEW(mp) CCostModelGPDB(mp, 1, m_cost_model_params);	// one segment

	COptimizerConfig *optimizer_config =
		COptimizerConfig::PoconfDefault(mp, pcm);
	// use the default constant expression evaluator which cannot evaluate any expression
	IConstExprEvaluator * pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
	COptCtxt *poctxt =
		COptCtxt::PoctxtCreate(mp, &_mda, pceeval, optimizer_config);
	poctxt->SetHasMasterOnlyTables();
	ITask::Self()->GetTls().Store(poctxt);
	

	CMDAccessor* mda = COptCtxt::PoctxtFromTLS()->Pmda();

	// now do whatever you want to do
	/*
		Post:Message 367
		Comment:Message 333
		Forum 391
		Person 305
		REPLY_OF_COMMENT_fwd 561
		REPLY_OF_fwd 543
		CONTAINER_OF_fwd 643
		HAS_MODERATOR_fwd 595
	
	*/
	vector<uint32_t> rel_ids_to_inspect({(uint32_t)305});	// 305 = vps_Person
	rel_ids_to_inspect.push_back(505);	// eps_IS_LOCATED_IN : 505
	rel_ids_to_inspect.push_back(521);	// vps_KNOWS : 521
	rel_ids_to_inspect.push_back(367);	// vps_Post:Message : 367
	rel_ids_to_inspect.push_back(465);	// eps_HAS_CREATOR : 465
	
	for (auto& rel_obj_id: rel_ids_to_inspect) {
		print_depth("[Inspecting Rel - mdid=" + std::to_string(rel_obj_id) + "]");
		auto *rel_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, rel_obj_id, 0, 0);
		auto *rel = mda->RetrieveRel(rel_mdid);
		CMDRelationGPDB *gpdb_rel = (CMDRelationGPDB *)rel;
		auto relname = gpdb_rel->Mdname().GetMDName();
		std::wstring relname_ws(relname->GetBuffer());
		string relname_str(relname_ws.begin(), relname_ws.end());
		print_depth("RelName: " + relname_str, 2);
		print_depth("HasDroppedColumns(): " + std::to_string(rel->HasDroppedColumns()), 2);
		print_depth("ColumnCount(): " + std::to_string(rel->ColumnCount()), 2);
		print_depth("SystemColumnsCount(): " + std::to_string(rel->SystemColumnsCount()), 2);
		for( gpos::ULONG i = 0; i < rel->ColumnCount(); i++ ) {
			auto* col_mdid = rel->GetMdCol(i);
			auto colname = col_mdid->Mdname().GetMDName();
			std::wstring colname_ws(colname->GetBuffer());
			string colname_str(colname_ws.begin(), colname_ws.end());
			print_depth(std::to_string(i)+" th column info =>" , 4);
			
			print_depth("MdName(): " + colname_str, 6);

			print_depth("AttrNum(): " + std::to_string(col_mdid->AttrNum()), 6);
			print_depth("IsSystemColumn(): " + std::to_string(col_mdid->IsSystemColumn()), 6);
			print_depth("IsDropped(): " + std::to_string(col_mdid->IsDropped()), 6);
		}
		print_depth("IndexCount(): " + std::to_string(mda->RetrieveRel(rel_mdid)->IndexCount()), 2);
		for( gpos::ULONG i = 0; i < mda->RetrieveRel(rel_mdid)->IndexCount(); i++ ) {
			IMDId* index_mdid = mda->RetrieveRel(rel_mdid)->IndexMDidAt(i);
			uint64_t idx_obj_id = (uint64_t) CMDIdGPDB::CastMdid(index_mdid)->Oid();
			const IMDIndex * index = mda->RetrieveIndex(index_mdid);
			print_depth(std::to_string(i) +" th index info => ", 4);
			std::wstring idxtyp_ws( index->GetDXLStr(index->IndexType())->GetBuffer() );
			string idxtype_str(idxtyp_ws.begin(), idxtyp_ws.end());
			print_depth("IndexCat_Oid: " + std::to_string(idx_obj_id), 6);
			print_depth("IndexType(): " + idxtype_str, 6);
			print_depth("Keys(): " + std::to_string(index->Keys()), 6);
			for (ULONG ul = 0; ul < index->Keys(); ul++) {
				// return the n-th key column
				print_depth("KeyAt("+std::to_string(ul)+"): " + std::to_string(index->KeyAt(ul)), 8);
			}
			print_depth("IncludedCols(): " + std::to_string(index->IncludedCols()), 6);
			for (ULONG ul = 0; ul < index->IncludedCols(); ul++) {
				// return the n-th included column
				print_depth("IncludedColAt("+std::to_string(ul)+"): " + std::to_string(index->IncludedColAt(ul)), 8);
			}
		
		}
		
		
	}


	CTaskLocalStorageObject *ptlsobj =
			ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt);
		ITask::Self()->GetTls().Remove(ptlsobj);
		GPOS_DELETE(ptlsobj);

	// shutdown
	CRefCount::SafeRelease(provider);
	CMDCache::Shutdown();

	return nullptr;
}

int main(int argc, char** argv) {
	

	// fprintf(stdout, "Initialize DiskAioParameters\n\n");
	// Initialize System Parameters
	DiskAioParameters::NUM_THREADS = 1;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
	DiskAioParameters::NUM_CPU_SOCKETS = 1;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = "/data/ldbc/sf1_test/";
	fprintf(stdout, "Workspace: %s\n", DiskAioParameters::WORKSPACE.c_str());
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

	// Initialize ChunkCacheManager
	// fprintf(stdout, "\nInitialize ChunkCacheManager\n");
	ChunkCacheManager::ccm = new ChunkCacheManager(DiskAioParameters::WORKSPACE.c_str());

	// Run Catch Test
	argc = 1;

	// Initialize Database
	// helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(DiskAioParameters::WORKSPACE.c_str());
	
	// Initialize ClientContext
	icecream::ic.disable();
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());
	duckdb::SetClientWrapper(client, make_shared<CatalogWrapper>(database->instance->GetCatalogWrapper()));

	PrintCatalogEntryOid(client, database->instance->GetCatalog());

	struct gpos_init_params gpos_params = {NULL};
	gpos_init(&gpos_params);
	gpdxl_init();
	gpopt_init();

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

	delete ChunkCacheManager::ccm;

	return 0;
}