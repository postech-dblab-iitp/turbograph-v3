#ifndef TURBOGRAPH_IMPL_H
#define TURBOGRAPH_IMPL_H

#include <thread>
#include <string>
#include <unistd.h>
#include <cstdlib>
#include "analytics/glog/logging.hpp"

#include "analytics/core/TurboDB.hpp"
#include "analytics/core/TG_NWSM.hpp"
#include "analytics/core/TG_DistributedVector.hpp"
#include "analytics/datastructure/VersionedBitMap.hpp"
#include "analytics/datastructure/GBVersionedBitMap.hpp"
// #include "analytics/datastructure/Aggregator.hpp"
#include "analytics/io/HomogeneousPageWriter.hpp"
#include "analytics/util/Aio_Helper.hpp"
#include "analytics/util/cxxopts.hpp"
#include "analytics/util/timer.hpp"

#include "tbb/concurrent_unordered_map.h"

#ifdef COVERAGE
extern "C" void __gcov_flush();
#endif


#ifndef MIN
#define MIN(x, y) ((x) < (y) ? (x) : (y));
#endif

//#define _MAX(a) _max(a)
#define ABS(a) _abs(a)
#define FMOD(a, b) std::fmod(a, b)
#define MOD(a, b) ((int64_t)a % (int64_t)b) 
#define LOG10(a) std::log10(a)

//#define CHECK_EQUALITY_VBM

template <typename T>
std::string to_string_with_precision(const T a_value, const int n = 6) {
	std::ostringstream out;
	out.precision(n);
	out << std::fixed << a_value;
	return out.str();
}

class TurbographImplementation {
	public:

		// System-provided APIs
		inline bool IsMarkedAtVOI(int lv, node_t vid) { return nwsm_->IsMarkedAtVOI(lv, vid); }
		inline void MarkAtVOI(int lv, node_t vid, bool activate=true) { 
			if (lv == 1) ActiveVertices.Initialize(vid, activate);
			return nwsm_->MarkAtVOI(lv, vid, activate); 
		}
		inline void MarkSubqueryAtVOI(int subquery_idx, int lv, node_t vid, bool activate=true) { 
			return nwsm_->MarkAtVOI(subquery_idx, lv, vid, activate); 
		}
		inline void MarkAtVOIAtApply(int lv, node_t vid, bool activate=true) { 
			if (lv == 1) ActiveVertices.Write(vid, activate);
			return nwsm_->MarkAtVOI(lv, vid, activate); 
		}
		inline void MarkAtVOIPrev(int lv, node_t vid, bool activate=true) { 
			ActiveVerticesPrev.Initialize(vid, activate); 
		}
		inline void MarkAtVOIPrevUnsafe(int lv, node_t vid, bool activate=true) { 
			ActiveVerticesPrev.InitializeUnsafe(vid, activate); 
		}
		inline void MarkAtVOIUnsafe(int lv, node_t vid, bool activate=true) { 
			if (lv == 1) ActiveVertices.InitializeUnsafe(vid, activate);
			return nwsm_->MarkAtVOIUnsafe(lv, vid, activate); 
		}
		inline void MarkAllAtVOI(int lv) { return nwsm_->MarkAllAtVOI(lv); }
		inline bool HasVertexInWindow(int lv, node_t vid) { return nwsm_->HasVertexInWindow(lv, vid); }
		inline bool HasVertexInWindow(int subquery_idx, int lv, node_t vid) { return nwsm_->HasVertexInWindow(subquery_idx, lv, vid); }
		inline void GetAdjList(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz) { return nwsm_->GetAdjList(lv, vid, data_ptr, data_sz); }
		inline void GetAdjList(int lv, node_t vid, node_t* data_ptr[], int64_t data_sz[]) { return nwsm_->GetAdjList(lv, vid, data_ptr, data_sz); }
		inline AdjacencyListMemoryPointer& GetAdjList(int lv, node_t vid) { return nwsm_->GetAdjList(lv, vid); }
		inline Range<node_t> GetVidRangeBeingProcessed(int lv) { return nwsm_->GetVidRangeBeingProcessed(lv); }
		inline bool dq_nodes(int subquery_idx, int lv, node_t vid) { 
			node_t idx = vid;
			if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
				idx = PartitionStatistics::DegreeOrderToVid(vid);
			}
			return nwsm_->dq_nodes(lv, subquery_idx, idx);
		}
		inline void set_dq_nodes(int subquery_idx, int lv, node_t vid) { 
			node_t idx = vid;
			//node_t idx = PartitionStatistics::DegreeOrderToVid(vid);
			nwsm_->set_dq_nodes(lv, subquery_idx, idx);
		}
		inline bool needGenerateNewUpdate(node_t vid) { 
			node_t idx = vid - PartitionStatistics::my_first_internal_vid();
			return genNew_vid_map.Get(idx);
		}
		inline bool needGenerateOldUpdate(node_t vid) { 
			node_t idx = vid - PartitionStatistics::my_first_internal_vid();
			return genOld_vid_map.Get(idx);
		}
		inline int64_t GetNbrSz(node_t u_nbr_sz[], DynamicDBType d_type) {
			int64_t nbr_sz = 0;
			ALWAYS_ASSERT(d_type != NEW);
			ALWAYS_ASSERT(u_nbr_sz != NULL);
			switch(d_type) {
				case OLD:
					nbr_sz += u_nbr_sz[0];
					break;
				case INSERT:
					nbr_sz += u_nbr_sz[1];
					break;
				case DELETE:
					nbr_sz += u_nbr_sz[2];
					break;
				case NEW:
					abort();
			}
			return nbr_sz;
		}

		// node_t* nbrs[] = [OLD | INSERT | DELETE]
		template <DynamicDBType ver>
			inline void ForNeighbor(node_t u1_nbrs_sz[], bool u1_d, std::function<void(node_t, node_t, bool)> func) {
				node_t begin = -1;
				node_t end = -1;
				if (ver == OLD) {
					bool u2_d = u1_d;
					begin = 0;
					end = begin + GetNbrSz(u1_nbrs_sz, OLD);
					func(begin, end, u2_d);
				} else if (ver == DELTA) {
					bool u2_d = u1_d;
					begin = GetNbrSz(u1_nbrs_sz, OLD);
					end = begin + GetNbrSz(u1_nbrs_sz, INSERT);
					func(begin, end, u2_d);
					u2_d = !u1_d;
					begin = GetNbrSz(u1_nbrs_sz, OLD) + GetNbrSz(u1_nbrs_sz, INSERT);
					end = begin + GetNbrSz(u1_nbrs_sz, DELETE);
					func(begin, end, u2_d);
				} else if (ver == NEW) {
					ForNeighbor<OLD>(u1_nbrs_sz, u1_d, func);
					ForNeighbor<DELTA>(u1_nbrs_sz, u1_d, func);
				} else {
					abort();
				}
			}

		template <DynamicDBType left_ver, CompareType left_ct_, DynamicDBType right_ver, CompareType right_ct_>
			inline void ForCommonNeighbor(node_t u1, node_t* u1_nbrs[], int64_t u1_nbrs_sz[], node_t u1_key_, node_t u2, node_t* u2_nbrs[], int64_t u2_nbrs_sz[], node_t u2_key_, bool u1_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
				bool u3_d;
				/*if ((left_ver == OLD || left_ver == INSERT) && (right_ver == OLD || right_ver == INSERT)) u3_d = u1_d;
					else if (left_ver == OLD && right_ver == DELETE) u3_d = !u1_d;
					else if (left_ver == DELETE && right_ver == OLD) u3_d = !u1_d;
					else if (left_ver == DELETE && right_ver == DELETE) u3_d = u1_d;*/
				const CompareType left_ct = (left_ct_ == NONE && right_ct_ != NONE) ? right_ct_ : left_ct_;
				const CompareType right_ct = (left_ct_ != NONE && right_ct_ == NONE) ? left_ct_ : right_ct_;
				node_t u1_key = u1_key_;
				node_t u2_key = u2_key_;
				if (left_ct_ == NONE && right_ct_ != NONE) {
					u1_key = u2_key;
				} else if (left_ct != NONE && right_ct_ == NONE) {
					u2_key = u1_key;
				}
				//const CompareType left_ct = left_ct_;
				//const CompareType right_ct = right_ct_;

				turbo_timer intersect_timer;

				if (left_ver == OLD) {
					if (right_ver == OLD) {
						u3_d = u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
					} else if (right_ver == DELTA) {
						u3_d = u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
						u3_d = !u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[2], u2_nbrs_sz[2], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
					} else if (right_ver == NEW) {
						INVARIANT(false);
						u3_d = u1_d;
					}
				} else if (left_ver == DELTA) {
					if (right_ver == OLD) {
						u3_d = u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
						u3_d = !u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[2], u1_nbrs_sz[2], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
					} else if (right_ver == DELTA) {
						u3_d = u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[2], u1_nbrs_sz[2], u1_key, u2, u2_nbrs[2], u2_nbrs_sz[2], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
						u3_d = !u1_d;
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[2], u2_nbrs_sz[2], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
						FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[2], u1_nbrs_sz[2], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
					} else if (right_ver == NEW) {
						ForCommonNeighbor<DELTA, left_ct_, OLD, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						ForCommonNeighbor<DELTA, left_ct_, DELTA, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						/*u3_d = u1_d;
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							u3_d = !u1_d;
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[2], u1_nbrs_sz[2], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[2], u1_nbrs_sz[2], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);*/
					}
				} else if (left_ver == NEW) {
					if (right_ver == OLD) {
						INVARIANT(false);
					} else if (right_ver == DELTA) {
						ForCommonNeighbor<OLD, left_ct_, DELTA, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						ForCommonNeighbor<DELTA, left_ct_, DELTA, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						/*u3_d = u1_d;
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							u3_d = !u1_d;
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[2], u2_nbrs_sz[2], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[2], u2_nbrs_sz[2], u2_key, u3_d, CheckPartialOrder, AggregateFunc);*/
					} else if (right_ver == NEW) { // XXX New = old + ins...? where is delete.........
						ForCommonNeighbor<OLD, left_ct_, OLD, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						ForCommonNeighbor<OLD, left_ct_, DELTA, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						ForCommonNeighbor<DELTA, left_ct_, OLD, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						ForCommonNeighbor<DELTA, left_ct_, DELTA, right_ct_> (u1, u1_nbrs, u1_nbrs_sz, u1_key_, u2, u2_nbrs, u2_nbrs_sz, u2_key_, u1_d, CheckPartialOrder, AggregateFunc);
						/*u3_d = u1_d;
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[0], u2_nbrs_sz[0], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[0], u1_nbrs_sz[0], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);
							FindCommonNeighbors<left_ct, right_ct>(u1, u1_nbrs[1], u1_nbrs_sz[1], u1_key, u2, u2_nbrs[1], u2_nbrs_sz[1], u2_key, u3_d, CheckPartialOrder, AggregateFunc);*/
					}
				}
			}


		template <typename T, Op op>
			inline void AllreduceAsync(BitMap<node_t>& bitmap, int queue_idx=0) { RequestRespond::TurboAllReduceInPlaceAsync<T, op>((char*) bitmap.GetBitMap(), bitmap.container_size(), sizeof(char), queue_idx); }
		template <typename T, Op op>
			inline void AllreduceAsync(TwoLevelBitMap<node_t>& bitmap, int queue_idx=0) { 
				RequestRespond::TurboAllReduceInPlaceAsync<T, op>((char*) bitmap.GetBitMap(), bitmap.container_size(), sizeof(char), queue_idx); 
				RequestRespond::TurboAllReduceInPlaceAsync<T, op>((char*) bitmap.two_level_data(), bitmap.two_level_container_size(), sizeof(char), queue_idx); 
			}
		inline void WaitAllreduceAsync() { RequestRespond::WaitAllreduce(); }
		inline void PopAllreduceAsync() { RequestRespond::PopAllreduce(); }

		inline void MarkDeletedVersion(node_t* old_vid, node_t deleted_version){
			ALWAYS_ASSERT(deleted_version >= 0);
			ALWAYS_ASSERT((deleted_version & ~(DELETE_MASKING_LSBS)) == 0);
			*old_vid = *old_vid | ((uint64_t) deleted_version << ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION));
			TurboDB::GetBufMgr()->GetDirectTable()->SetDirty(TG_ThreadContexts::pid_being_processed);
			return;
		}

		inline void ResetDeletedVersion(node_t* old_vid){
			*old_vid &= ~DELETE_MASKING_MSBS;
			TurboDB::GetBufMgr()->GetDirectTable()->SetDirty(TG_ThreadContexts::pid_being_processed);
			return;
		}

		// for deleted workload
		inline node_t GetDstVidValue(node_t vid) {
			return (vid) & ~(DELETE_MASKING_MSBS);
		}

		inline node_t GetDeletedVersion(node_t vid) {
			node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
			return deleted_version == 0 ? std::numeric_limits<node_t>::max(): deleted_version;
		}

		inline bool IsEdgeDeleted(node_t vid) {
			node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
			if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
			return deleted_version <= UserArguments::UPDATE_VERSION;
		}

		inline bool IsEdgeDeletedAtOld(node_t vid) {
			node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
			if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
			return deleted_version <= (UserArguments::UPDATE_VERSION - 1);
		}

		inline node_t GetVisibleDstVid(node_t& vid) {
			if (IsEdgeDeleted(vid)) return -1;
			else return GetDstVidValue(vid);
		}

		inline node_t GetVisibleDstVidAtOld(node_t& vid) {
			if (IsEdgeDeletedAtOld(vid)) return -1;
			else return GetDstVidValue(vid);
		}

		inline node_t GetVisibleDstVidAsDegreeOrder(node_t& vid) {
			if (IsEdgeDeleted(vid)) return -1;
			else return PartitionStatistics::VidToDegreeOrder(GetDstVidValue(vid));
		}

		void SetFinished(bool finish) { finished_ = finish; }

		TurbographImplementation() : argc_(0), argv_(NULL), options("query", "by syko") {
			need_OutDegree[0] = false;
			need_OutDegree[1] = false;
			need_OutDegree[2] = false;

			pTurboDB_[0] = new TurboDB();
			pTurboDB_[1] = new TurboDB();
			finished_ = false;
			iteration = 0;
			nwsm_ = NULL;
			UserArguments::MAX_LEVEL = 1;
			UserArguments::CURRENT_LEVEL = -1;

			prev_main_thread_cpu_time = 0;
			prev_time = 0;
			prev_cpu_time = 0;
			prev_openmp_threads_time = 0;
			prev_udf_cpu_time = 0;

			step1_backward_traversal_done = false;
		}

		~TurbographImplementation() {
		}

		// Users implement
		virtual void OpenVectors() {}
		virtual void Initialize() {}
		//virtual void InitializeVertices(bool flush=true) {}
		virtual void Initialize(node_t vid) {}
		virtual void DebugInitialize(node_t vid) {}
		virtual void DebugInitializePrev(node_t vid) {}
		virtual void InitializePrev(node_t vid) {}
		virtual void InitializeActiveVertices() {}
		virtual void InitializeOV(node_t src_vid) {
			for(auto& vec : vectors_) {
				if (vec->HasOv() && !vec->IsOvCachingAggregator()) {
					vec->ResetWriteBuffer(src_vid);
				}
			}
		}
		virtual void InitializeOVOfVector(TG_DistributedVectorBase* vec_, node_t src_vid) {
			vec_->ResetWriteBuffer(src_vid);
		}

		virtual TG_DistributedVectorBase* GetTargetVectorforPull() {}
		virtual void GenerateMessage(node_t vid, std::list<TG_DistributedVectorBase*> list_of_vectors) {
			for(auto& vec : list_of_vectors) {
				vec->PULL_StoreMessageDefault(vid);
			}
		}
		virtual void ClearGeneratedMessages(std::list<TG_DistributedVectorBase*> list_of_vectors) {
			for(auto& vec : list_of_vectors) {
				vec->PULL_ClearGeneratedMessages();
			}
		}

		virtual void Preprocessing() {}
		virtual void Postprocessing() {}
		virtual void Superstep(int argc, char** argv) = 0;

		virtual void FindPropertyChangedVertices() {
			bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
			std::list<TwoLevelBitMap<int64_t>*> list_of_bitmaps;
			for(auto& vec : vectors_) {
				if (vec->HasIv() && vec->IsReadOnlyPrevOpened()) {
#ifndef GB_VERSIONED_ARRAY
					list_of_bitmaps.push_back(vec->GetIvChangedBitMap());
#else
					list_of_bitmaps.push_back(vec->GetIvDirtyBitMap());
                    //fprintf(stdout, "IvDirty total = %ld\n", vec->GetIvDirtyBitMap()->GetTotal());
#endif
				}
			}
			if (list_of_bitmaps.size() == 0) {
			} else if (list_of_bitmaps.size() == 1) {
                //int64_t cnt_ = 0;
				if (sparse_mode) {
					list_of_bitmaps.front()->InvokeIfMarked([&](node_t idx) {
							node_t vid = idx + PartitionStatistics::my_first_internal_vid();
							if (isPropChanged(vid)) {
								prop_changed_vid_map.PushIntoList(idx);
							}
						}, true);
				} else {
					list_of_bitmaps.front()->InvokeIfMarked([&](node_t idx) {
							node_t vid = idx + PartitionStatistics::my_first_internal_vid();
							if (isPropChanged(vid)) {
								prop_changed_vid_map.Set(idx);
                                //std::atomic_fetch_add((std::atomic<int64_t>*) &cnt_, +1L);
							}
						});
                    //fprintf(stdout, "Prop_Changed total = %ld\n", cnt_);
				}
			} else {
				if (sparse_mode) {
					TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
							node_t vid = idx + PartitionStatistics::my_first_internal_vid();
							if (isPropChanged(vid)) {
								prop_changed_vid_map.PushIntoList(idx);
							}
						}, list_of_bitmaps, true);
				} else {
					TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
							node_t vid = idx + PartitionStatistics::my_first_internal_vid();
							if (isPropChanged(vid)) {
								prop_changed_vid_map.Set(idx);
							}
						}, list_of_bitmaps);
				}
			}
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
		}
		virtual bool isPropChanged(node_t vid) {
			bool ret_val = false;
			for(auto& vec : vectors_) {
				if (!vec->HasIv() || !vec->IsReadOnlyPrevOpened()) continue;
				ret_val = ret_val || vec->checkEqualityWithPrev(vid);
			}
			return ret_val;
		}
		virtual void GeneratePruningRules() {}

		virtual void DEBUG_PreprocessingPerSuperstep() {}
		virtual void DEBUG_PostprocessingPerSuperstep() {}
		virtual void DEBUG_PostprocessingBeforeFinalize() {}

		virtual bool PerSuperstepCheckFinished() {
			turbo_timer timer;
			int checkcontinue = 1;

			timer.start_timer(0);
#ifdef INCREMENTAL_PROFILING
			int64_t cnt = nwsm_->GetActiveVerticesBitMap().GetTotal();
			int64_t delta_nwsm_cnt = 0;
			delta_nwsm_cnt += nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT).GetTotal();
			delta_nwsm_cnt += nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE).GetTotal();

			//fprintf(stdout, "[%ld] (%ld, %ld) |ActiveVertices|: %ld %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, iteration, (int64_t) cnt, (int64_t) delta_nwsm_cnt);

			MPI_Allreduce(MPI_IN_PLACE, &cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
			MPI_Allreduce(MPI_IN_PLACE, &delta_nwsm_cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
			checkcontinue = (cnt > 0 || delta_nwsm_cnt > 0);
			system_fprintf(0, stdout, "(%ld, %ld) |ActiveVertices|: %ld %ld\n", UserArguments::UPDATE_VERSION, iteration, (int64_t) cnt, (int64_t) delta_nwsm_cnt);
			num_active_vertices = cnt;
#else
			system_fprintf(0, stdout, "Snapshot: %ld, Superstep: %ld\n", UserArguments::UPDATE_VERSION, iteration);
			checkcontinue = (nwsm_->HasActivatedVertex()) ? 1 : 0;
#endif
			timer.stop_timer(0);

			timer.start_timer(1);
			MPI_Allreduce(MPI_IN_PLACE, &checkcontinue, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);   // BARRIER, 어떻게 빠르게 하지
			//RequestRespond::TurboAllReduceSync<int64_t, PLUS>((char*) &checkcontinue, 1, sizeof(int64_t));
			timer.stop_timer(1);
			bool finished = false;
			if (checkcontinue == 0) {
				finished = true;
			}

			//#ifdef PRINT_PROFILING_TIMERS
			fprintf(stdout, "[%ld] PerSuperstepCheckFinished %.4f %.4f\n", PartitionStatistics::my_machine_id(), timer.get_timer(0), timer.get_timer(1));
			//#endif
			return finished;
		}


		void print_processor_info() {
			NumaHelper::print_numa_info();
		}

		virtual void run(int argc, char** argv) {
			run_(argc, argv);
			try {
				run_(argc, argv);
			} catch (std::exception& e) {
				// stack_dump();
				_exit(1);
			}
		}

		void InitializeOutDegreeVector() {
			turbo_timer tmr;
			tmr.start_timer(0);
			UserArguments::CURRENT_LEVEL = 0;
			bool degree_vector_initialized = false;
			std::string db_path = pTurboDB_[0]->GetDBPath();

			std::string outdegree_dir_name = db_path + "/OutDegree";
			std::string insertoutdegree_dir_name = db_path + "/OutDegreeDeltaInsert";
			std::string deleteoutdegree_dir_name = db_path + "/OutDegreeDeltaDelete";
			bool outDegreeFileExist = check_file_exists(outdegree_dir_name);

			for (int k = 0; k < 3; k++) {
				std::string vector_name = "OutDegree";
				if (k != 0) vector_name = vector_name + std::to_string(k);
				if ((need_OutDegree[k] || UserArguments::ITERATOR_MODE == FULL_LIST) && (k == 0)) {
					OutDegree[k].Open(RDWR, NOUSE, UserArguments::MAX_LEVEL, vector_name.c_str(), false, false, true, true, true);
				} else {
#ifndef GB_VERSIONED_ARRAY
					OutDegree[k].Open(RDWR, NOUSE, UserArguments::MAX_LEVEL, vector_name.c_str(), false, false, true, false, false);
#else
					OutDegree[k].Open(RDWR, NOUSE, UserArguments::MAX_LEVEL, vector_name.c_str(), false, false, true, true, true);
#endif
				}
				//OutDegree[k].SetAdvanceSuperstep(false);
				//OutDegree[k].SetConstructNextSuperStepArrayAsync(false);
				OutDegree[k].OpenWindow(RDWR, NOUSE, 1, false, false, false);
				register_vector(OutDegree[k]);
			}
			if (!outDegreeFileExist) InitializeOutDegreeVector(ALL);

			for (int k = 0; k < 3; k++) {
				if (need_OutDegree[k] || UserArguments::ITERATOR_MODE == FULL_LIST) continue;
				unregister_vector(OutDegree[k]);
				OutDegree[k].Close();
			}

			if (UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
				TurboDB::SetOutDegreeVectorBuffer((need_OutDegree[0] ? &OutDegree[0].Read(PartitionStatistics::my_first_node_id()) : nullptr), (need_OutDegree[1] ? &OutDegree[1].Read(PartitionStatistics::my_first_node_id()) : nullptr), (need_OutDegree[2] ? &OutDegree[2].Read(PartitionStatistics::my_first_node_id()) : nullptr));
			} else {
				TurboDB::SetOutDegreeVectorBuffer(&OutDegree[0].Read(PartitionStatistics::VidToDegreeOrder(PartitionStatistics::my_first_node_id())), &OutDegree[1].Read(PartitionStatistics::VidToDegreeOrder(PartitionStatistics::my_first_node_id())), &OutDegree[2].Read(PartitionStatistics::VidToDegreeOrder(PartitionStatistics::my_first_node_id())));
			}
			TurboDB::is_out_degree_vector_initialized_ = true;
			tmr.stop_timer(0);
			fprintf(stdout, "[%ld] InitializeOutDegreeVector Elapsed: %.2f\n", PartitionStatistics::my_machine_id(), tmr.get_timer(0));
		}

		void InitializeOutDegreeVector(DynamicDBType d_type) {
			system_fprintf(0, stdout, "OutDegree Vector is not initialized. Create BEGIN\n");

			std::vector<TG_DistributedVectorBase*> tmp_vector_container;
			for(auto& vec : vectors_) {
				tmp_vector_container.push_back(vec);
			}
			vectors_.clear();

			for (int k = 0; k < 3; k++) {
#ifndef GB_VERSIONED_ARRAY
				OutDegreeTemp[k].Open(RDWR, NOUSE, UserArguments::MAX_LEVEL, "", false, false, false, false);
#else
				OutDegreeTemp[k].Open(RDWR, NOUSE, UserArguments::MAX_LEVEL, "", false, false, false, true);
#endif
				OutDegreeTemp[k].OpenWindow(RDWR, NOUSE, 1, false, false, false);
				register_vector(OutDegreeTemp[k]);
				register_vector(OutDegree[k]);
				OutDegreeTemp[k].AllocateMemory();
				OutDegree[k].AllocateMemory();
			}

			IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;
			bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
			int64_t prev_num_subchunks_per_edge_chunk = PartitionStatistics::num_subchunks_per_edge_chunk();
			int64_t prev_max_level = UserArguments::MAX_LEVEL;
			bool prev_use_fullist_db = UserArguments::USE_FULLIST_DB;
			UserArguments::ITERATOR_MODE = PARTIAL_LIST;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
			UserArguments::USE_FULLIST_DB = false;
			UserArguments::MAX_LEVEL = 1;
			UserArguments::INC_STEP = -1;
			TG_NWSM<TurbographImplementation> nwsm;
			nwsm.InitUserProgram(this);

			ProcessVertices([&](node_t vid) { 
					for (int k = 0; k < 3; k++) {
						OutDegreeTemp[k].Read<1>(vid) = 0;
					}
				}, false);

			for (int ver = 0; ver <= UserArguments::MAX_VERSION; ver++) {
				ClearInputVectorFlags();

				AdvanceUpdateVersion(ver);    
				UserArguments::UPDATE_VERSION = ver;
				UserArguments::SUPERSTEP = 0;

				nwsm.SetApplyFunction(&TurbographImplementation::_ApplyforDegree);

				ProcessVertices([&](node_t vid) { 
						nwsm.MarkAtVOI(1, vid); 
					}, false);
				nwsm.AddScatter(&TurbographImplementation::_MultiplyforInsertDegree, 1);
				nwsm.Inc_Start(Range<int>(ver, ver), OUTEDGE, INSERT, false, false, false);

				ProcessVertices([&](node_t vid) { 
						nwsm.MarkAtVOI(1, vid); 
					}, false);
				nwsm.AddScatter(&TurbographImplementation::_MultiplyforDeleteDegree, 1);
				nwsm.Inc_Start(Range<int>(ver, ver), OUTEDGE, DELETE, false, false, false);

				if (ver >= 1) {
					ProcessVertices([&](node_t vid) { 
							nwsm.MarkAtVOI(1, vid); 
						}, false);
					nwsm.AddScatter(&TurbographImplementation::_Dummy, 1);
					nwsm.Inc_Start(Range<int>(ver, ver), INEDGE, INSERT, false, false, false);
					ProcessVertices([&](node_t vid) { 
							nwsm.MarkAtVOI(1, vid); 
						}, false);
					nwsm.AddScatter(&TurbographImplementation::_Dummy, 1);
					nwsm.Inc_Start(Range<int>(ver, ver), INEDGE, DELETE, false, false, false);
				}

				UserArguments::SUPERSTEP = -1;

				nwsm.Inc_Start(Range<int>(ver, ver), OUTEDGE, INSERT, false, true, true,
						[&] (node_t vid) {
							LOG_ASSERT(false); 
						}, [&](node_t vid) { 
							LOG_ASSERT(false); 
						}, [&] (node_t vid) {
							return true;
						}, [&]() {
#pragma omp parallel for
							for (node_t vid = PartitionStatistics::my_first_node_id();  vid <= PartitionStatistics::my_last_node_id(); vid++) {
								_ApplyforDegree(vid);
							}  
						});
				ResetNumProcessedEdges();
				Turbo_bin_aio_handler::WaitForAllPendingDiskIoOfAllInterfaces(false, true);
				if (ver == 0) {
					TurboDB::GetBufMgr()->FlushDirtyPages();
					TurboDB::GetBufMgr()->ClearAllFrames();
				}
			}

			for (int k = 0; k < 3; k++) {
				if (!need_OutDegree[k]) continue;
				ProcessVertices([&](node_t vid) { 
						for (int k = 0; k < 3; k++) {
						OutDegree[k].Read<1>(vid) = 0;
						}
						}, false);
			}

			nwsm.Close();
			//TurboDB::is_out_degree_vector_initialized_ = true;
			UserArguments::ITERATOR_MODE = prev_iterator_mode;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
			UserArguments::USE_FULLIST_DB = prev_use_fullist_db;
			UserArguments::MAX_LEVEL = prev_max_level;
			UserArguments::SUPERSTEP = 0;
			PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;

			for (int k = 0; k < 3; k++) {
				OutDegreeTemp[k].Close();
				OutDegree[k].ClearIvFlagsForIncrementalProcessing(true, true);
				OutDegree[k].SetCurrentVersion(0, -1); //XXX right?
				OutDegree[k].SetConstructedAlready(true);
				OutDegree[k].SetAdvanceSuperstep(false); 
			}
			vectors_.clear();

			for(auto& vec : tmp_vector_container) {
				vectors_.push_back(vec);
			}
			system_fprintf(0, stdout, "OutDegree Vector is not initialized. Create END:\n");
		}

		void ResetDelete() {
			system_fprintf(0, stdout, "ResetDelete BEGIN:\n\n", PartitionStatistics::my_machine_id());
			std::vector<TG_DistributedVectorBase*> tmp_vector_container;
			for(auto& vec : vectors_) {
				tmp_vector_container.push_back(vec);
			}
			vectors_.clear();

			IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;
			bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
			bool prev_use_fullist_db = UserArguments::USE_FULLIST_DB;
			int64_t prev_max_level = UserArguments::MAX_LEVEL;
			int prev_update_version = UserArguments::UPDATE_VERSION;
			int64_t prev_num_subchunks_per_edge_chunk = PartitionStatistics::num_subchunks_per_edge_chunk();
			UserArguments::ITERATOR_MODE = PARTIAL_LIST;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
			UserArguments::MAX_LEVEL = 1;
			UserArguments::INC_STEP = -1;
			
			TG_NWSM<TurbographImplementation> nwsm;
			nwsm.InitUserProgram(this);
			if (UserArguments::USE_FULLIST_DB) {
				PartitionStatistics::num_subchunks_per_edge_chunk() = 1;
				std::list<EdgeType> edge_types = { OUTEDGE, INEDGE };

				for (auto& edge_type: edge_types) {
					for (int ver = 0; ver <= UserArguments::MAX_VERSION; ver++) {
						UserArguments::UPDATE_VERSION = ver;
						UserArguments::SUPERSTEP = 0;

						nwsm.MarkAllAtVOI(1);
						nwsm.AddScatter(&TurbographImplementation::_ResetDelete, 1);
						nwsm.Inc_Start(Range<int>(ver, ver), edge_type, ALL, false, false, false);
					}
				}
				PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;
				TurboDB::GetBufMgr()->FlushDirtyPages();
				TurboDB::GetBufMgr()->ClearAllFrames();
			}

			UserArguments::USE_FULLIST_DB = false;
			std::list<EdgeType> edge_types = { OUTEDGE, INEDGE };

			for (auto& edge_type: edge_types) {
				for (int ver = 0; ver <= UserArguments::MAX_VERSION; ver++) {
					UserArguments::UPDATE_VERSION = ver;
					UserArguments::SUPERSTEP = 0;

					nwsm.MarkAllAtVOI(1);
					nwsm.AddScatter(&TurbographImplementation::_ResetDelete, 1);
					nwsm.Inc_Start(Range<int>(ver, ver), edge_type, ALL, false, false, false);
				}
			}
			TurboDB::GetBufMgr()->FlushDirtyPages();
			TurboDB::GetBufMgr()->ClearAllFrames();
			nwsm.Close();
			
			UserArguments::USE_FULLIST_DB = prev_use_fullist_db;
			UserArguments::ITERATOR_MODE = prev_iterator_mode;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
			UserArguments::MAX_LEVEL = prev_max_level;
			UserArguments::SUPERSTEP = iteration = 0;
			UserArguments::UPDATE_VERSION = prev_update_version;
			PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;

			for(auto& vec : tmp_vector_container) {
				vectors_.push_back(vec);
			}
			system_fprintf(0, stdout, "ResetDelete END:\n\n", PartitionStatistics::my_machine_id());
		}

		void ApplyDeleteDBToInsertDB() {
			system_fprintf(0, stdout, "\nApply Delete DB To Insert DB BEGIN:\n", PartitionStatistics::my_machine_id());
			turbo_timer tmr;
			tmr.start_timer(0);

			std::vector<TG_DistributedVectorBase*> tmp_vector_container;
			for(auto& vec : vectors_) {
				tmp_vector_container.push_back(vec);
			}
			vectors_.clear();

			IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;
			bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
			bool prev_use_full_list_db = UserArguments::USE_FULLIST_DB;
			int64_t prev_max_level = UserArguments::MAX_LEVEL;
			int prev_update_version = UserArguments::UPDATE_VERSION;
			int64_t prev_num_subchunks_per_edge_chunk = PartitionStatistics::num_subchunks_per_edge_chunk();
			UserArguments::ITERATOR_MODE = PARTIAL_LIST;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
			UserArguments::MAX_LEVEL = 1;
			UserArguments::INC_STEP = -1;
			
			TG_NWSM<TurbographImplementation> nwsm;
			nwsm.InitUserProgram(this);
			
			if (UserArguments::USE_FULLIST_DB) {
				PartitionStatistics::num_subchunks_per_edge_chunk() = 1;

				std::list<EdgeType> edge_types = { OUTEDGE, INEDGE };
				for (auto& edge_type: edge_types) {
					for (int ver = 0; ver <= UserArguments::MAX_VERSION; ver++) {
						if (ver == 0) continue;
						//if (!TurboDB::GetTurboDB(edge_type)->is_version_exist(DELETE, ver)) continue;
						UserArguments::UPDATE_VERSION = ver;
						UserArguments::SUPERSTEP = 0;

						// Load Delete DB edges into hash-map
						nwsm.MarkAllAtVOI(1);
						nwsm.AddScatter(&TurbographImplementation::_LoadDeleteDBToHashMap, 1);
						nwsm.Inc_Start(Range<int>(ver, ver), edge_type, DELETE, false, false, false);
						nwsm.ClearAllAtVOI(1);

						//#pragma omp parallel for
						std::for_each(begin(edge_src_map), end(edge_src_map), [&] (const std::pair<node_t, node_t>& src){
								nwsm.MarkAtVOI(1, src.first);
							});

						nwsm.AddScatter(&TurbographImplementation::_FindDeleteEdgesFromHashMap, 1);
						nwsm.Inc_Start(Range<int>(0, ver-1), edge_type, INSERT, false, false, false);

						edge_map.clear();
						edge_src_map.clear();
					}
				}
				PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;
				//TurboDB::GetBufMgr()->FlushDirtyPages();
				//TurboDB::GetBufMgr()->ClearAllFrames();
			}
			
			UserArguments::USE_FULLIST_DB = false;
			std::list<EdgeType> edge_types = { OUTEDGE, INEDGE };
			for (auto& edge_type: edge_types) {
				for (int ver = 0; ver <= UserArguments::MAX_VERSION; ver++) {
					if (ver == 0) continue;
					//if (!TurboDB::GetTurboDB(edge_type)->is_version_exist(DELETE, ver)) continue;
					UserArguments::UPDATE_VERSION = ver;
					UserArguments::SUPERSTEP = 0;

					// Load Delete DB edges into hash-map
					nwsm.MarkAllAtVOI(1);
					nwsm.AddScatter(&TurbographImplementation::_LoadDeleteDBToHashMap, 1);
					nwsm.Inc_Start(Range<int>(ver, ver), edge_type, DELETE, false, false, false);
					nwsm.ClearAllAtVOI(1);

					//#pragma omp parallel for
					std::for_each(begin(edge_src_map), end(edge_src_map), [&] (const std::pair<node_t, node_t>& src){
							nwsm.MarkAtVOI(1, src.first);
						});

					nwsm.AddScatter(&TurbographImplementation::_FindDeleteEdgesFromHashMap, 1);
					nwsm.Inc_Start(Range<int>(0, ver-1), edge_type, INSERT, false, false, false);

					edge_map.clear();
					edge_src_map.clear();
				}
			}
			//TurboDB::GetBufMgr()->FlushDirtyPages();
			//TurboDB::GetBufMgr()->ClearAllFrames();

			UserArguments::USE_FULLIST_DB = prev_use_full_list_db;
			UserArguments::ITERATOR_MODE = prev_iterator_mode;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
			UserArguments::MAX_LEVEL = prev_max_level;
			UserArguments::SUPERSTEP = iteration = 0;
			UserArguments::UPDATE_VERSION = prev_update_version;
			PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;

			for(auto& vec : tmp_vector_container) {
				vectors_.push_back(vec);
			}
			tmr.stop_timer(0);
			//system_fprintf(0, stdout, "Apply Delete DB To Insert DB END:\n\n", PartitionStatistics::my_machine_id());
			fprintf(stdout, "[%ld] Apply Delete DB To Insert DB END: Elapsed %.2f\n\n", PartitionStatistics::my_machine_id(), tmr.get_timer(0));
		}

		void LoadGraphIntoMemory(int ver) {
			system_fprintf(0, stdout, "\nLoad Graph Into Memory BEGIN:\n", PartitionStatistics::my_machine_id());

			std::vector<TG_DistributedVectorBase*> tmp_vector_container;
			for(auto& vec : vectors_) {
				tmp_vector_container.push_back(vec);
			}
			vectors_.clear();

			IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;
			bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
			int64_t prev_max_level = UserArguments::MAX_LEVEL;
			int prev_update_version = UserArguments::UPDATE_VERSION;
			int prev_num_threads = UserArguments::NUM_THREADS;
			int64_t prev_num_subchunks_per_edge_chunk = PartitionStatistics::num_subchunks_per_edge_chunk();
			UserArguments::ITERATOR_MODE = PARTIAL_LIST;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
			UserArguments::MAX_LEVEL = 1;
			UserArguments::INC_STEP = -1;
			UserArguments::NUM_THREADS = 1;
			UserArguments::GRAPH_IN_MEMORY = false;
			if (UserArguments::USE_FULLIST_DB) {
				PartitionStatistics::num_subchunks_per_edge_chunk() = 1;
			}
			TG_NWSM<TurbographImplementation> nwsm;
			nwsm.InitUserProgram(this);
			if (TurboDB::graph_in_memory.size() == 0)
				TurboDB::graph_in_memory.resize(PartitionStatistics::my_num_internal_nodes());

			std::list<EdgeType> edge_types = { OUTEDGE };
			for (auto& edge_type: edge_types) {
				UserArguments::UPDATE_VERSION = ver;
				UserArguments::SUPERSTEP = 0;

				nwsm.MarkAllAtVOI(1);
				nwsm.AddScatter(&TurbographImplementation::_LoadDBToInMemory, 1);
				nwsm.Inc_Start(Range<int>(ver, ver), edge_type, INSERT, false, false, false);
			}

			int64_t num_total_processed_edges = 0;
			for (int64_t src_vid = 0; src_vid < TurboDB::graph_in_memory.size(); src_vid++) {
				num_total_processed_edges += TurboDB::graph_in_memory[src_vid].size();
			}
			fprintf(stdout, "Num edges = %ld\n", num_total_processed_edges);

			nwsm.Close();
			UserArguments::GRAPH_IN_MEMORY = true;
			UserArguments::ITERATOR_MODE = prev_iterator_mode;
			UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
			UserArguments::MAX_LEVEL = prev_max_level;
			UserArguments::NUM_THREADS = prev_num_threads;
			UserArguments::SUPERSTEP = iteration = 0;
			UserArguments::UPDATE_VERSION = prev_update_version;
			PartitionStatistics::num_subchunks_per_edge_chunk() = prev_num_subchunks_per_edge_chunk;

			for(auto& vec : tmp_vector_container) {
				vectors_.push_back(vec);
			}
			system_fprintf(0, stdout, "Load Graph Into Memory END:\n\n", PartitionStatistics::my_machine_id());
		}

		void InitializeNestedWindowedStreamBuffer() {
			INVARIANT(nwsm_ != NULL);
			nwsm_->InitializeNestedWindowedStreamBuffer();
		}

		void InitializeIOStats() {
			DiskAioFactory::GetPtr()->ResetStats();
			TurboDB::GetBufMgr()->ResetPageReadIO();
			TurboDB::GetBufMgr()->ResetPageWriteIO();
			turbo_tcp::ResetBytesSentAndReceived();
			TurboDB::GetBufMgr()->ReportTimers(true);

			prev_page_io = 0;
			prev_network_io = 0;
			prev_versioned_array_io = 0;
		}

		virtual void run_(int argc, char** argv) {
			ParameterInitialize(argc, argv);
			SystemInitialize();

			OpenVectors();

			AutoTuneBufferPoolSize();
			InitializeOutDegreeVector();
			RebuildDBIfNeeded();

			AllocateMemoryForVectors();

			Initialize();

			InitializeNestedWindowedStreamBuffer();
			LocalStatistics::print_mem_alloc_info();
			StartTimers();
			while (iteration < MaxIters && !finished_) {
				Superstep(argc_, argv_);
				iteration++;

				MeasureCpuTimes(get_main_thread_cpu_time(), cpu_timer_.stop_timer(1), get_openmp_threads_cpu_time(), RequestRespond::rr_.async_udf_pool.GetTotalCpuTime());
			}
			EndTimers(get_main_thread_cpu_time(), cpu_timer_.stop_timer(1), get_openmp_threads_cpu_time(), RequestRespond::rr_.async_udf_pool.GetTotalCpuTime());

			Postprocessing();

			CloseVectors();
			SystemFinalize();
		}

		void OpenTemporaryVectorsForIncrementalProcessing() {
			StartingVertices.Open(RDONLY, WRONLY, UserArguments::MAX_LEVEL, "starting_vertices", false, true, false, false, false, false);
			//StartingVertices.SetAdvanceSuperstep(false);
			//StartingVertices.SetConstructNextSuperStepArrayAsync(false);
			StartingVertices.OpenWindow(RDONLY, WRONLY, 1, false, false, false);

			StartingVertices.SetOvDistributive(false, false);

			register_vector(StartingVertices);
			vectors1.insert(&StartingVertices);
			//std::string vector_dir_path = "/mnt/tmpfs/" + UserArguments::WORKSPACE_PATH + "/active_vertices";
			std::string vector_dir_path = UserArguments::WORKSPACE_PATH + "/active_vertices";
			CreateDirectory(vector_dir_path);
			std::string iv_path = UserArguments::WORKSPACE_PATH + "/active_vertices/iv";
			std::string iv_ss_path = iv_path + "_superstep" + std::to_string(UserArguments::SUPERSTEP);
			ActiveVertices.Init(iv_path, PartitionStatistics::my_num_internal_nodes(), false, false);
			ActiveVertices.Create();
			ActiveVerticesPrev.InitReadOnly(ActiveVertices);
			ActiveVerticesPrev.CreateReadOnly();
#ifdef GB_VERSIONED_ARRAY
            ActiveVertices.SetPrev(ActiveVerticesPrev);
#endif
		}

		virtual void inc_run(int argc, char** argv) {
			ParameterInitialize(argc, argv);
			SystemInitialize();
			OpenVectors();
			OpenTemporaryVectorsForIncrementalProcessing();
			AutoTuneBufferPoolSize();
			AllocateMemoryForVectors();

			ResetDelete();
			ApplyDeleteDBToInsertDB();
			InitializeOutDegreeVector();
			//LoadGraphIntoMemory(0); // XXX
			RebuildDBIfNeeded();

			TurboDB::GetBufMgr()->FlushDirtyPages();
			//TurboDB::GetBufMgr()->ClearAllFrames();
			DiskAioFactory::GetPtr()->ResetStats();

			//AllocateMemoryForVectors();

			Initialize();

			InitializeNestedWindowedStreamBuffer();
			LocalStatistics::print_mem_alloc_info();

			Range<int> version_range(0, UserArguments::MAX_VERSION);
			EdgeType edge_type;
			DynamicDBType db_type;

			page_io_of_steps.resize(5);
			network_io_of_steps.resize(5);
			versioned_array_io_of_steps.resize(5);
			num_processed_edges_of_steps.resize(5); // there are 4 steps
			num_processed_pages_of_steps.resize(5);
			num_processed_empty_pages_of_steps.resize(5);
			page_io_of_steps_json.resize(5);
			network_io_of_steps_json.resize(5);
			versioned_array_io_of_steps_json.resize(5);
			num_processed_edges_of_steps_json.resize(5); // there are 4 steps
			num_processed_pages_of_steps_json.resize(5);
			num_processed_empty_pages_of_steps_json.resize(5);

			execution_times_of_steps.resize(5); // there are 4 steps
			active_vertices_of_steps.resize(4); // there are 4 steps
			num_edges_in_pages_of_steps.resize(5);
			nwsm_time_breaker.resize(5);
			time_breaker.resize(17);
			versioned_vertices.resize(9);
			for (int i = 0; i < nwsm_time_breaker.size(); i++) {
				nwsm_time_breaker[i].resize(11);
			}

			bool prev_run_static_processing = false;
			bool run_static_processing = false;
			int64_t max_num_iterations_in_prev_execution = -1;

			//version_range.SetBegin(version_range.GetEnd());
			//version_range.SetEnd(version_range.GetBegin());
			//version_range.SetEnd(version_range.GetBegin() + 1);
			UserArguments::BEGIN_VERSION = version_range.GetBegin();
			UserArguments::END_VERSION = version_range.GetEnd();
      		UserArguments::QUERY_ONGOING = true;

			//for (int ver = version_range.GetEnd() - 1; ver <= version_range.GetEnd(); ver++) {
			//for (int ver = version_range.GetEnd(); ver <= version_range.GetEnd(); ver++) {
			//for (int ver = version_range.GetBegin(); ver <= version_range.GetBegin() + 2; ver++) {
			for (int ver = version_range.GetBegin(); ver <= version_range.GetEnd(); ver++) {
			    //char n; std::cout << "Enter n: "; std::cin >> n;
				InitializeIOStats();
				CompletePendingAioRequests();   // Isn't it a cheat?
				AdvanceUpdateVersion(ver);
				if (ActiveVertices.GetNumberOfUpdateVersions() <= ver) {
					ActiveVertices.AdvanceUpdateVersion(ver);
				}
				nwsm_->ClearAllAtVOI(1);

				PartitionStatistics::wait_for_all();    // BARRIER
				system_fprintf(0, stdout, "Process version %ld, MaxIters %ld, finished? %ld\n", ver, MaxIters, finished_?1:0); 
				finished_ = false;
				UserArguments::UPDATE_VERSION = ver;
				UserArguments::SUPERSTEP = iteration = 0;
				step1_backward_traversal_done = false;

				ResetTimers();
				StartTimers();

				main_ver.Set(0, ver);
				prev_main_ver.Set(0, ver-1);
				delta_ver.Set(ver, ver);
				double steps[5] = {0.0f};

				perf_timer_.clear_all_timers();

				Version delta_version_to_read;
				skip_step3and4 = 0;
				step2_processed = true;
				sparse_mode = false;

				while (iteration < MaxIters && !finished_) {

					perf_timer_.start_timer(11);
					snapshots.push_back(ver);
					supersteps.push_back(iteration);
					Version from_version (0, iteration);
					delta_version_to_read.Set(ver - 1, iteration);
					int64_t delta_size = 0;
					for (auto& vec : vectors_) {
						delta_size += vec->GetTotalDeltaSizeInDisk(from_version, delta_version_to_read);
					}
					delta_size += ActiveVertices.GetTotalDeltaSizeInDisk(from_version, delta_version_to_read);
					accum_delta_size.push_back(delta_size);

					if (PartitionStatistics::my_machine_id() == 0) {
						std::string tag = std::to_string(ver) + "_" + std::to_string(iteration);
						printProcessMemoryUsage(tag.c_str());
					}

					UserArguments::SUPERSTEP = iteration;


					bool first_snapshot = (ver == version_range.GetBegin());
					run_static_processing = first_snapshot || RunStaticOrIncremental(ver);

					char* run_something = getenv("TBGPP_RUN_MODE");
					if (run_something == NULL) {
						system_fprintf(0, stdout, "Running dynamic\n");
						// dynamic
					} else {
						std::string mode = std::string(run_something);
						system_fprintf(0, stdout, "Running %s\n", mode.c_str());
						if (mode == "Static") {
							run_static_processing = true;
						} else if (mode == "IncrementalToStatic") {
							if (ver > 0) run_static_processing = (iteration > 2) == 0;
							abort();
						} else if (mode == "Alternating") {
							abort();
						}
					}
					perf_timer_.stop_timer(11);
					perf_timer_.start_timer(5);
					if (iteration == 0) {
						// Initialize ActiveVertices & Construct Next Superstep Array Async & Set version
#ifndef GB_VERSIONED_ARRAY
						ActiveVertices.ClearAll();
#endif
						ActiveVertices.CallConstructNextSuperStepArrayAsync(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, run_static_processing);
						ActiveVertices.SetCurrentVersion(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
						if (UserArguments::UPDATE_VERSION >= 1) {
#ifndef GB_VERSIONED_ARRAY
							ActiveVerticesPrev.ClearAll();
#endif
							ActiveVerticesPrev.SetCurrentVersion(UserArguments::UPDATE_VERSION - 1, UserArguments::SUPERSTEP);
						}

						// Construct Next Superstep Array for Vectors & Set version
						for (auto& vec : vectors_) {
							if (!vec->CheckConstructNextSuperStepArrayAsync()) continue;
							vec->CallConstructNextSuperStepArrayAsync(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, run_static_processing);
							vec->SetCurrentVersion(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
						}
					}
					if (step2_processed || (skip_step3and4 == 0)) {
						TG_DistributedVectorBase::ClearAllReapplyRequired();
						TG_DistributedVectorBase::ClearOvMessageReceivedAll();
					}
					perf_timer_.stop_timer(5);
					if (run_static_processing) {
						perf_timer_.start_timer(4);
						if (iteration == 0 || !prev_run_static_processing) {
							if (iteration == 0) {
								ProcessVertices([&](node_t vid) {
										Initialize(vid);
										}, false);
								ActiveVertices.ClearFlags();
								ProcessVertices([&](node_t vid) {
										InitializeOV(vid);
										}, false);
							}
						}
						UserArguments::USE_DELTA_NWSM = false;
						UserArguments::INC_STEP = -1;
						StartingVertices.SetSkipIv(true);
						edge_type = UserArguments::EDGE_DB_TYPE;
						db_type = UserArguments::DYNAMIC_DB_TYPE;

						PrintNumberOfActiveVertices(ver, iteration);
						LogVersionedVerticesStates(); // dV_active, ...
						LogNumberOfActiveVerticesPerStep(); // per step
						unregister_vector(StartingVertices);

						DEBUG_PreprocessingPerSuperstep();
						reapply_vid_map.ClearAll();

						nwsm_->Start(main_ver, edge_type, db_type,
								[&](node_t vid) {
								node_t idx = vid - PartitionStatistics::my_first_internal_vid();
#ifdef INCREMENTAL_LOGGING
								if (false && INCREMENTAL_DEBUGGING_TARGET(vid)) {
								fprintf(stdout, "Activate(%ld)?\t%ld\n", (int64_t) vid, (int64_t) nwsm_->IsActive(vid)?1L:0L);
								}
#endif

								Apply(vid);
#ifndef GB_VERSIONED_ARRAY
								InitializeOV(vid);
#endif
								ActiveVertices.Write(vid, nwsm_->IsActive(vid));
								reapply_vid_map.Set_Atomic(idx); 

								}, [&](node_t vid) { 
								return true; 
								}, [&]() {
								// Invoke if ~reapply AND (Active != nwsm_->Active)
								TwoLevelBitMap<int64_t>::ComplexOperation3(reapply_vid_map, ActiveVertices.GetBitMap(), nwsm_->GetActiveVerticesBitMap(), [&](node_t idx) {
									node_t vid = idx + PartitionStatistics::my_first_internal_vid();
									ActiveVertices.Write(vid, nwsm_->IsActive(vid));
									});
								}
						);

						register_vector(StartingVertices);

						for(auto& vec : vectors_) {
							vec->ClearAllOvDirty();
							vec->ClearAllOvChanged();
						}

						steps[0] = 0;
						steps[1] = 0;
						steps[2] = 0;
						steps[3] = perf_timer_.stop_timer(4);
						steps[4] = steps[3];

						for (int step = 0; step < 5; step++) {
							execution_times_of_steps[step].push_back(steps[step]);
							//perf_timer_.reset_timer(step);
						}

						ReportPerStep(0, 0);
						ReportPerStep(1, 0);
						ReportPerStep(2, 0);
						ReportPerStep(3, steps[4]);

						ReportNWSMTime(0);
						ReportNWSMTime(1);
						ReportNWSMTime(2);
						ReportNWSMTime(3);
						ReportNWSMTime(4);
						for (int i = 0; i < time_breaker.size(); i++) {
							if (i == 12) time_breaker[i].push_back(steps[4]);
							else if (i <= 13) time_breaker[i].push_back(0.0);
						}

						if (finished_) {
							time_breaker[14].push_back(0.0);
							time_breaker[15].push_back(0.0);
							time_breaker[16].push_back(0.0);
							break;
						}
					} else {
						perf_timer_.start_timer(4);
						perf_timer_.start_timer(10);
						ALWAYS_ASSERT (prev_main_ver.GetEnd() >= 0);
						StartingVertices.SetSkipIv(false);
						if (iteration == 0) {
#ifndef GB_VERSIONED_ARRAY
							ProcessVertices([&](node_t vid) {
									Initialize(vid);
									InitializeOV(vid); //XXX
									}, false);
							if (UserArguments::UPDATE_VERSION >= 1) {
								ProcessVertices([&](node_t vid) {
										InitializePrev(vid);
										}, false);
							}
#else
							if (UserArguments::UPDATE_VERSION == 0) {
								ProcessVertices([&](node_t vid) {
									Initialize(vid);
									InitializeOV(vid); //XXX
									}, false);
							} else {
								/*ProcessVertices([&](node_t vid) {
								DebugInitialize(vid);
								}, false);
								ProcessVertices([&](node_t vid) {
								DebugInitializePrev(vid);
								}, false);*/
							}
#endif
						}
						time_breaker[0].push_back(perf_timer_.stop_timer(10));

						perf_timer_.start_timer(6);
						perf_timer_.start_timer(12);
						PrintNumberOfActiveVertices(ver, iteration);
						DEBUG_PreprocessingPerSuperstep();
						perf_timer_.stop_timer(12);

						perf_timer_.start_timer(13);
						perf_timer_.stop_timer(13);
						time_breaker[1].push_back(perf_timer_.stop_timer(6));

						system_fprintf(0, stdout, "\t======Step1======\t\n");
						perf_timer_.start_timer(0);
						Step1_Optimized(false); // Optimized for WCC,SSSP 
						//Step1(); 
						steps[0] = perf_timer_.stop_timer(0);
						ReportPerStep(0, steps[0]);

						// (2)
						system_fprintf(0, stdout, "\t======Step2======\t\n");
						perf_timer_.start_timer(1);
						num_prop_changed_vertices = -1;
						if (UserArguments::ITERATOR_MODE== PARTIAL_LIST) {
							Step2_OptimizationToAvoidReaggregation();
						} else {
							Step2_OptimizationToAvoidReaggregation_FullList();
						}
						steps[1] = perf_timer_.stop_timer(1);
						ReportPerStep(1, steps[1]);

						// (3)
						system_fprintf(0, stdout, "\t======Step3======\t\n");
						perf_timer_.start_timer(2);
						Step3();
						steps[2]= perf_timer_.stop_timer(2);
						ReportPerStep(2, steps[2]);

						// (4)
						system_fprintf(0, stdout, "\t======Step4======\t\n");
						perf_timer_.start_timer(3);
						Step4();
						steps[3] = perf_timer_.stop_timer(3);
						ReportPerStep(3, steps[3]);

						steps[4] = perf_timer_.stop_timer(4);

						for (int step = 0; step < 5; step++) {
							execution_times_of_steps[step].push_back(steps[step]);
							//perf_timer_.reset_timer(step);
						}   
					}

					perf_timer_.start_timer(7);
					if (ActiveVertices.GetNumberOfSuperstepVersions(UserArguments::UPDATE_VERSION) <= (UserArguments::SUPERSTEP+1)) {
						ActiveVertices.AdvanceSuperstepVersion(UserArguments::UPDATE_VERSION);
					}

					bool construct_prev = (UserArguments::UPDATE_VERSION >= 1);
					ActiveVertices.ConstructAndFlush(Range<int64_t>(0, PartitionStatistics::my_num_internal_nodes() - 1), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP + 1, construct_prev, &ActiveVerticesPrev, true);
					time_breaker[14].push_back(perf_timer_.stop_timer(7));

					DEBUG_PostprocessingPerSuperstep();

					if (max_num_iterations_in_prev_execution < iteration)
						max_num_iterations_in_prev_execution = iteration;

					iteration++;
					UserArguments::SUPERSTEP = iteration;
					prev_run_static_processing = run_static_processing;


					// XXX - It is wrong!
					if (!run_static_processing && !finished_ ) {
						perf_timer_.start_timer(8);
						int checkcontinue = ActiveVertices.IsSet() ? 1 : 0;
						//PartitionStatistics::wait_for_all(); // XXX remove this line
						time_breaker[15].push_back(perf_timer_.stop_timer(8));

						perf_timer_.start_timer(9);
						MPI_Allreduce(MPI_IN_PLACE, &checkcontinue, 1, MPI_INT, MPI_LOR, MPI_COMM_WORLD);   // BARRIER
						if (checkcontinue == 0) {
							checkcontinue = ActiveVertices.GetBitMap().IsSet() ? 1 : 0;
							MPI_Allreduce(MPI_IN_PLACE, &checkcontinue, 1, MPI_INT, MPI_LOR, MPI_COMM_WORLD);   // BARRIER
						}
						time_breaker[16].push_back(perf_timer_.stop_timer(9));

						fprintf(stdout, "(%ld, %ld, %ld)\tStep1(): %.3f\tStep2(): %.3f\tStep3(): %.3f\tStep4(): %.3f\tSum: %.3f // %.3f %.3f %.3f %.3f %.3f // %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, iteration, 
								perf_timer_.get_timer(0), perf_timer_.get_timer(1), perf_timer_.get_timer(2), perf_timer_.get_timer(3), perf_timer_.get_timer(4), 
								perf_timer_.get_timer(5), perf_timer_.get_timer(6), perf_timer_.get_timer(7), perf_timer_.get_timer(8), perf_timer_.get_timer(9), 
								perf_timer_.get_timer(10), perf_timer_.get_timer(11), perf_timer_.get_timer(12), perf_timer_.get_timer(13));
						if (checkcontinue == 0) {
							system_fprintf(0, stdout, "FINISHED INC_RUN at %ld ss. %ld <= %ld ?\n", iteration, max_num_iterations_in_prev_execution, iteration);
						}
						if (checkcontinue == 0 && (max_num_iterations_in_prev_execution <= iteration)) {
							finished_ = true;
							break;
						}
					} else {
						time_breaker[15].push_back(0.0);
						time_breaker[16].push_back(0.0);
						fprintf(stdout, "(%ld, %ld, %ld)\tStep1(): %.3f\tStep2(): %.3f\tStep3(): %.3f\tStep4(): %.3f\tSum: %.3f // %.3f %.3f %.3f %.3f %.3f %.3f %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, iteration, perf_timer_.get_timer(0), perf_timer_.get_timer(1), perf_timer_.get_timer(2), perf_timer_.get_timer(3), perf_timer_.get_timer(4), perf_timer_.get_timer(5), perf_timer_.get_timer(6), perf_timer_.get_timer(7), perf_timer_.get_timer(8), perf_timer_.get_timer(9), perf_timer_.get_timer(10), perf_timer_.get_timer(11), perf_timer_.get_timer(12), perf_timer_.get_timer(13));
					}
					//XXX remove this line
					//MaterializedAdjacencyLists::DropMaterializedViewInMemory(0);

					for (auto& vec : vectors_) {
						vec->logVersionedArrayIOWaitTime();
					}

					MeasureCpuTimes(get_main_thread_cpu_time(), cpu_timer_.stop_timer(1), get_openmp_threads_cpu_time(), RequestRespond::rr_.async_udf_pool.GetTotalCpuTime());
				}
				UserArguments::QUERY_ONGOING = false;

				ActiveVertices.FinishAsyncJob();
				EndTimers(get_main_thread_cpu_time(), cpu_timer_.stop_timer(1), get_openmp_threads_cpu_time(), RequestRespond::rr_.async_udf_pool.GetTotalCpuTime());

				CompletePendingAioRequests();
				ClearInputVectorFlags();
				Postprocessing();

				std::string head = std::string("[TOTAL_IO_COST at Snapshot ") + std::to_string(UserArguments::UPDATE_VERSION) + std::string("]");
				Turbo_bin_aio_handler::WaitForAllPendingDiskIoOfAllInterfaces(false, true);
				dump_io_stats(head, true);
				system_fprintf(0, stdout, "\n\n\n");
				//char n; std::cout << "Enter n: "; std::cin >> n;
			}
			//char n; std::cout << "Enter n: "; std::cin >> n;
			DEBUG_PostprocessingBeforeFinalize();

			CloseVectors();
			SystemFinalize();
		}

		void LogNumberOfActiveVerticesPerStep() {
#ifndef INCREMENTAL_PROFILING
			return;
#endif
			int64_t cnt = 0;

			ProcessVertices(&ActiveVertices, &ActiveVerticesPrev,
					[&](node_t vid) {
					if (nwsm_->IsMarkedAtVOI(1, vid)) {
					std::atomic_fetch_add((std::atomic<int64_t>*) &cnt, +1L);
					}
					}, false);
			MPI_Allreduce(MPI_IN_PLACE, &cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

			if (UserArguments::INC_STEP == -1) {
				active_vertices_of_steps[3].push_back(cnt);
				for (int k = 0; k < 3; k++) {
					if(active_vertices_of_steps[3].size() > active_vertices_of_steps[k].size()) {
						active_vertices_of_steps[k].push_back(0);
					}
					ALWAYS_ASSERT(active_vertices_of_steps[3].size() == active_vertices_of_steps[k].size());
				}
			} else {
				active_vertices_of_steps[UserArguments::INC_STEP - 1].push_back(cnt);
				for (int k = 0; k < UserArguments::INC_STEP - 1; k++) {
					if(active_vertices_of_steps[UserArguments::INC_STEP - 1].size() > active_vertices_of_steps[k].size()) {
						active_vertices_of_steps[k].push_back(0);
					}
					ALWAYS_ASSERT(active_vertices_of_steps[UserArguments::INC_STEP - 1].size() == active_vertices_of_steps[k].size());
				}
			}
		}

		void PrintNumberOfActiveVertices(int64_t u, int64_t s) {
#ifndef INCREMENTAL_PROFILING
			return;
#endif                
			int64_t cnt = 0;
			ProcessVertices(&ActiveVertices, &ActiveVerticesPrev,
					[&](node_t vid) {
#ifdef INCREMENTAL_LOGGING
					//if (PartitionStatistics::my_machine_id() == 0 && (vid % 64 == 0)) {
					if (INCREMENTAL_DEBUGGING_TARGET(vid)) {
					//if (true) {
					//if ((UserArguments::UPDATE_VERSION == 6 || UserArguments::UPDATE_VERSION == 14) && ActiveVertices.Read(vid)) {
					//if (PartitionStatistics::my_machine_id() == 10 && (UserArguments::UPDATE_VERSION == 8 || UserArguments::UPDATE_VERSION == 12) && ActiveVertices.Read(vid)) {
					//if (PartitionStatistics::my_machine_id() == 10 && ActiveVertices.Read(vid)) {
					//if (TARGET_VID == -1 || INCREMENTAL_DEBUGGING_TARGET(vid)) {
					//if (INCREMENTAL_DEBUGGING_TARGET(vid) && ActiveVertices.Read(vid)) {
					fprintf(stdout, "(%ld, %ld) vid = %ld  ActiveVertices %ld  ActiveVerticesPrev %ld\n", (int64_t) u, (int64_t) s, (int64_t)vid, ActiveVertices.Read(vid)?1L:0L, ActiveVerticesPrev.Read(vid)?1L:0L);
					}
#endif
					if (ActiveVertices.Read(vid)) {
					std::atomic_fetch_add((std::atomic<int64_t>*) &cnt, +1L);
					}
					}, false);
			fprintf(stdout, "[%ld] (%ld, %ld)\tVector\t|ActiveVerticesPerMachine| = %ld\n", PartitionStatistics::my_machine_id(), (int64_t) u, (int64_t) s, (int64_t) cnt);
			MPI_Allreduce(MPI_IN_PLACE, &cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
			system_fprintf(0, stdout, "(%ld, %ld)\tVector\t|ActiveVertices| = %ld\n", (int64_t) u, (int64_t) s, (int64_t) cnt);
			active_vertices.push_back(cnt);
			portion_of_active_vertices.push_back((double) cnt / (double) PartitionStatistics::num_total_nodes());
					}

					void AdvanceUpdateVersion(int next_u) {
						for (auto& vec : vectors_) {
							vec->CreateIvForNextUpdate(next_u);
							vec->CreateOvForNextUpdate(next_u);
						}
					}

					virtual void CloseVectors() {
						system_fprintf(0, stdout, "Close Vectors...\n");
						std::vector<json11::Json> vector_names;
						for (auto& vec : vectors_) {
#ifdef INCREMENTAL_PROFILING
							vec->getvectornames(vector_names);
							vec->aggregateReadIO(json_logger);
							vec->aggregateWriteIO(json_logger);
#endif
#ifdef REPORT_PROFILING_TIMERS
							vec->aggregateNonOverlappedTime(json_logger);
#endif
							vec->Close();
						}
						json_logger->InsertKeyValue("Vectors", vector_names);
						ActiveVerticesPrev.Close(false);
						ActiveVertices.Close(true);
					}

						protected:

					ReturnStatus SystemInitialize() {
						load_db();

						num_vertices = PartitionStatistics::num_total_nodes();

						Aio_Helper::Initialize(nio_threads, num_threads, NumaHelper::num_cores() - 1);
						RequestRespond::Initialize(8 * 1024 * 1024L, 128, DEFAULT_NIO_PREALLOCATED_BUFFER_TOTAL_SIZE, DEFAULT_NIO_BUFFER_SIZE, nio_threads, num_threads);
						rr_receiver_ = new std::thread(RequestRespond::ReceiveRequest);
						{
							cpu_set_t cpuset;
							int affinity = NumaHelper::num_cores() - 1;

							CPU_ZERO(&cpuset);
							CPU_SET(affinity, &cpuset);

							int rc = pthread_setaffinity_np(rr_receiver_->native_handle(), sizeof(cpu_set_t), &cpuset);
							if (rc != 0) {
								std::cout << "ERROR calling pthread_setaffinity_np; " << rc << std::endl;
								abort();
							}
						}

						openmp_threads_cpu_time_begin = new timespec[UserArguments::NUM_THREADS];
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
						{
							int i = omp_get_thread_num();
							TG_ThreadContexts::thread_id = i;
							TG_ThreadContexts::core_id = NumaHelper::assign_core_id_scatter(i, 0, UserArguments::NUM_THREADS - 1);
							TG_ThreadContexts::socket_id = NumaHelper::get_socket_id_by_omp_thread_id(i);
						}

						int64_t de_vid_map_mem_usage = 0;

						pull_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						prop_changed_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						affected_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						reapply_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						genOld_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						genNew_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						Vdv_vid_map.Init(PartitionStatistics::my_num_internal_nodes());
						de_vid_map = new TwoLevelBitMap<node_t>*[UserArguments::MAX_LEVEL+1];
						for (int i = 0; i < UserArguments::MAX_LEVEL+1; i++) {
							de_vid_map[i] = new TwoLevelBitMap<node_t>[3];
							//if (i >= 1) {
							if (true) {
								de_vid_map[i][0].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map[i][1].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map[i][2].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map_mem_usage += (3 * de_vid_map[i][0].total_container_size());
							} else {
								de_vid_map[i][0].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map[i][1].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map[i][2].Init(PartitionStatistics::my_num_internal_nodes());
								de_vid_map_mem_usage += (3 * de_vid_map[i][0].total_container_size());
							}
						}

						LocalStatistics::register_mem_alloc_info("BitMap@turbograph_implementation", (8 * pull_vid_map.total_container_size() + de_vid_map_mem_usage) / (1024*1024L));

						int result = posix_memalign((void**) &per_thread_num_processed_edges_cnts, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_cnts1, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_cnts2, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_num_pages_io_cnts, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_num_processed_pages_cnts, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_num_edges_in_pages_cnts, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						result = posix_memalign((void**) &per_thread_num_processed_empty_pages_cnts, 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
						for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
							per_thread_num_processed_edges_cnts[tid].idx = 0;
							per_thread_cnts1[tid].idx = 0;
							per_thread_cnts2[tid].idx = 0;
							per_thread_num_pages_io_cnts[tid].idx = 0;
							per_thread_num_processed_pages_cnts[tid].idx = 0;
							per_thread_num_edges_in_pages_cnts[tid].idx = 0;
							per_thread_num_processed_empty_pages_cnts[tid].idx = 0;
						}
						return ReturnStatus::OK;
						}

						void start_main_thread_timer() {
							clockid_t cid;
							timespec currTime;
							int rc = pthread_getcpuclockid(pthread_self(), &cid);
							if (rc != 0)
								perror("pthread_getcpuclockid");
							clock_gettime(cid, &currTime);
							main_thread_cpu_time_begin = currTime;
						}

						void start_openmp_threads_timer() {
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
							{
								int i = omp_get_thread_num();
								struct timespec currTime;
								clockid_t cid;
								int rc = pthread_getcpuclockid(pthread_self(), &cid);
								if (rc != 0)
									perror("pthread_getcpuclockid");
								clock_gettime(cid, &currTime);
								openmp_threads_cpu_time_begin[i] = currTime;
							}
						}

						ReturnStatus ParameterInitialize(int argc, char** argv) {
							/*options.add_options()
								("d,debug", "Enable debugging") // a bool parameter
								("i,integer", "Int param", cxxopts::value<int>())
								("f,file", "File name", cxxopts::value<std::string>())
								("v,verbose", "Verbose output", cxxopts::value<bool>()->default_value("false"))
								;
								auto parse_result = options.parse(argc, argv);*/
							//int64_t opt = parse_result["opt"].as<int64_t>();
							argc_ = argc;
							argv_ = argv;
							working_dir = std::string(argv_[2]);
							int provided = MPI::Init_thread(argc_, argv_, MPI_THREAD_MULTIPLE);
							if (provided != MPI_THREAD_MULTIPLE) {
								throw std::runtime_error("[Turbograph::run] MPI::Init_thread failed\n");
								abort();
							}
							PartitionStatistics::init();
							turbo_tcp::init_host();

							set_signal_handler();
							setbuf(stdout, NULL);
							std::string glog_filename;
							//char* name = getlogin();
							char* name = (char*) std::getenv("USER_NAME"); 
							std::vector<std::string> binary_file;
							std::vector<std::string> db_file;
							split_string(std::string(argv_[0]), '/', binary_file);
							split_string(std::string(argv_[2]), '/', db_file);
							//create json log file
							std::string json_filename;
							if(const char* env_p = std::getenv("JSON_FILENAME")) {
								json_filename.append(env_p);
							}
							//json_filename.append("/home/" + std::string(name) + "/json_log_temp/tbgpp."); // TODO get file name from arguments
							json_filename.append(binary_file[binary_file.size()-1]).append(".");
							json_filename.append(db_file[db_file.size()-1]).append(".");

							std::string current_date = std::to_string(get_current_time());
							MPI_Bcast(const_cast<char*>(current_date.data()), current_date.size(), MPI_CHAR, 0, MPI_COMM_WORLD);
							json_filename.append(current_date).append(".json");
						       	
							std::string local_json_filename;
							if(const char* env_p = std::getenv("LOCAL_JSON_FILENAME")) {
								local_json_filename.append(env_p);
							}
							//local_json_filename.append("/home/" + std::string(name) + "/json_log/tbgpp."); // TODO get file name from arguments
							local_json_filename.append(binary_file[binary_file.size()-1]).append(".");
							local_json_filename.append(db_file[db_file.size()-1]).append(".");
							local_json_filename.append(current_date).append(".");
							local_json_filename.append(std::to_string(PartitionStatistics::my_machine_id())).append(".json");

							json_logger->OpenLogger(json_filename.c_str(), false);
							local_json_logger->OpenLogger(local_json_filename.c_str(), true);
							system_fprintf(0, stdout, "[JsonLogger] Open Json Log File %s at Machine 1\n", json_filename.c_str());
							system_fprintf(0, stdout, "[JsonLogger] Open Local Json Log File %s for each Machine\n", local_json_filename.c_str());

							json_logger->InsertKeyValue("Query", binary_file[binary_file.size()-1]);
							//json_logger->InsertKeyValue("DB", db_file[5]+"/"+db_file[6]);
							local_json_logger->InsertKeyValue("Query", binary_file[binary_file.size()-1]);
							//local_json_logger->InsertKeyValue("DB", db_file[5]+"/"+db_file[6]);
							local_json_logger->InsertKeyValue("MachineID", PartitionStatistics::my_machine_id());

							std::cout << std::flush;
							FLAGS_logtostderr = 0;
							if (this->argc_ < 11) {
								if (PartitionStatistics::my_machine_id() == 0) {
									std::cerr << "Arguements are incorrect" << std::endl;
									std::cerr
										<< "argv_[1] : " << "Raw Data Directory" << std::endl
										<< "argv_[2] : " << "DB Working Directory" << std::endl
										<< "argv_[3] : " << "Num Sequential Vector Chunks per Machine" << std::endl
										<< "argv_[4] : " << "Buffer Pool Size in MB" << std::endl
										<< "argv_[5] : " << "Number of Threads (default 1)" << std::endl
										<< "argv_[6] : " << "Maximum Number of Iterations (if -1, INT_MAX)" << std::endl
										<< "argv_[7] : " << "Maximum Memory Size" << std::endl
										<< "argv_[8] : " << "schedule type in [0,3]" << std::endl
										<< "argv_[9] : " << "Num SubChunks per Edge Chunk" << std::endl
										<< "argv_[10] : " << "Max Version Number" << std::endl;
								}
								throw std::runtime_error("[Turbograph::run] init() failed\n");
								return ReturnStatus::FAIL;
							}
							if (PartitionStatistics::my_machine_id() == 0) {
								std::cout << "<INPUT> "<< std::endl
									<< "    argv_[0] : " << this->argv_[0] << std::endl
									<< "    argv_[1] : " << this->argv_[1] << std::endl
									<< "    argv_[2] : " << this->argv_[2] << std::endl
									<< "    argv_[3] : " << atol(this->argv_[3]) << std::endl
									<< "    argv_[4] : " << atol(this->argv_[4]) << std::endl
									<< "    argv_[5] : " << atol(this->argv_[5]) << std::endl
									<< "    argv_[6] : " << atol(this->argv_[6]) << std::endl
									<< "    argv_[7] : " << atol(this->argv_[7]) << std::endl
									<< "    argv_[8] : " << atol(this->argv_[8]) << std::endl
									<< "    argv_[9] : " << atol(this->argv_[9]) << std::endl
									<< "    argv_[10] : " << atol(this->argv_[10]) << std::endl;
							}
							config_.edge_in_out_mode = OUT_MODE;

							UserArguments::RAWDATA_PATH = std::string(this->argv_[1]);
							num_seq_vector_partitions = atol(this->argv_[3]);
							UserArguments::VECTOR_PARTITIONS = num_seq_vector_partitions;
							UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK = std::atol(this->argv_[9]);
							UserArguments::WORKSPACE_PATH = std::string(this->argv_[2]);

							buffer_pool_size_in_mb = atol(this->argv_[4]);
							config_.page_buffer_size_in_MB = buffer_pool_size_in_mb;

							num_threads = atol(this->argv_[5]);
							UserArguments::NUM_THREADS = num_threads;
							if (num_threads == -1) num_threads = 1;

							MaxIters = atol(this->argv_[6]);
							if (MaxIters == -1) MaxIters = INT_MAX;
							UserArguments::NUM_ITERATIONS = MaxIters;

							UserArguments::PHYSICAL_MEMORY = (int64_t) atol(this->argv_[7]);
							UserArguments::SCHEDULE_TYPE = (int) atol(this->argv_[8]);

							omp_set_num_threads(num_threads);
							core_id::set_core_ids(num_threads);

							if (PartitionStatistics::my_machine_id() == 0) {
								print_processor_info();
								fprintf(stdout, "(# of Execution Threads) = %lld\n", num_threads);
							}

							nio_threads = DEFAULT_NIO_THREADS;

							UserArguments::MAX_VERSION = std::stoll(argv_[10]);
							UserArguments::NUM_CPU_SOCKETS = NumaHelper::num_sockets();
							UserArguments::NUM_DISK_AIO_THREADS = UserArguments::NUM_CPU_SOCKETS * 2;
							UserArguments::tmp = PartitionStatistics::my_machine_id();

							argc_ -= 10;
							argv_ = &argv_[10];
							return ReturnStatus::OK;
						}

						double get_main_thread_cpu_time() {
							clockid_t cid;
							timespec currTime;
							int rc = pthread_getcpuclockid(pthread_self(), &cid);
							if (rc != 0)
								perror("pthread_getcpuclockid");
							clock_gettime(cid, &currTime);
							return (currTime.tv_sec - main_thread_cpu_time_begin.tv_sec + (currTime.tv_nsec - main_thread_cpu_time_begin.tv_nsec) / 1000000000.0);
						}

						double get_openmp_threads_cpu_time() {
							double total_secs = 0;
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
							{
								int i = omp_get_thread_num();
								struct timespec currTime;
								clockid_t cid;
								int rc = pthread_getcpuclockid(pthread_self(), &cid);
								if (rc != 0)
									perror("pthread_getcpuclockid");
								clock_gettime(cid, &currTime);
								double secs = ((currTime.tv_sec - openmp_threads_cpu_time_begin[i].tv_sec) + (currTime.tv_nsec - openmp_threads_cpu_time_begin[i].tv_nsec) / 1000000000.0);
								//openmp_threads_cpu_time_begin[i] = currTime;
								AtomicOperation<double, PLUS>(&total_secs, secs);
							}
							return total_secs;
						}

						void set_openmp_threads_affinity() {
#pragma omp parallel num_threads(num_threads)
							{
								cpu_set_t cpuset;
								int thread_idx = omp_get_thread_num();
								pid_t tid = (pid_t) syscall(SYS_gettid);

								CPU_ZERO(&cpuset);
								CPU_SET( (thread_idx) % std::thread::hardware_concurrency(), &cpuset);

								unsigned long mask = -1;
								int rc = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
								if (rc != 0) {
									std::cout << "ERROR calling pthread_setaffinity_np; " << rc << std::endl;
									abort();
								}
							}
						}

						void InitDiskAioFactory() {
							int res;
							DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, UserArguments::NUM_DISK_AIO_THREADS, 128);
						}

						static void MallocStatDump(void* cbopaque, const char* msg) {
							LOG(INFO) << msg;
						}

						void dump_io_stats(std::string& header, bool accum) {
							int64_t ver_array_io_bytes[2] = {0};
							int64_t total_ver_array_io_bytes[2] = {0};
							int64_t vec_spill_write_bytes, vec_spill_read_bytes, vec_write_bytes, vec_read_bytes;
							int64_t edge_read_bytes, edge_write_bytes;
							int64_t num_reads, num_read_bytes, num_writes, num_write_bytes;
							int64_t num_received_bytes, num_sent_bytes;
							int64_t num_msgs_before_combining_per_thread;
							int64_t num_msgs_after_combining_per_thread;

							int64_t page_buffer_hit, page_buffer_miss, matadj_buffer_hit, matadj_buffer_miss;
							double buffer_hit, buffer_miss;

							per_partition_elem<int64_t> per_partition_disk_read;
							per_partition_elem<int64_t> per_partition_disk_write;
							per_partition_elem<int64_t> per_partition_network_send;
							per_partition_elem<int64_t> per_partition_network_recv;
							per_partition_elem<int64_t> per_partition_buffer_hit;
							per_partition_elem<int64_t> per_partition_buffer_miss;
							per_partition_elem<int64_t> per_partition_mat_adj_buffer_hit;

							diskaio::DiskAioStats dstats = DiskAioFactory::GetPtr()->GetStats();
							num_reads = dstats.num_reads;
							num_read_bytes = dstats.num_read_bytes;
							num_writes = dstats.num_writes;
							num_write_bytes = dstats.num_write_bytes;

							edge_read_bytes = TurboDB::GetBufMgr()->GetPageReadIO();
							edge_write_bytes = TurboDB::GetBufMgr()->GetPageWriteIO();

							vec_spill_write_bytes = TG_DistributedVectorBase::GetSpilledDiskVectorWrites();
							vec_spill_read_bytes = vec_spill_write_bytes;
							vec_write_bytes = TG_DistributedVectorBase::GetDiskVectorWrites();
							vec_read_bytes = 0;

							ver_array_io_bytes[0] = num_read_bytes - edge_read_bytes;
							ver_array_io_bytes[1] = num_write_bytes - edge_write_bytes;

							total_ver_array_io_bytes[0] = ver_array_io_bytes[0];
							total_ver_array_io_bytes[1] = ver_array_io_bytes[1];

							//num_read_bytes += vec_spill_read_bytes;
							//num_write_bytes += vec_spill_write_bytes;

							num_received_bytes = turbo_tcp::GetBytesReceived();
							num_sent_bytes = turbo_tcp::GetBytesSent();

							pTurboDB_[0]->GetBufMgr()->GetPageBufferHitMissBytes(page_buffer_hit, page_buffer_miss);
							pTurboDB_[1]->GetBufMgr()->GetPageBufferHitMissBytes(page_buffer_hit, page_buffer_miss);
							RequestRespond::GetMatAdjBufferHitMissBytes(matadj_buffer_hit, matadj_buffer_miss);

							per_partition_disk_read.mine() = num_read_bytes;
							per_partition_disk_write.mine() = num_write_bytes;
							per_partition_network_send.mine() = num_sent_bytes;
							per_partition_network_recv.mine() = num_received_bytes;
							per_partition_buffer_hit.mine() = page_buffer_hit + matadj_buffer_hit;
							per_partition_buffer_miss.mine() = page_buffer_miss + matadj_buffer_miss;
							per_partition_mat_adj_buffer_hit.mine() = matadj_buffer_hit;

							per_partition_disk_read.pull();
							per_partition_disk_write.pull();
							per_partition_network_send.pull();
							per_partition_network_recv.pull();
							per_partition_buffer_hit.pull();
							per_partition_buffer_miss.pull();
							per_partition_mat_adj_buffer_hit.pull();

							// Edge Disk I/O
							MPI_Allreduce(MPI_IN_PLACE, &edge_read_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &edge_write_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

							// Disk I/O
							MPI_Allreduce(MPI_IN_PLACE, &num_reads, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &num_read_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &num_writes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &num_write_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &total_ver_array_io_bytes[0], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &total_ver_array_io_bytes[1], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

							// Network I/O
							MPI_Allreduce(MPI_IN_PLACE, &num_received_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &num_sent_bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

							// Spilled Messages (Writes + Reads)

							// Buffer Hit & Miss
							buffer_hit = (double) (page_buffer_hit + matadj_buffer_hit) / (page_buffer_hit + matadj_buffer_hit + page_buffer_miss + matadj_buffer_miss);
							buffer_miss = (double) (page_buffer_miss + matadj_buffer_miss) / (page_buffer_hit + matadj_buffer_hit + page_buffer_miss + matadj_buffer_miss);
							MPI_Allreduce(MPI_IN_PLACE, &buffer_hit, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &buffer_miss, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &matadj_buffer_hit, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &matadj_buffer_miss, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							buffer_hit = buffer_hit / PartitionStatistics::num_machines();
							buffer_miss = buffer_miss / PartitionStatistics::num_machines();

							num_msgs_before_combining_per_thread = TG_DistributedVectorBase::NumMessagesBeforeCombiningPerThreadBuffer;
							num_msgs_after_combining_per_thread = TG_DistributedVectorBase::NumMessagesAfterCombiningPerThreadBuffer;
							MPI_Allreduce(MPI_IN_PLACE, &num_msgs_before_combining_per_thread, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &num_msgs_after_combining_per_thread, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);

							nwsm_->PrintEdgeWindowIoInformation();

							if(PartitionStatistics::my_machine_id() == 0) {
								if (accum) {
									fprintf(stdout, "%s\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%.2f\t%.2f\t{EdgeRead, EdgeWrite, VerArrayRead, VerArrayWrite, NetworkReceived, NetworkSent, MatAdjBufferHit} [Bytes] {BufferHit, BufferMiss} [0,1]\n", header.c_str(), (int64_t) edge_read_bytes, (int64_t) edge_write_bytes, (int64_t) total_ver_array_io_bytes[0], (int64_t) total_ver_array_io_bytes[1], (int64_t) num_received_bytes, (int64_t) num_sent_bytes, (int64_t) matadj_buffer_hit, buffer_hit, buffer_miss);

									/*
										 for (int i = 0; i < PartitionStatistics::num_machines(); i++) {
										 fprintf(stdout, "[IO_COST: Rank-%lld]\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%.2f\t%.2f\t{EdgeRead, EdgeWrite, VerArrayRead, VerArrayWrite,NetworkReceived, NetworkSent} [Bytes] {BufferHit, BufferMiss} [0,1]\n", (int64_t) i, (int64_t) (per_partition_disk_read[i] - accm_versionned_array_io_in_bytes[0]), (int64_t) (per_partition_disk_write[i] - accm_versionned_array_io_in_bytes[1]), (int64_t) accm_versionned_array_io_in_bytes[0], (int64_t) accm_versionned_array_io_in_bytes[1], (int64_t) per_partition_network_recv[i], (int64_t) per_partition_network_send[i], per_partition_buffer_hit[i], per_partition_buffer_miss[i]);
										 }
										 */
								} else {
									ALWAYS_ASSERT (false); // syko - deprecated
								}
							}
							PartitionStatistics::wait_for_all();

							per_partition_disk_read.close();
							per_partition_disk_write.close();
							per_partition_network_send.close();
							per_partition_network_recv.close();
							per_partition_buffer_hit.close();
							per_partition_buffer_miss.close();
							per_partition_mat_adj_buffer_hit.close();
						}

						void SystemFinalize() {
							system_fprintf(0, stdout, "Finalize...\n");
							PartitionStatistics::wait_for_all();

							PartitionStatistics::wait_for_all();
							if (pTurboDB_[0]) {
								delete pTurboDB_[0];
								pTurboDB_[0] = NULL;
							}
							if (pTurboDB_[1]) {
								delete pTurboDB_[1];
								pTurboDB_[1] = NULL;
							}
							json_logger->InsertKeyValue("System","TBGPP");
							json_logger->InsertKeyValue("NumMachines", PartitionStatistics::num_machines());
							json_logger->InsertKeyValue("ExecutionTimePerSuperstep", json_time_per_ss_vector);
							json_logger->InsertKeyValue("Snapshot", snapshots);
							json_logger->InsertKeyValue("Superstep", supersteps);
							json_logger->InsertKeyValue("AccumDeltaSize", accum_delta_size);
							for (int step = 0; step < 5; step++) {
								std::string key = "Step" + std::to_string(step+1);
								json_logger->InsertKeyValue(key.c_str(), execution_times_of_steps[step]);
							} 
							json_logger->InsertKeyValue("V_active", active_vertices);
							json_logger->InsertKeyValue("V_active_percent", portion_of_active_vertices);
							//#ifdef INCREMENTAL_PROFILING
							for (int step = 0; step < 4; step++) {
								std::string key = "V_active_Step" + std::to_string(step+1);
								json_logger->InsertKeyValue(key.c_str(), active_vertices_of_steps[step]);
							}
							num_processed_edges_of_steps_per_machine_json.resize(num_processed_edges_of_steps[0].size());
							for (int step = 0; step < 4; step++) {
								std::string key = "E_active_Step" + std::to_string(step+1);
								for (int64_t j = 0; j < num_processed_edges_of_steps[step].size(); j++) {
									num_processed_edges_of_steps_per_machine_json[j] = num_processed_edges_of_steps[step][j];
								}
								local_json_logger->InsertKeyValue(key.c_str(), num_processed_edges_of_steps_per_machine_json);
								MPI_Allreduce(MPI_IN_PLACE, &num_processed_edges_of_steps[step][0], num_processed_edges_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < num_processed_edges_of_steps[step].size(); j++) {
									num_processed_edges_of_steps_json[step].push_back(num_processed_edges_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), num_processed_edges_of_steps_json[step]);
							}
							num_processed_pages_of_steps_per_machine_json.resize(num_processed_pages_of_steps[0].size());
							for (int step = 0; step < 4; step++) {
								std::string key = "Num_Processed_Pages_Step" + std::to_string(step+1);
								for (int64_t j = 0; j < num_processed_pages_of_steps[step].size(); j++) {
									num_processed_pages_of_steps_per_machine_json[j] = num_processed_pages_of_steps[step][j];
								}
								local_json_logger->InsertKeyValue(key.c_str(), num_processed_pages_of_steps_per_machine_json);
								MPI_Allreduce(MPI_IN_PLACE, &num_processed_pages_of_steps[step][0], num_processed_pages_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < num_processed_pages_of_steps[step].size(); j++) {
									num_processed_pages_of_steps_json[step].push_back(num_processed_pages_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), num_processed_pages_of_steps_json[step]);
							}
							for (int step = 0; step < 4; step++) {
								std::string key = "Num_Processed_Empty_Pages_Step" + std::to_string(step+1);
								MPI_Allreduce(MPI_IN_PLACE, &num_processed_empty_pages_of_steps[step][0], num_processed_pages_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < num_processed_empty_pages_of_steps[step].size(); j++) {
									num_processed_empty_pages_of_steps_json[step].push_back(num_processed_empty_pages_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), num_processed_empty_pages_of_steps_json[step]);
							}
							for (int step = 0; step < 4; step++) {
								std::string key = "Num_Edges_In_Processed_Pages_Step" + std::to_string(step+1);
								json_logger->InsertKeyValue(key.c_str(), num_edges_in_pages_of_steps[step]);
							}
							page_io_of_steps_per_machine_json.resize(page_io_of_steps[0].size());
							for (int step = 0; step < 4; step++) {
								std::string key = "PageIO_Step" + std::to_string(step+1);
								for (int64_t j = 0; j < page_io_of_steps[step].size(); j++) {
									page_io_of_steps_per_machine_json[j] = page_io_of_steps[step][j];
								}
								local_json_logger->InsertKeyValue(key.c_str(), page_io_of_steps_per_machine_json);
								MPI_Allreduce(MPI_IN_PLACE, &page_io_of_steps[step][0], page_io_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < page_io_of_steps[step].size(); j++) {
									page_io_of_steps_json[step].push_back(page_io_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), page_io_of_steps_json[step]);
							}
							for (int step = 0; step < 4; step++) {
								std::string key = "VersionedArrayIO_Step" + std::to_string(step+1);
								MPI_Allreduce(MPI_IN_PLACE, &versioned_array_io_of_steps[step][0], versioned_array_io_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < versioned_array_io_of_steps[step].size(); j++) {
									versioned_array_io_of_steps_json[step].push_back(versioned_array_io_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), versioned_array_io_of_steps_json[step]);
							}
							network_io_of_steps_per_machine_json.resize(network_io_of_steps[0].size());
							for (int step = 0; step < 4; step++) {
								std::string key = "NetworkIO_Step" + std::to_string(step+1);
								MPI_Allreduce(MPI_IN_PLACE, &network_io_of_steps[step][0], network_io_of_steps[step].size(), MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								for (int64_t j = 0; j < network_io_of_steps[step].size(); j++) {
									network_io_of_steps_per_machine_json[j] = network_io_of_steps[step][j];
								}
								local_json_logger->InsertKeyValue(key.c_str(), network_io_of_steps_per_machine_json);
								for (int64_t j = 0; j < network_io_of_steps[step].size(); j++) {
									network_io_of_steps_json[step].push_back(network_io_of_steps[step][j]);
								}
								json_logger->InsertKeyValue(key.c_str(), network_io_of_steps_json[step]);
							}
							//#endif
							json_logger->InsertKeyValue("dV_prop", versioned_vertices[0]);
							json_logger->InsertKeyValue("dV_e", versioned_vertices[1]);
							json_logger->InsertKeyValue("dV_prop_AND_V_active", versioned_vertices[2]);
							json_logger->InsertKeyValue("dV_e_AND_V_active", versioned_vertices[3]);
							json_logger->InsertKeyValue("dV_active", versioned_vertices[4]);
							json_logger->InsertKeyValue("V_affected", versioned_vertices[5]);
							json_logger->InsertKeyValue("genOld_AND_genNew", versioned_vertices[6]);
							json_logger->InsertKeyValue("NotGenOld_AND_genNew", versioned_vertices[7]);
							json_logger->InsertKeyValue("genOld_AND_NotGenNew", versioned_vertices[8]);

							local_json_logger->InsertKeyValue("System","TBGPP");
							local_json_logger->InsertKeyValue("NumMachines", PartitionStatistics::num_machines());
							local_json_logger->InsertKeyValue("ExecutionTimePerSuperstep", json_time_per_ss_vector);
							local_json_logger->InsertKeyValue("Snapshot", snapshots);
							local_json_logger->InsertKeyValue("Superstep", supersteps);
							for (int i = 0; i < nwsm_time_breaker.size(); i++) {
								std::string key = "IdentifyAndCount_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][0]);
								key = "ReadInputVectors_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][1]);
								key = "InitializeOutputVectors_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][2]);
								key = "ReadOutputVectors_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][3]);
								key = "RunLocalScatterGather_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][4]);
								key = "FlushOutputVectors_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][5]);
								key = "ProcessApply_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][6]);
								key = "ReadGGB_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][7]);
								key = "Barrier_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][8]);
								key = "UpdatePhaseHJWithoutScatterGather_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][9]);
								key = "PerSuperstepCheckFinished_NWSM" + std::to_string(i+1);
								local_json_logger->InsertKeyValue(key.c_str(), nwsm_time_breaker[i][10]);
							}
							local_json_logger->InsertKeyValue("InitializeVertices", time_breaker[0]);
							local_json_logger->InsertKeyValue("FindPropertyChangedVertices", time_breaker[1]);
							local_json_logger->InsertKeyValue("Step1_1_ClearAlldEMap", time_breaker[2]);
							local_json_logger->InsertKeyValue("Step1_2_NWSMOnDeltaVer", time_breaker[3]);
							local_json_logger->InsertKeyValue("Step1_3_NWSMPostProcessing", time_breaker[4]);
							local_json_logger->InsertKeyValue("Step1_4_Step1PostProcessing", time_breaker[5]);
							local_json_logger->InsertKeyValue("Step2_1_Compute_genNew_vid_map", time_breaker[6]);
							local_json_logger->InsertKeyValue("Step2_2_NWSMOnDeltaVer", time_breaker[7]);
							local_json_logger->InsertKeyValue("Step2_3_ActiveGenNewOrGenOldVertices", time_breaker[8]);
							local_json_logger->InsertKeyValue("Step2_4_NWSMOnPrevMainVer", time_breaker[9]);
							local_json_logger->InsertKeyValue("Step2_5_FindAffectedVertices", time_breaker[10]);
							local_json_logger->InsertKeyValue("Step3_1_NWSMOnMainVer", time_breaker[11]);
							local_json_logger->InsertKeyValue("Step4_1_NWSMOnMainVer", time_breaker[12]);
							local_json_logger->InsertKeyValue("Step4_2_ClearOvDirtyAndChanged", time_breaker[13]);
							local_json_logger->InsertKeyValue("ActiveVerticesConstructAndFlush", time_breaker[14]);
							local_json_logger->InsertKeyValue("ActiveVerticesIsSet", time_breaker[15]);
							local_json_logger->InsertKeyValue("BarrierPerSuperstep", time_breaker[16]);

							for (auto& vec : vectors_) {
								vec->writeVersionedArrayIOWaitTime(local_json_logger);
							}

							json_logger->WriteIntoDisk();
							local_json_logger->WriteIntoDisk();

							json_logger->Close();
							local_json_logger->Close();

							nwsm_->Close();

							system_fprintf(0, stdout, "Closing PartitionStatistics...\n");
							PartitionStatistics::wait_for_all();
							PartitionStatistics::close();

							system_fprintf(0, stdout, "Closing ThreadPool...\n");
							RequestRespond::Close();
							Aio_Helper::async_pool.Close();
							PartitionStatistics::wait_for_all();

							system_fprintf(0, stdout, "Closing RequestRespond...\n");
							RequestRespond::EndRunning();
							rr_receiver_->join();
							PartitionStatistics::wait_for_all();

							system_fprintf(0, stdout, "Closing MPI...\n");
#ifdef COVERAGE
							__gcov_flush();
#endif
							_exit(1);
							MPI::Finalize();
						}


						virtual ReturnStatus load_db() {
							ReturnStatus st = ReturnStatus::FAIL;
							InitDiskAioFactory();

							//if (argc_ < 13) {
							if (true) {
								st = pTurboDB_[0]->LoadDB((working_dir+"/OutEdge").c_str(), config_, OUTEDGE, ALL);
								st = pTurboDB_[1]->LoadDB((working_dir+"/InEdge").c_str(), config_, INEDGE, ALL);
								UserArguments::WORKSPACE_PATH = working_dir;
								DiskAioFactory::GetPtr()->CreateAioInterfaces();
							}

							if (st != ReturnStatus::OK) {
								system_fprintf(0, stderr, "[LoadDB] Failed\n");
								_exit(1);
							}

							int64_t edge_file_size = DiskAioFactory::GetPtr()->GetAioFileSize();

							PartitionStatistics::print_partition_info(edge_file_size);

							if (PartitionStatistics::my_machine_id() == 0 && PartitionStatistics::num_total_nodes() % PartitionStatistics::num_machines() != 0) {
								fprintf(stderr, "[ERROR] VertexIDs are not aligned\n");
								_exit(1);
							}

							return ReturnStatus::OK;
						}

							public:

						void _Dummy(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							return;
						}

						void _MultiplyforInsertDegree(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							atomic_fetch_add_<int32_t>((std::atomic<int32_t>*) &OutDegreeTemp[0].Read<1>(src_vid), num_nbrs[0]);
							atomic_fetch_add_<int32_t>((std::atomic<int32_t>*) &OutDegreeTemp[1].Read<1>(src_vid), num_nbrs[0]);
							return;
						}

						void _MultiplyforDeleteDegree(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							atomic_fetch_add_<int32_t>((std::atomic<int32_t>*) &OutDegreeTemp[0].Read<1>(src_vid), -1 * num_nbrs[0]);
							atomic_fetch_add_<int32_t>((std::atomic<int32_t>*) &OutDegreeTemp[2].Read<1>(src_vid), num_nbrs[0]);
							return;
						}

						void _ApplyforDegree(node_t src_vid) {
							//fprintf(stdout, "[_ApplyforDegree] src_vid = %ld / %ld %ld %ld / %ld %ld %ld\n", (int64_t) src_vid, (int64_t) OutDegree[0].Read(src_vid), (int64_t) OutDegree[1].Read(src_vid), (int64_t) OutDegree[2].Read(src_vid), (int64_t) OutDegreeTemp[0].Read(src_vid), (int64_t) OutDegreeTemp[1].Read(src_vid), (int64_t) OutDegreeTemp[2].Read(src_vid)); 
							OutDegree[0].Write(src_vid, OutDegree[0].Read(src_vid) + OutDegreeTemp[0].Read(src_vid));
							OutDegreeTemp[0].Read(src_vid) = 0;

							if (UserArguments::UPDATE_VERSION == 0) {
								OutDegree[1].Write(src_vid, 0);
								OutDegreeTemp[1].Read(src_vid) = 0;
								OutDegree[2].Write(src_vid, 0);
								OutDegreeTemp[2].Read(src_vid) = 0;
								return;
							}

							for (int k = 1; k < 3; k++) {
								OutDegree[k].Write(src_vid, OutDegreeTemp[k].Read(src_vid));
								OutDegreeTemp[k].Read(src_vid) = 0;
							}
						}

						void _LoadDBToInMemory(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							std::vector<std::vector<node_t>>& graph_im = TurboDB::graph_in_memory;
							for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
								graph_im[src_vid].push_back(dst_vids[0][idx]);
							}
							return;
						}

						void _LoadDeleteDBToHashMap(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							std::pair<EdgePair, int> pair_to_insert;
							pair_to_insert.first.first = src_vid;
							pair_to_insert.second = 0;

							std::pair<node_t, node_t> src_;
							src_.first = src_vid;
							src_.second = src_vid;
							edge_src_map.insert(src_);

							for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
								pair_to_insert.first.second = dst_vids[0][idx];
								edge_map.insert(pair_to_insert);
							}
							return;
						}

						void _FindDeleteEdgesFromHashMap(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							EdgePair pair_to_find;
							pair_to_find.first = src_vid;

							for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
								pair_to_find.second = dst_vids[0][idx];
								auto edge_iter = edge_map.find (pair_to_find);
								if (edge_iter != edge_map.end()) {
									// Record deleted version & Mark page as dirty
									//fprintf(stdout, "%ld src_vid: %ld, Delete dst_vid: %ld\n", UserArguments::UPDATE_VERSION, src_vid, dst_vids[idx]);
									MarkDeletedVersion(&dst_vids[0][idx], UserArguments::UPDATE_VERSION);
								}
							}
							return;
						}

						void _ResetDelete(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
							for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
								if (GetDeletedVersion(dst_vids[0][idx]) != std::numeric_limits<node_t>::max()) {
									ResetDeletedVersion(&dst_vids[0][idx]);
									ALWAYS_ASSERT (GetDeletedVersion(dst_vids[0][idx]) == std::numeric_limits<node_t>::max());
								}
								ALWAYS_ASSERT (!IsEdgeDeleted(dst_vids[0][idx]));
							}
							return;
						}

						template <typename T, Op op>
							void unregister_vector(TG_DistributedVector<T, op>& vector) {
								TG_DistributedVectorBase* pVector = (TG_DistributedVectorBase*) &vector;

								for (auto it = vectors_.begin(); it != vectors_.end();) {
									if (*it == pVector) {
										vectors_.erase(it);
									} else {
										++it;
									}
								}
							}

						template <typename T, Op op>
							void register_vector(TG_DistributedVector<T, op>& vector) {
								vectors_.push_back((TG_DistributedVectorBase*)&vector);
							}

						void clear_vector_list() {
							vectors_.clear();
						}

						void reset_vector_states() {
							for (auto& vec : vectors_) {
								vec->ResetStates();
							}
						}

						void wait_on_rr(bool need_barrier = false) {
							RequestRespond::WaitRequestRespond(need_barrier);
						}

						void wait_for_vector_io() {
							for (auto& vec : vectors_) {
								vec->WaitForVectorIO();
							}
						}

						void AllocateMemoryForVectors() {
							for (auto& vec : vectors_) {
								vec->AllocateMemory();
							}
							AllocateGGB();
						}
						void AutoTuneBufferPoolSize() {
							int64_t total_memory = UserArguments::PHYSICAL_MEMORY;
							int64_t cur_mem_allocated = LocalStatistics::get_total_mem_size_allocated();
							int64_t available_mem = total_memory - cur_mem_allocated;
							int64_t vw = 0;
							int64_t ggb = 0;
							int64_t lgb = 0;
							int64_t vec_static_bitmap = TG_DistributedVectorBase::ComputeRandVectorWriteBufferMemorySize() / (1024 * 1024);
							int64_t voi_mem = 0;

							for (auto& vec : vectors_) {
								vw += vec->GetSizeOfVW() / (1024*1024);
								ggb += vec->GetSizeOfGGB() / (1024*1024);
								lgb += vec->GetSizeOfLGB() / (1024*1024);
							}

							//for (int lv = 0; lv < UserArguments::MAX_LEVEL; lv++) {
							//    voi_mem += nwsm_->ComputeVOIMemorySize(lv) / (1024*1024);
							//}
							available_mem -= vw;
							available_mem -= ggb;
							available_mem -= lgb;
							available_mem -= vec_static_bitmap;
							available_mem -= voi_mem;
							if (available_mem < 0) {
								system_fprintf(0, stderr, "[Turbograph] OutOfMemory, increase (# Vchunks per Machine) or (Physical Memory Size per Machine)\n\tMemory Allocation Info:\n\t\tTotal Mem = %ld\n\t\tCur Mem Allocated = %ld\n\t\tAvailable Mem = %ld\n\t\tVW = %ld\n\t\tGGB = %ld\n\t\tLGB = %ld\n\t\tVec_Static_Bitmap = %ld\n\t\tVOI_Mem_Usage = %ld\n", total_memory, cur_mem_allocated, available_mem + vw + ggb + lgb, vw, ggb, lgb, vec_static_bitmap, voi_mem);
								abort();
							} else {
								system_fprintf(0, stderr, "[Turbograph] AutoTuneBufferPoolSize\n\tMemory Allocation Info:\n\t\tTotal Mem = %ld\n\t\tCur Mem Allocated = %ld\n\t\tAvailable Mem = %ld\n\t\tVW = %ld\n\t\tGGB = %ld\n\t\tLGB = %ld\n\t\tVec_Static_Bitmap = %ld\n\t\tVOI_Mem_Usage = %ld\n", total_memory, cur_mem_allocated, available_mem + vw + ggb + lgb, vw, ggb, lgb, vec_static_bitmap, voi_mem);
							}

							int64_t page_buffer_pool_size;
							char* tbgpp_edge_page_buffer_size = getenv("TBGPP_EDGE_PAGE_BUFFER_SIZE");
							double edge_page_buffer_portion = 1;
							if (tbgpp_edge_page_buffer_size != NULL && (edge_page_buffer_portion = std::atof(tbgpp_edge_page_buffer_size)) != -1) {
								int64_t edge_file_size = DiskAioFactory::GetPtr()->GetAioFileSize();
								INVARIANT (edge_page_buffer_portion > 0 && edge_page_buffer_portion <= 1);
								page_buffer_pool_size = edge_file_size * edge_page_buffer_portion;
							} else {
								page_buffer_pool_size = available_mem;

								// 128 MB is enough to saturate even PCI-E SSD :D
								if (page_buffer_pool_size < 128) {
									page_buffer_pool_size = 128;
								}
							}

							if (available_mem - page_buffer_pool_size < 0 || page_buffer_pool_size < config_.page_buffer_size_in_MB) {
								fprintf(stderr, "[Turbograph] OutOfMemory, increase (# Vchunks per Machine) or (Physical Memory Size per Machine)\n");
								abort();
							}
							ALWAYS_ASSERT (page_buffer_pool_size > 0);

							if (config_.page_buffer_size_in_MB != -1) {
								TurboDB::_Init_BufferManager(config_.page_buffer_size_in_MB * 1024L * 1024L);
							} else {
								TurboDB::_Init_BufferManager(page_buffer_pool_size * 1024L * 1024L);
							}
						}

						void AllocateGGB() {
							for (auto& vec: vectors_) {
								vec->PinRandVectorWriteBufferInMemory();
							}
						}
						void ProcessVertices(std::vector<TG_DistributedVectorBase*>& vectors, std::function<void(node_t)> process, bool MT = true) {
							bool use_degree_order = false;
							if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION == true) {
								use_degree_order = true;
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							}
							Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
							for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
								Range<node_t> vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionStatistics::my_machine_id(), chunk_idx);
								Range<int64_t> start_input_vector_chunks(input_vector_chunks.GetBegin(), input_vector_chunks.GetBegin());
								Range<int64_t> cur_input_vector_chunks(input_vector_chunks.GetBegin() + chunk_idx, input_vector_chunks.GetBegin() + chunk_idx);
								nwsm_->ReadInputVectors(vectors, start_input_vector_chunks, cur_input_vector_chunks, Range<int64_t>(-1, -1));
								if (MT) {	
#pragma omp parallel for
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								} else {
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								}
								nwsm_->FlushInputVectorsAfterApplyPhase(vectors, chunk_idx, UserArguments::SUPERSTEP, [](node_t vid){ return true; });
							}
							if (use_degree_order) {
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = true;
							}
						}

						void ProcessVertices(TG_DistributedVectorBase* vec, std::function<void(node_t)> process, bool flush, bool MT = true) {
							bool use_degree_order = false;
							if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION == true) {
								use_degree_order = true;
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							}
							Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
							for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
								Range<node_t> vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionStatistics::my_machine_id(), chunk_idx);
								Range<int64_t> start_input_vector_chunks(input_vector_chunks.GetBegin(), input_vector_chunks.GetBegin());
								Range<int64_t> cur_input_vector_chunks(input_vector_chunks.GetBegin() + chunk_idx, input_vector_chunks.GetBegin() + chunk_idx);
								nwsm_->ReadInputVectors(vec, start_input_vector_chunks, cur_input_vector_chunks, Range<int64_t>(-1, -1));
								if (MT) {	
#pragma omp parallel for
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								} else {
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								}
								if (flush) {
									nwsm_->FlushInputVectorsAfterApplyPhase(chunk_idx, UserArguments::SUPERSTEP, [](node_t vid){ return true; });
								}
							}
							if (use_degree_order) {
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = true;
							}
						}

#ifndef GB_VERSIONED_ARRAY
						void ProcessVertices(VersionedBitMap<bool, LOR>* versioned_bitmap, VersionedBitMap<bool, LOR>* versioned_bitmap_prev, std::function<void(node_t)> process, bool flush, bool MT = true) {
#else
						void ProcessVertices(GBVersionedBitMap<bool, LOR>* versioned_bitmap, GBVersionedBitMap<bool, LOR>* versioned_bitmap_prev, std::function<void(node_t)> process, bool flush, bool MT = true) {
#endif
							bool use_degree_order = false;
							if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION == true) {
								use_degree_order = true;
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							}
							Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
							for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
								Range<node_t> vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionStatistics::my_machine_id(), chunk_idx);
								Range<int64_t> start_input_vector_chunks(input_vector_chunks.GetBegin(), input_vector_chunks.GetBegin());
								Range<int64_t> cur_input_vector_chunks(input_vector_chunks.GetBegin() + chunk_idx, input_vector_chunks.GetBegin() + chunk_idx);

								if (flush) {
									bool construct_prev = (UserArguments::UPDATE_VERSION >= 1);
									versioned_bitmap->ConstructAndFlush(Range<int64_t>(0, PartitionStatistics::my_num_internal_nodes() - 1), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, construct_prev, versioned_bitmap_prev);
								}

								if (MT) {	
#pragma omp parallel for
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								} else {
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								}
								if (flush) {
									versioned_bitmap->FlushAll(UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
								}
							}
							if (use_degree_order) {
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = true;
							}
						}
						void ProcessVerticesWithoutVectors(std::function<void(node_t)> process, bool MT = true) {
							Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
							for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
								Range<node_t> vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionStatistics::my_machine_id(), chunk_idx);
								if (MT) {	
#pragma omp parallel for
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								} else {
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								}
							}
						}

						void ProcessVertices(std::function<void(node_t)> process, bool flush = true, bool MT = true) {
							turbo_timer tim;
							bool use_degree_order = false;
							if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION == true) {
								use_degree_order = true;
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							}
							Range<int64_t> input_vector_chunks = PartitionStatistics::my_vector_chunks();
							for (int64_t chunk_idx = 0; chunk_idx < UserArguments::VECTOR_PARTITIONS; chunk_idx++) {
								Range<node_t> vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(PartitionStatistics::my_machine_id(), chunk_idx);
								Range<int64_t> start_input_vector_chunks(input_vector_chunks.GetBegin(), input_vector_chunks.GetBegin());
								Range<int64_t> cur_input_vector_chunks(input_vector_chunks.GetBegin() + chunk_idx, input_vector_chunks.GetBegin() + chunk_idx);
								tim.start_timer(0);
								nwsm_->ReadInputVectors(start_input_vector_chunks, cur_input_vector_chunks, Range<int64_t>(-1, -1));
								tim.stop_timer(0);
								tim.start_timer(1);
								if (MT) {
									int64_t chunk_size = (((vid_range.length() / UserArguments::NUM_THREADS) + 63) / 64) * 64;
#pragma omp parallel for schedule(static, chunk_size)
									//#pragma omp parallel for
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								} else {
									for (node_t i = vid_range.GetBegin() ; i <= vid_range.GetEnd(); i++)  {
										process(i);
									}
								}
								tim.stop_timer(1);
								tim.start_timer(2);
								if (flush) {
									nwsm_->FlushInputVectorsAfterApplyPhase(chunk_idx, UserArguments::SUPERSTEP, [](node_t vid){ return true; });
								}
								tim.stop_timer(2);
							}
							if (use_degree_order) {
								UserArguments::USE_DEGREE_ORDER_REPRESENTATION = true;
							}
#ifdef PRINT_PROFILING_TIMERS
							fprintf(stdout, "[%ld] ProcessVertices %.3f %.3f %.3f\n", PartitionStatistics::my_machine_id(), tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
#endif
						}

						void set_context (Range<int64_t> src_edge_part, Range<int64_t> dst_edge_part) {
							current_src_edge_partitions = src_edge_part;
							current_dst_edge_partitions = dst_edge_part;

							src_begin_machine_id = current_src_edge_partitions.GetBegin() / UserArguments::VECTOR_PARTITIONS;
							src_begin_chunk_id = current_src_edge_partitions.GetBegin() % UserArguments::VECTOR_PARTITIONS;

							src_end_machine_id = current_src_edge_partitions.GetEnd() / UserArguments::VECTOR_PARTITIONS;
							src_end_chunk_id = current_src_edge_partitions.GetEnd() % UserArguments::VECTOR_PARTITIONS;

							dst_begin_machine_id = current_dst_edge_partitions.GetBegin() / UserArguments::VECTOR_PARTITIONS;
							dst_begin_chunk_id = current_dst_edge_partitions.GetBegin() % UserArguments::VECTOR_PARTITIONS;

							dst_end_machine_id = current_dst_edge_partitions.GetEnd() / UserArguments::VECTOR_PARTITIONS;
							dst_end_chunk_id = current_dst_edge_partitions.GetEnd() % UserArguments::VECTOR_PARTITIONS;

							src_edge_partition_vids.Set (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(src_begin_machine_id, src_begin_chunk_id).GetBegin(), PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(src_end_machine_id, src_end_chunk_id).GetEnd());
							dst_edge_partition_vids.Set (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(dst_begin_machine_id, dst_begin_chunk_id).GetBegin(), PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(dst_end_machine_id, dst_end_chunk_id).GetEnd());
						}

						ReturnStatus ScheduleNextEdgePartition(int schedule_type, Range<int64_t> start_src_edge_partitions, Range<int64_t> start_dst_edge_partitions, Range<int64_t>& src_edge_partitions, Range<int64_t>& dst_edge_partitions) {

							int64_t src_edge_chunk_length = start_src_edge_partitions.length();
							int64_t dst_edge_chunk_length = start_dst_edge_partitions.length();
							ALWAYS_ASSERT(UserArguments::VECTOR_PARTITIONS % src_edge_chunk_length == 0);
							ALWAYS_ASSERT(PartitionStatistics::num_target_vector_chunks() % dst_edge_chunk_length == 0);
							if(schedule_type == 0) {
								//row-direction
								//
								//  ---------------->
								//  ---------------->
								//  ---------------->
								//

								if(src_edge_partitions == Range<int64_t> (-1,-1)) { // first time
									ALWAYS_ASSERT(dst_edge_partitions == Range<int64_t> (-1,-1));
									src_edge_partitions = start_src_edge_partitions;
									dst_edge_partitions = start_dst_edge_partitions;
									return ReturnStatus::OK;
								}
								if(dst_edge_partitions.GetEnd() == PartitionStatistics::num_target_vector_chunks() -1) {
									dst_edge_partitions.Set(0, dst_edge_chunk_length - 1);
									if(src_edge_partitions.GetEnd() == PartitionStatistics::my_vector_chunks().GetEnd())
										src_edge_partitions.Set(PartitionStatistics::my_vector_chunks().GetBegin(), PartitionStatistics::my_vector_chunks().GetBegin()+src_edge_chunk_length -1);
									else
										src_edge_partitions.Set(src_edge_partitions.GetEnd() +1, src_edge_partitions.GetEnd() + src_edge_chunk_length);
								} else {
									dst_edge_partitions.Set(dst_edge_partitions.GetEnd() + 1, dst_edge_partitions.GetEnd() + dst_edge_chunk_length);
								}
								if(src_edge_partitions == start_src_edge_partitions && dst_edge_partitions == start_dst_edge_partitions)
									return ReturnStatus::DONE;
								else
									return ReturnStatus::OK;

							} else if(schedule_type == 1) {
								// column-direction
								//
								//    | | | |
								//    | | | |
								//    | | | |
								//    V V V V
								//

								if(src_edge_partitions == Range<int64_t> (-1,-1)) { // first time
									ALWAYS_ASSERT(dst_edge_partitions == Range<int64_t> (-1,-1));
									src_edge_partitions = start_src_edge_partitions;
									dst_edge_partitions = start_dst_edge_partitions;
									return ReturnStatus::OK;
								}
								if(src_edge_partitions.GetEnd() == PartitionStatistics::my_vector_chunks().GetEnd()) {

									src_edge_partitions.Set(PartitionStatistics::my_vector_chunks().GetBegin(), PartitionStatistics::my_vector_chunks().GetBegin()+src_edge_chunk_length -1);
									if(dst_edge_partitions.GetEnd() == PartitionStatistics::num_target_vector_chunks() -1)
										dst_edge_partitions.Set(0, dst_edge_chunk_length -1);
									else
										dst_edge_partitions.Set(dst_edge_partitions.GetEnd() +1, dst_edge_partitions.GetEnd() + dst_edge_chunk_length);
								} else {
									src_edge_partitions.Set(src_edge_partitions.GetEnd() + 1, src_edge_partitions.GetEnd() + src_edge_chunk_length);
								}
								if(src_edge_partitions == start_src_edge_partitions && dst_edge_partitions == start_dst_edge_partitions)
									return ReturnStatus::DONE;
								else
									return ReturnStatus::OK;

							} else if(schedule_type == 2) {
								// twisted-derection
								//
								//     _   _   _
								//  | | | | | | |  ->
								//  |_| |_| |_| |_|
								//
								if(src_edge_partitions == Range<int64_t> (-1,-1)) { // first time
									ALWAYS_ASSERT(dst_edge_partitions == Range<int64_t> (-1,-1));
									src_edge_partitions = start_src_edge_partitions;
									dst_edge_partitions = start_dst_edge_partitions;
									return ReturnStatus::OK;
								}
								if((dst_edge_partitions.GetBegin()/dst_edge_chunk_length) % 2 == 0) { // go down
									if(src_edge_partitions.GetEnd() == PartitionStatistics::my_vector_chunks().GetEnd()) {
										if(dst_edge_partitions.GetEnd() == PartitionStatistics::num_target_vector_chunks() -1) {
											dst_edge_partitions.Set(0, dst_edge_chunk_length -1);
										} else {
											dst_edge_partitions.Set(dst_edge_partitions.GetEnd() +1, dst_edge_partitions.GetEnd() + dst_edge_chunk_length);
										}
									} else {
										src_edge_partitions.Set(src_edge_partitions.GetEnd() + 1, src_edge_partitions.GetEnd() + src_edge_chunk_length);
									}
								} else { // go up
									if(src_edge_partitions.GetBegin() == PartitionStatistics::my_vector_chunks().GetBegin()) {
										if(dst_edge_partitions.GetEnd() == PartitionStatistics::num_target_vector_chunks() -1) {
											dst_edge_partitions.Set(0, dst_edge_chunk_length -1);
										} else {
											dst_edge_partitions.Set(dst_edge_partitions.GetEnd() +1, dst_edge_partitions.GetEnd() + dst_edge_chunk_length);
										}
									} else {
										src_edge_partitions.Set(src_edge_partitions.GetBegin() - src_edge_chunk_length, src_edge_partitions.GetBegin() -1);
									}
								}
								if(src_edge_partitions == start_src_edge_partitions && dst_edge_partitions == start_dst_edge_partitions)
									return ReturnStatus::DONE;
								else
									return ReturnStatus::OK;
							} else if(schedule_type == 3) {
								//  odd number's superstep: forward row-direction
								//
								//  ---------------->
								//  ---------------->
								//  ---------------->
								//
								//  even number's superstep: backward row-direction
								//
								//  <----------------
								//  <----------------
								//  <----------------
								//
								if(iteration % 2 == 0) {
									if(src_edge_partitions == Range<int64_t> (-1,-1)) { // first time
										ALWAYS_ASSERT(dst_edge_partitions == Range<int64_t> (-1,-1));
										src_edge_partitions = start_src_edge_partitions;
										dst_edge_partitions = start_dst_edge_partitions;
										return ReturnStatus::OK;
									}
									dst_edge_partitions.Set((dst_edge_partitions.GetEnd() + 1) % PartitionStatistics::num_target_vector_chunks(), (dst_edge_partitions.GetEnd() + dst_edge_chunk_length) % PartitionStatistics::num_target_vector_chunks());
									if(dst_edge_partitions.GetEnd() == 0) {
										int64_t next_src_edge_partition = (src_edge_partitions.GetEnd() + 1) % UserArguments::VECTOR_PARTITIONS + PartitionStatistics::my_vector_chunks().GetBegin();
										src_edge_partitions.Set(next_src_edge_partition, next_src_edge_partition + src_edge_chunk_length - 1);
									}
									if(src_edge_partitions == start_src_edge_partitions && dst_edge_partitions == start_dst_edge_partitions)
										return ReturnStatus::DONE;
									else
										return ReturnStatus::OK;
								} else {
									if(src_edge_partitions == Range<int64_t> (-1,-1)) { // first time
										ALWAYS_ASSERT(dst_edge_partitions == Range<int64_t> (-1,-1));
										src_edge_partitions = start_src_edge_partitions;
										dst_edge_partitions = start_dst_edge_partitions;
										return ReturnStatus::OK;
									}
									int64_t next_dst_edge_partition = (dst_edge_partitions.GetBegin() - 1 + PartitionStatistics::num_target_vector_chunks())%PartitionStatistics::num_target_vector_chunks();
									dst_edge_partitions.Set(next_dst_edge_partition, next_dst_edge_partition);
									if(dst_edge_partitions.GetBegin() == PartitionStatistics::num_target_vector_chunks() - 1) {
										int64_t next_src_edge_partition = (src_edge_partitions.GetEnd() - 1 + UserArguments::VECTOR_PARTITIONS) % UserArguments::VECTOR_PARTITIONS + PartitionStatistics::my_vector_chunks().GetBegin();
										src_edge_partitions.Set(next_src_edge_partition, next_src_edge_partition + src_edge_chunk_length - 1);
									}
									if(src_edge_partitions == start_src_edge_partitions && dst_edge_partitions == start_dst_edge_partitions)
										return ReturnStatus::DONE;
									else
										return ReturnStatus::OK;
								}

							} else {
								fprintf(stdout,"Not Defined schedule type!!\n");
								abort();
							}
							return ReturnStatus::OK;
						}

						// Default scheduling
						virtual void ScheduleVectorWindow(int machine_id, std::vector<Range<int64_t>>& sched_input_vector_chunks, std::vector<Range<int64_t>>& sched_output_vector_chunks, std::vector<bool>& sched_input_vector_chunks_active_flag, std::vector<bool>& sched_output_vector_chunks_active_flag) {
							sched_input_vector_chunks.resize(UserArguments::VECTOR_PARTITIONS);
							sched_input_vector_chunks_active_flag.resize(UserArguments::VECTOR_PARTITIONS, true);

							int first_i;

							if (UserArguments::USE_FULLIST_DB && UserArguments::ITERATOR_MODE == PARTIAL_LIST) {
								sched_output_vector_chunks.resize(1);
								sched_output_vector_chunks_active_flag.resize(1, true);
								first_i = 0;
							} else {
								sched_output_vector_chunks.resize(PartitionStatistics::num_target_vector_chunks());
								sched_output_vector_chunks_active_flag.resize(PartitionStatistics::num_target_vector_chunks(), true);
								first_i = machine_id * UserArguments::VECTOR_PARTITIONS;
							}

							for (int i = 0; i < UserArguments::VECTOR_PARTITIONS; i++) {
								int begin = machine_id * UserArguments::VECTOR_PARTITIONS + i;
								int end = machine_id * UserArguments::VECTOR_PARTITIONS + i;
								sched_input_vector_chunks[i].Set(begin, end);;
								sched_input_vector_chunks_active_flag[i] = true;
							}

							for (int i = 0; i < sched_output_vector_chunks.size(); i++) {
								int begin = (first_i + i) % PartitionStatistics::num_target_vector_chunks();
								int end = (first_i + i) % PartitionStatistics::num_target_vector_chunks();
								sched_output_vector_chunks[i].Set(begin, end);
								sched_output_vector_chunks_active_flag[i] = true;
							}
							Range<int64_t> end_of_range(-1, -1);
							sched_input_vector_chunks.push_back(end_of_range);
							sched_output_vector_chunks.push_back(end_of_range);
						}

						void RebuildDB_MakeEdgePagesWithMatAdjLists(MaterializedAdjacencyLists& mat_adj, int64_t q_new, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk, std::vector<Page>& page_to_append, std::vector<HomogeneousPageWriter*>& edge_files, std::vector<Range<node_t>>& range_to_append, VidRangePerPage& vidrangeperpage) {
							EdgePairEntry<node_t> edge_iter;
							EdgePairEntry<node_t> prev_edge_iter;
							prev_edge_iter.src_vid_ = prev_edge_iter.dst_vid_ = -1;
							int64_t num_subchunks_per_edge_chunk = UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;
							int64_t num_files = q_new * PartitionStatistics::num_machines() * UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;

							int64_t idx = 0;
							node_t* begin = (node_t*) mat_adj.Data();
							for(int64_t adj_idx = 0 ; adj_idx < mat_adj.GetNumAdjacencyLists() ; adj_idx++) {
								node_t int_src_vid = mat_adj.GetSlotVid(adj_idx);
								node_t* int_data = &begin[idx];
								node_t int_sz = mat_adj.GetSlot(adj_idx)->end_offset - idx;

								idx += int_sz;

								// Append edges
								for (int64_t i = 0; i < int_sz; i++) {
									edge_iter.src_vid_ = int_src_vid;
									edge_iter.dst_vid_ = int_data[i];

									//remove duplicate edges & self_edges
									if(edge_iter == prev_edge_iter) continue;
									if(edge_iter.src_vid_ == edge_iter.dst_vid_) continue;

									if (edge_iter.src_vid_ < PartitionStatistics::my_first_node_id() || edge_iter.src_vid_ > PartitionStatistics::my_last_node_id()) {
										fprintf(stderr, "Edge (%lld, %lld), VidRange [%lld, %lld]\n", edge_iter.src_vid_, edge_iter.dst_vid_, PartitionStatistics::my_first_node_id(), PartitionStatistics::my_last_node_id());
									}

									ALWAYS_ASSERT(prev_edge_iter.src_vid_ == -1 || prev_edge_iter < edge_iter);
									ALWAYS_ASSERT(edge_iter.src_vid_ >= PartitionStatistics::my_first_node_id());
									ALWAYS_ASSERT(edge_iter.src_vid_ <= PartitionStatistics::my_last_node_id());
									ALWAYS_ASSERT(edge_iter.dst_vid_ < PartitionStatistics::num_total_nodes());

									int64_t dst_file = dst_vid_to_file_num(edge_iter.dst_vid_, num_subchunks_per_edge_chunk, q_new, SubchunkVidRangePerEdgeChunk);
									if (dst_file < 0 || dst_file >= num_files) {
										fprintf(stdout, "edge (%lld, %lld), dst_file = %lld, num_files = %lld\n", edge_iter.src_vid_, edge_iter.dst_vid_, dst_file, num_files);
									}
									ALWAYS_ASSERT(0 <= dst_file && dst_file < num_files);

									if(MakePage(page_to_append[dst_file], edge_iter) == ReturnStatus::DONE) {
										ALWAYS_ASSERT(range_to_append[dst_file].GetBegin() <PartitionStatistics::num_total_nodes());
										vidrangeperpage.push_back(dst_file, range_to_append[dst_file]);
										range_to_append[dst_file].Set(edge_iter.src_vid_, edge_iter.src_vid_);
										edge_files[dst_file]->AppendPage(page_to_append[dst_file]);
										CleanPage(page_to_append[dst_file]);

										MakePage(page_to_append[dst_file], edge_iter);
									} else {
										if(range_to_append[dst_file].GetBegin() == -1)
											range_to_append[dst_file].SetBegin(edge_iter.src_vid_);
										range_to_append[dst_file].SetEnd(edge_iter.src_vid_);
									}

									prev_edge_iter = edge_iter;
								}
							}
						}


						virtual void ApplyWindowMemoryAssignmentPolicy(int64_t total_bytes, std::vector<int64_t>& bytes_per_window) {
							system_fprintf(0, stdout, "ApplyWindowMemoryAssignmentPolicy with %ld MB\n", total_bytes / (1024 * 1024L));
							bytes_per_window.clear();
							int64_t last_level_edge_streaming_window_size = 2 * 1024 * 1024 * 1024L; // 2 GB
							INVARIANT(total_bytes - last_level_edge_streaming_window_size > 0);
							for (int lv = 0; lv < UserArguments::MAX_LEVEL - 1; lv++) {
								bytes_per_window.push_back ((total_bytes - last_level_edge_streaming_window_size) / (UserArguments::MAX_LEVEL - 1));
							}
							bytes_per_window.push_back(last_level_edge_streaming_window_size);
						}

						std::vector<TG_DistributedVectorBase*>& Vectors() {
							return vectors_;
						}

						TwoLevelBitMap<node_t>& GetGenOld() {
							return genOld_vid_map;
						}

						TwoLevelBitMap<node_t>& GetGenNew() {
							return genNew_vid_map;
						}

						int64_t dst_vid_to_file_num (int64_t dst_vid, int64_t num_subchunks_per_edge_chunk, int64_t q_new, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
							int64_t idx = dst_vid / PartitionStatistics::my_num_internal_nodes();
							idx *= q_new;
							int j;
							for(j = idx; j < idx + q_new; j++) {
								for(int i = 0; i < num_subchunks_per_edge_chunk; i++) {
									if(dst_vid <= SubchunkVidRangePerEdgeChunk[j][i])
										return (j * num_subchunks_per_edge_chunk + i);
								}
							}
							LOG_ASSERT(false);
							abort();
						}

						ReturnStatus MakePage(Page& page, EdgePairEntry<node_t> edge) {
							node_t prev_src;
							if(page.NumEntry() == 0) {
								prev_src = -1;
							} else {
								prev_src = page.GetSlot(page.NumEntry() -1)->src_vid;
							}

							if(prev_src != edge.src_vid_) {

								int64_t free_space = 0;
								if(page.NumEntry() != 0)
									free_space = page.GetSlot(page.NumEntry() -1)->end_offset;

								if((void *)&page[free_space] >= (void *)page.GetSlot(page.NumEntry()))
									return ReturnStatus::DONE;
								page.NumEntry()++;
								ALWAYS_ASSERT(page[free_space] == 0);
								page[free_space] = edge.dst_vid_;
								page.GetSlot(page.NumEntry() -1)->src_vid = edge.src_vid_;
								page.GetSlot(page.NumEntry() -1)->end_offset = free_space + 1;
							} else {
								int64_t free_space = page.GetSlot(page.NumEntry() -1)->end_offset;
								if((void *)&page[free_space] >= (void *)page.GetSlot(page.NumEntry() -1))
									return ReturnStatus::DONE;
								ALWAYS_ASSERT(page[free_space] == 0);
								page[free_space] = edge.dst_vid_;
								page.GetSlot(page.NumEntry() -1)->end_offset = free_space +1;
							}
							return ReturnStatus::OK;
						}

						ReturnStatus CleanPage(Page& page) {
							for(int64_t i = 0 ; i < TBGPP_PAGE_SIZE / sizeof(node_t) ; i++)
								page[i] = 0;
							return ReturnStatus::DONE;
						}

						// Size of output vectors
						int64_t ComputeSizeOfVWs() {
							int64_t bytes = 0;
							for(auto& vec : vectors_) {
								bytes += vec->GetSizeOfVW();
							}
							if (UserArguments::ITERATOR_MODE == FULL_LIST) {
								bytes += sizeof(AdjacencyListMemoryPointer) * PartitionStatistics::max_num_nodes_per_vector_chunk();
							}
							MPI_Allreduce(MPI_IN_PLACE, &bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							//RequestRespond::TurboAllReduceSync<int64_t, PLUS>((char*)&bytes, 1, sizeof(int64_t));
							return bytes;
						}

						// Size of output vectors
						int64_t ComputeSizeOfLGBs() {
							int64_t bytes = 0;
							for(auto& vec : vectors_) {
								bytes += vec->GetSizeOfLGB();
							}
							MPI_Allreduce(MPI_IN_PLACE, &bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							//RequestRespond::TurboAllReduceSync<int64_t, PLUS>((char*)&bytes, 1, sizeof(int64_t));
							return bytes;
						}

						// Size of global gather buffer per output vector
						int64_t ComputeSizeOfGGB() {
							int64_t bytes = 0;
							for(auto& vec : vectors_) {
								bytes += vec->GetSizeOfGGB();
							}
							MPI_Allreduce(MPI_IN_PLACE, &bytes, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
							//RequestRespond::TurboAllReduceSync<int64_t, PLUS>((char*)&bytes, 1, sizeof(int64_t));
							return bytes;
						}
						int64_t ComputeQmin() {
							double M_total = UserArguments::PHYSICAL_MEMORY;
							int64_t PS = 2 * TBGPP_PAGE_SIZE / (1024*1024); // TODO
							int64_t p = PartitionStatistics::num_machines();
							int64_t k = UserArguments::MAX_LEVEL;
							double M_voi = 1 + (double) k * PartitionStatistics::num_total_nodes() / (double)(8*1024*1024);
							double M_vector = 1 + (ComputeSizeOfVWs() + ComputeSizeOfLGBs() + ComputeSizeOfGGB()) / (1024*1024);


							int64_t q_min = std::ceil((double)1/(double)p * (double)(M_vector) / (double)(M_total - k*2*PS - M_voi));
							if (!(q_min >= 1)) {
								system_fprintf(0, stdout, "%ld\t%ld\t%ld\t%ld\n", q_min, M_vector, (int64_t) M_total, (int64_t) M_voi);
								q_min = 1;
							}
							INVARIANT (q_min >= 1);
							return q_min;
						}

						bool RebuildDBIfNeeded() {
							int64_t q_old = UserArguments::VECTOR_PARTITIONS;
							int64_t q_new = ComputeQmin();


							//if (true) {
							if (q_new > q_old) {
								system_fprintf(0, stdout, "### Repartitioning Q_old = %ld -> Q_new = %ld ###\n", q_old, q_new);

								LOG_ASSERT(false);

								// Rerun BBP
								RebuildDBwithQnew(q_new);

								// Reload DB
								load_db();

								// Auto tune buffer pool size again
								AutoTuneBufferPoolSize();
								return true;
							} else {
								system_fprintf(0, stdout, "### Q = %ld ###\n", q_old);
								return false;
							}
						}

						void RebuildDBwithQnew (int64_t q_new) {
							std::string root_path, new_root_path;
							std::string hist_path;

							// Input Path
							root_path = UserArguments::WORKSPACE_PATH ;
							new_root_path = root_path + "/tmproot/";

							{
								mode_t old_umask;
								old_umask = umask(0);
								int is_error = mkdir((new_root_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
								if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
									fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (new_root_path).c_str(), errno);
									umask(old_umask);
									return;
								}
								umask(old_umask);
							}

							// Compute vid range of subchunks for each edge chunk
							std::vector<std::vector<int64_t>> SubchunkVidRangePerEdgeChunk;
							ComputeBoundariesOfEdgeSubchunks(q_new, root_path, new_root_path, SubchunkVidRangePerEdgeChunk);

							// Rebuild DB
							ReadAllEdgesAndRebuildDB(q_new, root_path, new_root_path, SubchunkVidRangePerEdgeChunk);

							// Close DB //XXX change this --> record which DB is opened when LoadDB, and close those when CloseDB
							pTurboDB_[0]->CloseDB(true);
							pTurboDB_[1]->CloseDB(true);
							INVARIANT(DiskAioFactory::GetPtr() != NULL);
							delete DiskAioFactory::GetPtr();
							DiskAioFactory::GetPtr() = NULL;

							// XXX
							std::string old_edge_path = root_path + "/edgedb";
							{
								int err = remove((old_edge_path + "/edgedb").c_str());
								if (err != 0) {
									fprintf(stdout, "Cannot delete %s ErrorNo=%ld\n", (old_edge_path + "/edgedb").c_str(), errno);
								}
								err = remove((old_edge_path).c_str());
								if (err != 0) {
									fprintf(stdout, "Cannot delete %s ErrorNo=%ld\n", old_edge_path.c_str(), errno);
								}
							}

							// Rename Files
							std::vector<std::string> new_files;
							DiskAioFactory::GetAioFileList(new_root_path, new_files);
							for(int64_t i = 0 ; i < new_files.size() ; i++) {
								std::string tmp_path(new_root_path + new_files[i]);
								std::string new_path(root_path + new_files[i]);
								rename(tmp_path.c_str(), new_path.c_str());
							}

							remove(new_root_path.c_str());
							PartitionStatistics::wait_for_all();
						}

						void ReadAllEdgesAndRebuildDB(int64_t q_new, const std::string& root_path, const std::string& new_root_path, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
							Range<node_t> vid_range(PartitionStatistics::my_first_node_id(), PartitionStatistics::my_last_node_id());
							Range<node_t> cur_vid_range(-1, -1);
							Range<int64_t> dst_vector_chunks(0, PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS - 1);
							Range<int64_t> dst_edge_partitions = PartitionStatistics::vector_chunks_to_edge_subchunks(dst_vector_chunks);
							BitMap<node_t> vids;

							// initialize
							vids.Init(PartitionStatistics::my_num_internal_nodes());
							vids.SetAll();
							int64_t prev_degree_threshold = UserArguments::DEGREE_THRESHOLD;
							bool prev_use_degree_order_representation = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
							IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;

							UserArguments::DEGREE_THRESHOLD = 0;
							UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							UserArguments::ITERATOR_MODE = FULL_LIST;

							EdgePairEntry<int64_t> edge_iter_64bits;
							int64_t num_files = q_new * PartitionStatistics::num_machines() * UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;
							std::vector<int64_t> page_offset(num_files, 0);
							std::vector<HomogeneousPageWriter*> edge_files(num_files, NULL);
							std::vector<Page> page_to_append(num_files);
							std::vector<Range<node_t> > range_to_append(num_files);

							// file paths
							std::string vid_range_per_page_file_path = new_root_path + "edgedbVidRangePerPage";
							std::string fpid_path = new_root_path + "FirstPageIds.txt";
							std::string new_edge_path = new_root_path + "/edgedb";

							VidRangePerPage vidrangeperpage(num_files, vid_range_per_page_file_path);
							for(int64_t i = 0 ; i < num_files ; i++) {
								edge_files[i] = new HomogeneousPageWriter((new_edge_path+std::to_string(i)).c_str());
								CleanPage(page_to_append[i]);
								range_to_append[i].Set(-1,-1);
							}

							int64_t num_subchunks_per_edge_chunk = UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;
							EdgePairEntry<node_t> edge_iter;
							EdgePairEntry<node_t> prev_edge_iter;
							prev_edge_iter.src_vid_ = prev_edge_iter.dst_vid_ = -1;

							node_t cur_vid = PartitionStatistics::my_first_node_id();
							while (cur_vid <= vid_range.GetEnd()) {
								if (cur_vid_range.GetBegin() == -1) {
									cur_vid_range.SetBegin(cur_vid);
								}
								cur_vid_range.SetEnd(cur_vid);

								// XXX
								if (cur_vid_range.length() >= 1024 || cur_vid == vid_range.GetEnd()) {
									MaterializedAdjacencyLists mat_adj;
									LOG_ASSERT(false);
									//mat_adj.MaterializeFullListSykoTemp(&vids, cur_vid_range, dst_edge_partitions, -1); // deleted at 2019.08.31

									RebuildDB_MakeEdgePagesWithMatAdjLists(mat_adj, q_new, SubchunkVidRangePerEdgeChunk, page_to_append, edge_files, range_to_append, vidrangeperpage);

									SimpleContainer cont = mat_adj.GetContainer();
									char*	tmp_container = cont.data;
									int64_t tmp_container_capacity = cont.capacity;
									TurboFree(tmp_container, tmp_container_capacity);

									cur_vid_range.Set(-1, -1);
								}
								cur_vid++;
							}

							// Flush the remaining edges in the pages and close the files
							for(int64_t i = 0 ; i < num_files; i++) {
								node_t* pagedata = (node_t*)page_to_append[i].GetData();
								if(pagedata[TBGPP_PAGE_SIZE/sizeof(node_t) -1] != 0) {
									vidrangeperpage.push_back(i, range_to_append[i]);
									edge_files[i]->AppendPage(page_to_append[i]);
								}
								edge_files[i]->Close(false);;
								delete edge_files[i];
							}

							// write VidRangePerPage data into disk
							vidrangeperpage.Save();

							// concatenate the edge chunk files into a single File.
							size_t total_file_size = 0;
							const size_t transfer_unit = TBGPP_PAGE_SIZE;

							std::ofstream fpid;
							fpid.open(fpid_path.c_str());
							{
								remove(new_edge_path.c_str());
								mode_t old_umask;
								old_umask = umask(0);
								int is_error = mkdir((new_edge_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
								if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
									fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (new_edge_path).c_str(), errno);
									umask(old_umask);
									return;
								}
								umask(old_umask);
							}

							Turbo_bin_io_handler out_file((new_edge_path + "/edgedb").c_str(), true, true);
							char* buffer = new char[transfer_unit];
							for (auto i = 0; i < num_files; i++) {
								Turbo_bin_io_handler in_file((new_edge_path+std::to_string(i)).c_str(), false, false);
								size_t file_size = in_file.file_size();
								size_t data_left = file_size;
								while (data_left) {
									size_t to_read = MIN(data_left, transfer_unit);
									in_file.Read(file_size - data_left, to_read, buffer);
									out_file.Append(to_read, buffer);
									data_left -= to_read;
								}
								fpid << i << " " << (total_file_size / transfer_unit) << std::endl;
								total_file_size += file_size;
								in_file.Close(true);
							}
							out_file.Close();
							fpid.close();
							delete[] buffer;

							PartitionStatistics::wait_for_all();
							system_fprintf(0, stdout, "Repartitioning with Q = %ld finished\n", q_new);
							PartitionStatistics::wait_for_all();

							UserArguments::DEGREE_THRESHOLD = prev_degree_threshold;
							UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order_representation;
							UserArguments::ITERATOR_MODE = prev_iterator_mode;
							return;
						}

						void ComputeBoundariesOfEdgeSubchunks(int64_t q_new, const std::string& root_path, const std::string& new_root_path, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
							int32_t num_edge_chunks = PartitionStatistics::num_machines() * q_new;
							std::string hist_path = root_path + "/LocalHistogram/";
							std::string old_subrange_path = root_path + "/EdgeSubChunkVidRanges/";
							std::string new_subrange_path = new_root_path + "/EdgeSubChunkVidRanges/";

							std::vector<std::vector<int32_t>> InDegreeHistogramPerEdgeChunk;
							std::vector<int64_t> SumOfInDegreePerEdgeChunk;

							node_t bin_width;
							std::ifstream* local_histogram = new std::ifstream[PartitionStatistics::num_machines()];
							InDegreeHistogramPerEdgeChunk.resize(PartitionStatistics::num_machines());
							for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
								std::string file_name = hist_path + std::to_string(i) + ".txt";
								local_histogram[i].open(file_name);

								ALWAYS_ASSERT (local_histogram[i].is_open());

								std::string temp_str;
								std::stringstream temp_stream;
								int64_t num_bins, degree;
								int64_t cnt = 0;

								getline(local_histogram[i], temp_str);
								temp_stream.clear();
								temp_stream.str(temp_str);

								temp_stream >> num_bins >> bin_width;
								InDegreeHistogramPerEdgeChunk[i].resize(num_bins);

								while(getline(local_histogram[i], temp_str)) {
									temp_stream.clear();
									temp_stream.str(temp_str);

									temp_stream >> degree;
									InDegreeHistogramPerEdgeChunk[i][cnt] = degree;
									cnt++;
								}
								ALWAYS_ASSERT(cnt == num_bins);
								local_histogram[i].close();
							}
							delete local_histogram;
							local_histogram = NULL;

							int64_t num_subchunks_per_edge_chunk = UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;
							int64_t num_bins = InDegreeHistogramPerEdgeChunk[0].size();

							std::vector<std::vector<node_t>> SubchunkDegreeSum;
							SumOfInDegreePerEdgeChunk.resize(num_edge_chunks);
							SubchunkVidRangePerEdgeChunk.resize(num_edge_chunks);
							SubchunkDegreeSum.resize(num_edge_chunks);

							int64_t idx2 = 0;
							// Find Vid Range for Subchunks for each Edge Chunk
							for(int i = 0; i < num_edge_chunks; i++) {
								SumOfInDegreePerEdgeChunk[i] = 0;
								SubchunkVidRangePerEdgeChunk[i].resize(num_subchunks_per_edge_chunk);

								int partition_id = i / q_new;
								int chunk_id = i % q_new;
								int64_t start_bin = (InDegreeHistogramPerEdgeChunk[partition_id].size() * chunk_id) / q_new;
								int64_t end_bin = ceil((double)(InDegreeHistogramPerEdgeChunk[partition_id].size() * (chunk_id + 1)) / q_new);
								for(int64_t idx = start_bin; idx < end_bin; idx++) {
									SumOfInDegreePerEdgeChunk[i] += InDegreeHistogramPerEdgeChunk[partition_id][idx];
								}
								//fprintf(stdout, "SumOfInDegreePerEdgeChunks[%lld] = %lld\n", i, SumOfInDegreePerEdgeChunk[i]);

								int64_t num_edges_per_subchunk = (SumOfInDegreePerEdgeChunk[i] + num_subchunks_per_edge_chunk - 1) / num_subchunks_per_edge_chunk;

								int64_t acc_sum = 0;
								int64_t cur_subchunk_idx = 0;
								int64_t degree_sum = 0;

								for(int64_t idx = start_bin; idx < end_bin; idx++) {
									acc_sum += InDegreeHistogramPerEdgeChunk[partition_id][idx];
									degree_sum += InDegreeHistogramPerEdgeChunk[partition_id][idx];
									if(acc_sum >= num_edges_per_subchunk * (cur_subchunk_idx + 1)) {
										SubchunkVidRangePerEdgeChunk[i][cur_subchunk_idx] = ceil((double)((idx2 + idx + 1) * bin_width - 1) / 64) * 64;
										cur_subchunk_idx++;
										if(cur_subchunk_idx == num_subchunks_per_edge_chunk) {
											SubchunkVidRangePerEdgeChunk[i][cur_subchunk_idx - 1] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
											break;
										}
									}
								}
								if(cur_subchunk_idx < num_subchunks_per_edge_chunk) {
									for(int j = cur_subchunk_idx; j < num_subchunks_per_edge_chunk; j++) {
										//fprintf(stdout, "%lld\n", i);
										SubchunkVidRangePerEdgeChunk[i][j] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
									}
								}
								if(chunk_id == q_new - 1) {
									idx2 += InDegreeHistogramPerEdgeChunk[partition_id].size();
								}
							}

							remove((old_subrange_path + "/tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt").c_str());
							remove(old_subrange_path.c_str());
							{
								mode_t old_umask;
								old_umask = umask(0);
								int is_error = mkdir((new_subrange_path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
								if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
									fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (new_subrange_path).c_str(), errno);
									umask(old_umask);
									return;
								}
								umask(old_umask);
							}
							std::ofstream subrange_file((new_subrange_path + "/tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt").c_str());
							for(int i = 0; i < num_edge_chunks; i++) {
								for(int j = 0; j < num_subchunks_per_edge_chunk; j++) {
									if(i == 0 && j == 0) {
										ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
										subrange_file << 0 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
									} else if(j == 0) {
										ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
										subrange_file << SubchunkVidRangePerEdgeChunk[i - 1][num_subchunks_per_edge_chunk - 1] + 1 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
									} else {
										ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
										subrange_file << SubchunkVidRangePerEdgeChunk[i][j - 1] + 1 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
									}
									//fprintf(stdout, "Degree sum of subchunk[%lld][%lld] = %lld\n", i, j, SubchunkDegreeSum[i][j]);
								}
							}
							PartitionStatistics::wait_for_all();
							subrange_file.close();
						}

						void ResetTimers() {
              debug_timer_.reset_timer(0);
							timer_.reset_timer(0);
							cpu_timer_.reset_timer(0);
							cpu_timer_.reset_timer(1);
							prev_main_thread_cpu_time = 0;
							prev_time = 0;
							prev_cpu_time = 0;
							prev_openmp_threads_time = 0;
							prev_udf_cpu_time = 0;

							nwsm_->ResetTimers();
						}

						void StartTimers() {
              debug_timer_.start_timer(0);
							timer_.start_timer(0);
							cpu_timer_.start_timer(0);
							cpu_timer_.start_timer(1);
							start_main_thread_timer();
							start_openmp_threads_timer();
						}

						void EndTimers(double main_thread_cpu_time, double cpu_time, double openmp_threads_cpu_time, double udf_threads_cpu_time) {
              debug_timer_.stop_timer(0);
							timer_.stop_timer(0);

							MPI_Allreduce(MPI_IN_PLACE, &main_thread_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &openmp_threads_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &udf_threads_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

							//system_fprintf(0, stdout, "(%ld, %ld) Completed\t%.2f secs,\tmain_thread_cpu_time %.2f, total_cpu_time %.2f, computation_worker_cpu_time %.2f\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, timer_.get_timer(0), main_thread_cpu_time, cpu_time, openmp_threads_cpu_time + udf_threads_cpu_time);
							system_fprintf(0, stdout, "(%ld, %ld) Completed\t%.2f secs,\tmain_thread_cpu_time %.2f, total_cpu_time %.2f, computation_worker_cpu_time %.2f\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, debug_timer_.get_timer(0), main_thread_cpu_time, cpu_time, openmp_threads_cpu_time + udf_threads_cpu_time);

							for(auto& vec : vectors_) {
								vec->PrintTimers();
							}
						}


						void MeasureCpuTimes(double main_thread_cpu_time, double cpu_time, double openmp_threads_cpu_time, double udf_threads_cpu_time) {
#ifdef PERFORMANCE
							double t = timer_.get_timer_without_stop(0);
							json_time_per_ss_vector.push_back(t - prev_time);
							system_fprintf(0, stdout, "==================================\n");
							system_fprintf(0, stdout, "(%ld, %ld) took %.3f secs\n", UserArguments::UPDATE_VERSION, iteration, t - prev_time);
							system_fprintf(0, stdout, "==================================\n");
							prev_time = t;
							prev_cpu_time = cpu_time;
							prev_main_thread_cpu_time = main_thread_cpu_time;
							prev_openmp_threads_time = openmp_threads_cpu_time;
							prev_udf_cpu_time = udf_threads_cpu_time;
							for(auto& vec : vectors_) {
								vec->PrintTimers();
							}
							return;
#endif
							//RequestRespond::TurboAllReduceSync<double, PLUS>((char*)&main_thread_cpu_time, 1, sizeof(double));
							//RequestRespond::TurboAllReduceSync<double, PLUS>((char*)&cpu_time, 1, sizeof(double));
							//RequestRespond::TurboAllReduceSync<double, PLUS>((char*)&openmp_threads_cpu_time, 1, sizeof(double));
							//RequestRespond::TurboAllReduceSync<double, PLUS>((char*)&udf_threads_cpu_time, 1, sizeof(double));
							MPI_Allreduce(MPI_IN_PLACE, &main_thread_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &openmp_threads_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							MPI_Allreduce(MPI_IN_PLACE, &udf_threads_cpu_time, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
							json_time_per_ss_vector.push_back(timer_.get_timer_without_stop(0) - prev_time);
							system_fprintf(0, stdout, "%d-th iteration took %.3f secs, main_thread_cpu_time %.2f total_cpu_time %.2f, computation_worker_cpu_time %.2f\n", iteration, timer_.get_timer_without_stop(0) - prev_time, main_thread_cpu_time - prev_main_thread_cpu_time, cpu_time, (openmp_threads_cpu_time - prev_openmp_threads_time) + (udf_threads_cpu_time - prev_udf_cpu_time));

							prev_main_thread_cpu_time = main_thread_cpu_time;
							prev_time = timer_.get_timer_without_stop(0);
							prev_cpu_time = cpu_time;
							prev_openmp_threads_time = openmp_threads_cpu_time;
							prev_udf_cpu_time = udf_threads_cpu_time;
							for(auto& vec : vectors_) {
								vec->PrintTimers();
							}
						}

							protected:
						int num_seq_vector_partitions;
						int buffer_pool_size_in_mb;
						int num_threads;
						int MaxIters;
						int argc_;
						char** argv_ = NULL;
						TurboDB *pTurboDB_[2] = { NULL };
						RunConfig config_;

						int nio_threads;

						bool finished_;
						std::vector<TG_DistributedVectorBase*> vectors_;

						std::thread* rr_receiver_;

							public:
						Range<int64_t> current_src_edge_partitions;
						Range<int64_t> current_dst_edge_partitions;

						Range<node_t> src_edge_partition_vids;
						Range<node_t> dst_edge_partition_vids;


						int src_begin_machine_id;
						int64_t src_begin_chunk_id;
						int src_end_machine_id;
						int64_t src_end_chunk_id;
						int dst_begin_machine_id;
						int64_t dst_begin_chunk_id;
						int dst_end_machine_id;
						int64_t dst_end_chunk_id;

						// CPU Stats
						double prev_main_thread_cpu_time;
						double prev_time;
						double prev_cpu_time;
						double prev_openmp_threads_time;
						double prev_udf_cpu_time;
						timespec main_thread_cpu_time_begin;
						timespec* openmp_threads_cpu_time_begin;

						// timer
						turbo_timer timer_;
						turbo_timer perf_timer_;
						cpu_timer cpu_timer_;
						debug_timer debug_timer_;

						std::string working_dir;

						// json
						std::vector<json11::Json> json_time_per_ss_vector;
						JsonLogger* json_logger = new JsonLogger();
						JsonLogger* local_json_logger = new JsonLogger();

							public:

						int GetMaxLevel() {
							return UserArguments::MAX_LEVEL;
						}
						int GetCurrentLastLevel() {
							return UserArguments::CURRENT_LEVEL;
						}

						void InitNWSMBase (TG_NWSM_Base* nwsm) {
							nwsm_ = nwsm;
						}



						// OPTIMIZATION flags
						bool step1_backward_traversal_opt;

						bool step1_backward_traversal_done; 

						bool IsReapplyRequired(node_t src_vid) {
							for (auto& vec : vectors_) {
								if (vec->IsReapplyRequired(src_vid))
									return true;
							}
							return false;
						}

						void PruneReaggregationRequired() {
							for (auto& vec : vectors_) {
								vec->PruneReaggregationRequired();
							}
						}
						void PruneReaggregationRequired(node_t src_vid) {
							for (auto& vec : vectors_) {
								vec->PruneReaggregationRequired(src_vid);
							}
						}
						bool IsReaggregationRequired(node_t src_vid) {
							for (auto& vec : vectors_) {
								if (vec->IsReaggregationRequired(src_vid))
									return true;
							}
							return false;
						}

						virtual void Apply(node_t src_vid) {}
						virtual void SetIncScatterFunction(int i) {
							switch(i) {
								case 1:
									nwsm_->SetIncScatterFunction(&TurbographImplementation::BW_Scatter_OutEdge, 1);
									break;
								case 2:
									nwsm_->SetIncScatterFunction(&TurbographImplementation::BW_Scatter_InEdge, 1);
									break;
								case 3:
									nwsm_->SetIncScatterFunction(&TurbographImplementation::BfsScatter, 1);
									break;
								case 4:
									nwsm_->SetIncScatterFunction(&TurbographImplementation::BfsScatterBackward, 1);
									break;
								case 5:
									nwsm_->SetIncScatterFunction(&TurbographImplementation::BW_Scatter_InEdge2, 1);
									break;
							}
						}

						bool RunStaticOrIncremental(int u_ver) {
							bool run_static = false;
							if (u_ver == 0) return true;
							int64_t PrevNumIvVersions = 0;
							for (auto& vec : vectors_) {
								int n = vec->GetNumIvSuperstepVersions(u_ver - 1);
								PrevNumIvVersions = n > PrevNumIvVersions ? n : PrevNumIvVersions;
							}
							if (PrevNumIvVersions <= iteration + 1) run_static = true;
							system_fprintf(0, stdout, "RunStaticOrNot\t%ld\t%ld\t%ld\t%ld\n", u_ver, PrevNumIvVersions, iteration, (run_static) ? 1L:0L);
							return run_static;
						}

						// (1) Backward traversal
						// Find starting vertices, V_s, whose txns enumerate walks 
						// that the updated edges are part of
						void Step1_BackwardTraversal() {
							nwsm_->ClearDqNodes();
							IteratorMode_t prev_iterator_mode = UserArguments::ITERATOR_MODE;
							bool prev_use_degree_order = UserArguments::USE_DEGREE_ORDER_REPRESENTATION;
							int64_t prev_max_level = UserArguments::MAX_LEVEL;
							bool prev_use_fullist_db = UserArguments::USE_FULLIST_DB;
							UserArguments::ITERATOR_MODE = PARTIAL_LIST;
							UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
							UserArguments::MAX_LEVEL = 1;
							UserArguments::OV_FLUSH = false;
							UserArguments::USE_FULLIST_DB = false;
							turbo_timer step1_timer;

							std::vector<TG_DistributedVectorBase*> tmp_vector_container;
							for(auto& vec : vectors_) {
								tmp_vector_container.push_back(vec);
							}
							vectors_.clear();
							register_vector(StartingVertices);

							EdgeType edge_type = GetEdgeTypeForStep1();

							step1_timer.start_timer(0);
							if (UserArguments::UPDATE_VERSION > 1) {
								if (false) {
									for (int lv = 0; lv <= prev_max_level; lv++) {
										auto de_iter = de_vid_map[lv][ALL].entries_list_.begin();
										for (; de_iter != de_vid_map[lv][ALL].entries_list_.end(); de_iter++) {
											de_vid_map[lv][INSERT].Clear(*de_iter);
											de_vid_map[lv][DELETE].Clear(*de_iter);
										}
										de_vid_map[lv][INSERT].ClearAllLv2();
										de_vid_map[lv][DELETE].ClearAllLv2();
										de_vid_map[lv][ALL].ClearAll();
									}
								} else {
									for (int lv = 0; lv <= prev_max_level; lv++) {
										de_vid_map[lv][INSERT].ClearAll();
										de_vid_map[lv][DELETE].ClearAll();
										de_vid_map[lv][ALL].ClearAll();
									}
								}
							}
							step1_timer.stop_timer(0);

							step1_timer.start_timer(5);
							if (prev_iterator_mode == FULL_LIST) {
								UserArguments::RUN_PRUNING_SUBQUERIES = true;
							}

							for (int lv = 0; lv < prev_max_level; lv++) {
								step1_timer.start_timer(6);
								UserArguments::PRUNING_BFS_LV = lv;
								step1_timer.stop_timer(6);

								step1_timer.start_timer(7);
								if (lv == 0) {
									nwsm_->MarkAllAtVOI(1);
									if (UserArguments::ITERATOR_MODE != FULL_LIST)
										SetIncScatterFunction(5);
								} else {
									if (UserArguments::ITERATOR_MODE != FULL_LIST)
										SetIncScatterFunction(2);
								}
								step1_timer.stop_timer(7);

								step1_timer.start_timer(1);
								nwsm_->SkipApplyPhase(true);
								if (lv == 0) {
									nwsm_->Inc_Start(delta_ver, edge_type, ALL, true, true, false);
								} else {
									nwsm_->Inc_Start(main_ver, edge_type, INSERT, true, true, false);
									//nwsm_->Inc_Start(main_ver, edge_type, db_type, true, true, false);
								}
								step1_timer.stop_timer(1);

								step1_timer.start_timer(2);
								ProcessVertices(&StartingVertices, [&](node_t vid) {
										node_t idx = vid - PartitionStatistics::my_first_internal_vid();
										if (StartingVertices.AccumulatedUpdate(vid)) {
											de_vid_map[lv+1][ALL].Set_Atomic(idx);
											nwsm_->MarkAtVOI(1, vid, true);
										}
										StartingVertices.ResetWriteBuffer(vid);
									}, false);
								step1_timer.stop_timer(2);
							}

							step1_timer.stop_timer(5);
							step1_timer.start_timer(3);
							step1_timer.start_timer(4);
							for (int lv = 0; lv < prev_max_level; lv++) {
								if (lv == 0 && prev_iterator_mode == PARTIAL_LIST) continue;
								nwsm_->AllreduceDQNodes(lv);
							}
							step1_timer.stop_timer(4);
							if (prev_iterator_mode == FULL_LIST) {
								UserArguments::RUN_PRUNING_SUBQUERIES = false;
							}
							ReportNWSMTime(0);    

							vectors_.clear();
							for(auto& vec : tmp_vector_container) {
								vectors_.push_back(vec);
							}

							/*for (int lv = 0; lv <= prev_max_level; lv++) {
								TwoLevelBitMap<int64_t>::Union(de_vid_map[lv][INSERT], de_vid_map[lv][DELETE], de_vid_map[lv][ALL]);
							}*/

							GeneratePruningRules();
							nwsm_->ClearAllAtVOI(1);
							if (prev_use_fullist_db) {
								TurboDB::GetBufMgr()->ClearAllFrames(); // XXX
							}
							step1_timer.stop_timer(3);

							//fprintf(stdout, "[%ld] (%ld, %ld) Step1_BackwardTraversal |de_vid_map[0]| = %ld, |de_vid_map[1]| = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, de_vid_map[0].GetTotal(), de_vid_map[1].GetTotal());

							UserArguments::ITERATOR_MODE = prev_iterator_mode;
							UserArguments::USE_DEGREE_ORDER_REPRESENTATION = prev_use_degree_order;
							UserArguments::MAX_LEVEL = prev_max_level;
							UserArguments::OV_FLUSH = true;
							UserArguments::USE_FULLIST_DB = prev_use_fullist_db;

							time_breaker[2].push_back(step1_timer.get_timer(0));
							time_breaker[3].push_back(step1_timer.get_timer(1));
							time_breaker[4].push_back(step1_timer.get_timer(2));
							time_breaker[5].push_back(step1_timer.get_timer(3));
							//#ifdef PRINT_PROFILING_TIMERS
							fprintf(stdout, "Step1 %.4f %.4f %.4f %.4f %.4f %.4f %.4f %.4f\n", step1_timer.get_timer(0), step1_timer.get_timer(1), step1_timer.get_timer(2), step1_timer.get_timer(3), step1_timer.get_timer(4), step1_timer.get_timer(5), step1_timer.get_timer(6), step1_timer.get_timer(7));
							//#endif
						}

						void Step1_Optimized(bool check_walk_update_every_superstep) {
							UserArguments::INC_STEP = 1;
							if (!step1_backward_traversal_done) {
								Step1_BackwardTraversal();
								step1_backward_traversal_done = !check_walk_update_every_superstep;
							} else {
								time_breaker[2].push_back(0.0);
								time_breaker[3].push_back(0.0);
								time_breaker[4].push_back(0.0);
								time_breaker[5].push_back(0.0);
								ReportNWSMTime(0);
							}
						}

						void Step2_OptimizationToAvoidReaggregation_FullList() {
							if (!CheckIfStep2Required()) return;
							turbo_timer step2_timer;

							UserArguments::OV_FLUSH = false;
							UserArguments::RUN_SUBQUERIES = true;
							UserArguments::INC_STEP = 2;
							EdgeType edge_type = GetEdgeTypeForStep2();
							DynamicDBType db_type = ALL;

							nwsm_->SkipApplyPhase(false);
							SetIncScatterFunction(5);
							unregister_vector(StartingVertices);

							// 1
							step2_timer.start_timer(0);
							prop_changed_vid_map.ClearAll();
							FindPropertyChangedVertices();
							step2_timer.stop_timer(0);

							step2_timer.start_timer(1);
							step2_timer.stop_timer(1);

							/* 
							 * genOld_vid_map = (~Active AND ActivePrev) OR (Active AND ActivePrev AND PropChanged)
							 * genNew_vid_map = (Active AND ~ActivePrev) OR (Active AND ActivePrev AND PropChanged)
							 * nwsm_->GetActiveVerticesBitMap() = genOld_vid_map U genNew_vid_map
							 */

							step2_timer.start_timer(2);
							genOld_vid_map.ClearAll();
							genNew_vid_map.ClearAll();
							Vdv_vid_map.ClearAll();
							TwoLevelBitMap<node_t>::Intersection(de_vid_map[1][ALL], ActiveVertices.GetBitMap(), nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT), false, false);
							TwoLevelBitMap<node_t>::Intersection(de_vid_map[1][ALL], ActiveVertices.GetBitMap(), nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE), false, false);
							//TwoLevelBitMap<node_t>::Intersection(de_vid_map[1][INSERT], ActiveVertices.GetBitMap(), nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT), false, false);
							//TwoLevelBitMap<node_t>::Intersection(de_vid_map[1][DELETE], ActiveVertices.GetBitMap(), nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE), false, false);

							TwoLevelBitMap<int64_t>::ComplexOperation1(ActiveVertices.GetBitMap(), ActiveVerticesPrev.GetBitMap(), prop_changed_vid_map, genNew_vid_map, genOld_vid_map, nwsm_->GetActiveVerticesBitMap());
							TwoLevelBitMap<int64_t>::Union(genOld_vid_map, genNew_vid_map, Vdv_vid_map);
//#ifdef OPTIMIZE_WINDOW_SHARING
							nwsm_->MarkStartingVertices(ActiveVertices.GetBitMap(), ActiveVerticesPrev.GetBitMap(), Vdv_vid_map, nwsm_->GetActiveVerticesBitMap(), de_vid_map);
//#endif
							//fprintf(stdout, "Active total %ld, nwsm Active total %ld\n", ActiveVertices.GetBitMap().GetTotal(), nwsm_->GetActiveVerticesBitMap().GetTotal());

							//TwoLevelBitMap<int64_t>::UnionToRight(nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT), nwsm_->GetActiveVerticesBitMap());
							//TwoLevelBitMap<int64_t>::UnionToRight(nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE), nwsm_->GetActiveVerticesBitMap());

							//fprintf(stdout, "%ld %ld %ld Step2 |Active| = %ld, |gewNew| = %ld, |genOld| = %ld, |de_vid_map[0]| = %ld, |de_vid_map[1]| = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, nwsm_->GetActiveVerticesBitMap().GetTotal(), genNew_vid_map.GetTotal(), genOld_vid_map.GetTotal(), de_vid_map[0].GetTotal(), de_vid_map[1].GetTotal());
							LogNumberOfActiveVerticesPerStep();
							step2_timer.stop_timer(2);

							step2_timer.start_timer(3);
							UserArguments::USE_DELTA_NWSM = true;
							nwsm_->Inc_Start(main_ver, edge_type, db_type, true, true, false, [&](node_t vid) {
									PruneReaggregationRequired(vid);
									});
							UserArguments::USE_DELTA_NWSM = false;
							step2_timer.stop_timer(3);

							// affected --> need re-apply
							step2_timer.start_timer(4);
							affected_vid_map.CopyFrom(prop_changed_vid_map);
							bool has_updatable_ov = false;
							skip_step3and4 = 1;
							for (auto& vec : vectors_) {
								has_updatable_ov = has_updatable_ov | vec->IsUpdatable();
							}
							if (has_updatable_ov) {
								TG_DistributedVectorBase::GetGgbReaggregationFlags()->InvokeIfMarked([&](node_t idx) {
										node_t vid = idx + PartitionStatistics::my_first_internal_vid();
										nwsm_->MarkAtVOIUnsafe(1, vid);
										InitializeOV(vid);
										affected_vid_map.Set(idx);
										skip_step3and4 = 0;
										});
							}
							MPI_Allreduce(MPI_IN_PLACE, &skip_step3and4, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
							LogVersionedVerticesStates();
							UserArguments::OV_FLUSH = true;
							UserArguments::RUN_SUBQUERIES = false;

							step2_timer.stop_timer(4);

							register_vector(StartingVertices);
						}

						// For partial list
						void Step2_OptimizationToAvoidReaggregation() {
							if (!CheckIfStep2Required()) return;
							turbo_timer step2_timer;
							turbo_timer step2_aux_timer;

							Range<int> target_ver = main_ver;
							UserArguments::OV_FLUSH = false;
							UserArguments::INC_STEP = 2;
							UserArguments::RUN_SUBQUERIES = true;
							EdgeType edge_type = GetEdgeTypeForStep2();
							DynamicDBType db_type = ALL;

							nwsm_->SkipApplyPhase(false);
							SetIncScatterFunction(5);
							unregister_vector(StartingVertices);

							step2_timer.start_timer(0);

							prop_changed_vid_map.ClearAll();
							genOld_vid_map.ClearAll();
							genNew_vid_map.ClearAll();

#ifdef OPTIMIZE_SPARSE_COMPUTATION
							//if (ActiveVertices.GetBitMap().GetTotal() <= 10) {
                            if (false) {
								sparse_mode = true;
								prop_changed_vid_map.ConvertToList(true);
							} else {
								sparse_mode = false;
							}
#endif

							FindPropertyChangedVertices();
							if (sparse_mode) {
								num_prop_changed_vertices = prop_changed_vid_map.GetTotal();
							}

							step2_timer.stop_timer(0);

							step2_timer.start_timer(1);
							step2_aux_timer.start_timer(0);
#ifdef OPTIMIZE_SPARSE_COMPUTATION
							if (UserArguments::SUPERSTEP == 0) {
								for (int lv = 0; lv <= UserArguments::MAX_LEVEL; lv++) {
									de_vid_map[lv][ALL].ConvertToList(false, false);
								}
							} else {
								for (int lv = 0; lv <= UserArguments::MAX_LEVEL; lv++) {
									de_vid_map[lv][ALL].ConvertToList(false, true);
								}
							}
#endif
							if (sparse_mode) {
								step2_aux_timer.start_timer(1);
								ActiveVertices.ConvertToList(ActiveVerticesPrev);
								affected_vid_map.ConvertToList(true);
								genNew_vid_map.ConvertToList(true);
								genOld_vid_map.ConvertToList(true);
								Vdv_vid_map.ConvertToList(true);
								step2_aux_timer.stop_timer(1);
							} else {
#ifdef OPTIMIZE_SPARSE_COMPUTATION
								for (int lv = 0; lv <= UserArguments::MAX_LEVEL; lv++) {
									de_vid_map[lv][ALL].RevertList(true, true, false, true);
								}
#endif
							}
							affected_vid_map.CopyFrom(prop_changed_vid_map);
							step2_aux_timer.stop_timer(0);
							step2_timer.stop_timer(1);
							ReportNWSMTime(1);

							/* 
							 * genOld_vid_map = (~Active AND ActivePrev) OR (Active AND ActivePrev AND PropChanged)
							 * genNew_vid_map = (Active AND ~ActivePrev) OR (Active AND ActivePrev AND PropChanged)
							 * nwsm_->GetActiveVerticesBitMap() = genOld_vid_map U genNew_vid_map
							 */
							step2_timer.start_timer(2);
							step2_aux_timer.start_timer(2);
							TwoLevelBitMap<int64_t>::ComplexOperation1(ActiveVertices.GetBitMap(), ActiveVerticesPrev.GetBitMap(), prop_changed_vid_map, genNew_vid_map, genOld_vid_map, nwsm_->GetActiveVerticesBitMap(), sparse_mode); //XXX
							TwoLevelBitMap<int64_t>::Union(genOld_vid_map, genNew_vid_map, Vdv_vid_map);
							step2_aux_timer.stop_timer(2);
							step2_aux_timer.start_timer(4);
//#ifdef OPTIMIZE_WINDOW_SHARING
							nwsm_->MarkStartingVerticesPartialListMode(ActiveVertices.GetBitMap(), ActiveVerticesPrev.GetBitMap(), Vdv_vid_map, nwsm_->GetActiveVerticesBitMap(), nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT), nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE), de_vid_map);
//#endif
							step2_aux_timer.stop_timer(4);
							step2_aux_timer.start_timer(3);

							// XXX tslee - assumption: for partial_list, old_ver only accessed when Vdv_vid_map is not empty
							if (Vdv_vid_map.GetTotal() == 0) {
								target_ver = delta_ver;
							}

							if (sparse_mode) {
								affected_vid_map.RevertList(false, true, false, true);
								prop_changed_vid_map.RevertList(true, true, false, true);
								Vdv_vid_map.RevertList(true);
								genNew_vid_map.RevertList(false, true, false, true);
								genOld_vid_map.RevertList(false, true, false, true);
								for (int lv = 0; lv <= UserArguments::MAX_LEVEL; lv++) {
									de_vid_map[lv][ALL].RevertList(true, true, false, true);
								}
							}
							step2_aux_timer.stop_timer(3);


							// Caching Simulation
							//nwsm_->SimulateCaching(nwsm_->GetActiveVerticesBitMap(), main_ver, edge_type, db_type);

							LogNumberOfActiveVerticesPerStep();
							step2_timer.stop_timer(2);

							/*
								 ProcessVertices([&](node_t vid) {
								 fprintf(stdout, "(%ld, %ld) Step2. src_vid: %ld, genOld: %ld, genNew: %ld, ActivePrev: %ld, Active: %ld, Active_NWSM: %ld, Active_DeltaNWSMInsert: %ld, Active_DeltaNWSMDelete: %ld, de_Insert: %ld, de_Delete: %ld, prop_changed: %ld\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, (int64_t) vid, (int64_t) genOld_vid_map.Get(vid), (int64_t) genNew_vid_map.Get(vid), (int64_t) ActiveVerticesPrev.Read(vid), (int64_t) ActiveVertices.Read(vid), (int64_t) nwsm_->GetActiveVerticesBitMap().Get(vid), (int64_t) nwsm_->GetDeltaNwsmActiveVerticesBitMap(INSERT).Get(vid), (int64_t) nwsm_->GetDeltaNwsmActiveVerticesBitMap(DELETE).Get(vid), (int64_t) de_vid_map[INSERT].Get(vid), (int64_t) de_vid_map[DELETE].Get(vid), (int64_t) prop_changed_vid_map.Get(vid));
								 }, false);
								 */
							//fprintf(stdout, "%ld %ld %ld Step2 |Active| = %ld, |gewNew| = %ld, |genOld| = %ld, |de_vid_map[0]| = %ld, |de_vid_map[1]| = %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, nwsm_->GetActiveVerticesBitMap().GetTotal(), genNew_vid_map.GetTotal(), genOld_vid_map.GetTotal(), de_vid_map[0].GetTotal(), de_vid_map[1].GetTotal());

							step2_timer.start_timer(3);
							UserArguments::USE_DELTA_NWSM = true;
							step2_processed = nwsm_->Inc_Start(target_ver, edge_type, db_type, true, true, false, [&](node_t vid) {
									PruneReaggregationRequired(vid);
									});
							UserArguments::USE_DELTA_NWSM = false;

							step2_aux_timer.start_timer(5);
							if (sparse_mode) {
								ActiveVertices.RevertList(ActiveVerticesPrev, step2_processed);
							}
							step2_aux_timer.stop_timer(5);
							step2_timer.stop_timer(3);

							// affected --> need re-apply
							step2_timer.start_timer(4);
							bool has_updatable_ov = false;
							skip_step3and4 = 1;
							for (auto& vec : vectors_) {
								has_updatable_ov = has_updatable_ov | vec->IsUpdatable();
							}
							if (has_updatable_ov) {
								TG_DistributedVectorBase::GetGgbReaggregationFlags()->InvokeIfMarked([&](node_t idx) {
										node_t vid = idx + PartitionStatistics::my_first_internal_vid();
										nwsm_->MarkAtVOIUnsafe(1, vid);
										InitializeOV(vid);
										affected_vid_map.Set(idx);
										skip_step3and4 = 0;
										});
							}
							step2_timer.start_timer(5);
							MPI_Allreduce(MPI_IN_PLACE, &skip_step3and4, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
							step2_timer.stop_timer(5);
							if (skip_step3and4 == 0) affected_vid_map.ClearList();
							//fprintf(stdout, "[%ld] skip_step3and4 = %ld, affected_vid_map total = %ld\n", PartitionStatistics::my_machine_id(), (int64_t) skip_step3and4, affected_vid_map.GetTotal());
							LogVersionedVerticesStates();
							UserArguments::OV_FLUSH = true;
							UserArguments::RUN_SUBQUERIES = false;


							step2_timer.stop_timer(4);

							time_breaker[6].push_back(step2_timer.get_timer(0));
							time_breaker[7].push_back(step2_timer.get_timer(1));
							time_breaker[8].push_back(step2_timer.get_timer(2));
							time_breaker[9].push_back(step2_timer.get_timer(3));
							time_breaker[10].push_back(step2_timer.get_timer(4));
							ReportNWSMTime(2);

							//#ifdef PRINT_PROFILING_TIMERS
							fprintf(stdout, "[%ld] (%ld, %ld) Step2 %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, step2_timer.get_timer(0), step2_timer.get_timer(1), step2_timer.get_timer(2), step2_timer.get_timer(3), step2_timer.get_timer(4), step2_timer.get_timer(5));
							fprintf(stdout, "[%ld] (%ld, %ld) Step2_aux %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, step2_aux_timer.get_timer(0), step2_aux_timer.get_timer(1), step2_aux_timer.get_timer(2), step2_aux_timer.get_timer(3), step2_aux_timer.get_timer(4), step2_aux_timer.get_timer(5));
							//#endif

							register_vector(StartingVertices);
						}

						void Step3() {
							if (!CheckIfStep2Required()) return;
							if (skip_step3and4 == 1) {
								time_breaker[11].push_back(0.0f);
								return;
							}

							// Turn on/off pull optimization by enabling/disabling USE_PULL flag
#ifdef TEMP_SINGLE_MACHINE
							UserArguments::USE_PULL = false; 
#else
							UserArguments::USE_PULL = true; 
#endif

							UserArguments::OV_FLUSH = false;
							UserArguments::INC_STEP = 3;
							EdgeType edge_type = GetEdgeTypeForStep1();
							turbo_timer step3_timer;

							step3_timer.start_timer(0);
							SetIncScatterFunction(2);
							nwsm_->SkipApplyPhase(false);
							LogNumberOfActiveVerticesPerStep();
							step3_timer.start_timer(1);
							if (UserArguments::USE_PULL)
								pull_vid_map.CopyFrom(nwsm_->GetActiveVerticesBitMap());    // PULL
							step3_timer.stop_timer(1);

							bool prev_use_pull = UserArguments::USE_PULL;
							UserArguments::USE_PULL = false;

							nwsm_->Inc_Start(main_ver, edge_type, INSERT, true, true, false, [&](node_t vid) {
									node_t idx = vid - PartitionStatistics::my_first_internal_vid();
									// V_scatter = (V_starting_vertices) ^ V_active
									if (StartingVertices.AccumulatedUpdate(vid) && ActiveVertices.Read(vid)) { // In-Edge
									nwsm_->MarkAtVOI(1, vid);
									}
									StartingVertices.ResetWriteBuffer(vid);
									});

							UserArguments::USE_PULL = prev_use_pull;
							step3_timer.start_timer(2);
							if (UserArguments::USE_PULL)
								pull_vid_map.SwapBitMap(nwsm_->GetActiveVerticesBitMap());    // PULL
							step3_timer.stop_timer(2);

							step3_timer.stop_timer(0);

							time_breaker[11].push_back(step3_timer.get_timer(0));
							ReportNWSMTime(3);

							//#ifdef PRINT_PROFILING_TIMERS
							//system_fprintf(0, stdout, "(%ld, %ld) Step3 %.3f\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, step3_timer.get_timer(0));
							fprintf(stdout, "[%ld] (%ld, %ld) Step3 %.4f (%.4f + %.4f)\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, step3_timer.get_timer(0), step3_timer.get_timer(1), step3_timer.get_timer(2));
							//#endif
							UserArguments::OV_FLUSH = true;
						}

						void Step4() {
							UserArguments::INC_STEP = 4;
							EdgeType edge_type = GetEdgeTypeForStep2();
							nwsm_->SkipApplyPhase(false);
							turbo_timer step4_timer;

							unregister_vector(StartingVertices);
							step4_timer.start_timer(0);
							reapply_vid_map.ClearAll();

							std::list<TG_DistributedVectorBase*> list_of_vectors;
							if (skip_step3and4 == 0) {
								for(auto& vec : vectors_) {
									if (vec->CheckTargetForPull()) {
										list_of_vectors.push_back(vec);
									}
								}

								bool use_om_ggb_as_message_buffer = false;
								if (UserArguments::USE_PULL) {
									edge_type = GetEdgeTypeForStep1();
									pull_vid_map.InvokeIfMarked([&](node_t idx) {
											node_t vid = idx + PartitionStatistics::my_first_node_id();
											GenerateMessage(vid, list_of_vectors);
											}, use_om_ggb_as_message_buffer);
								}
							}
							step4_timer.stop_timer(0);

#ifdef INCREMENTAL_LOGGING
							ProcessVerticesWithoutVectors([&](node_t vid) {
									if (INCREMENTAL_DEBUGGING_TARGET(vid)) {
									node_t idx = vid - PartitionStatistics::my_first_internal_vid();
									fprintf(stdout, "X Step4. vid: %ld, affected: %ld, reapply: %ld, reagg: %ld, reapply: %ld\n", (int64_t) vid, (int64_t) affected_vid_map.Get(idx), (int64_t) reapply_vid_map.Get(idx), (int64_t) IsReaggregationRequired(vid), (int64_t) IsReapplyRequired(vid));
									}
									});
#endif

							step4_timer.start_timer(1);
							LogNumberOfActiveVerticesPerStep();
                            //fprintf(stdout, "Step4. step2_processed = %s, skip_step3_and4 = %d, affected total = %ld, ggb_reapply total = %ld\n", step2_processed ? "true" : "false", skip_step3and4, affected_vid_map.GetTotal(), TG_DistributedVectorBase::ggb_reaggregation_flags.GetTotal());
							nwsm_->Inc_Start(main_ver, edge_type, INSERT, false, true, true,
									[&](node_t vid) {
									// INC_APPLY
									node_t idx = vid - PartitionStatistics::my_first_internal_vid();
#ifdef INCREMENTAL_LOGGING
									//if (UserArguments::UPDATE_VERSION == 9 || UserArguments::UPDATE_VERSION == 11) {
									if (true && (INCREMENTAL_DEBUGGING_TARGET(vid))) {
									fprintf(stdout, "A Step4. vid: %ld, affected: %ld, reapply: %ld, reagg: %ld, reapply: %ld\n", (int64_t) vid, (int64_t) affected_vid_map.Get(idx), (int64_t) reapply_vid_map.Get(idx), (int64_t) IsReaggregationRequired(vid), (int64_t) IsReapplyRequired(vid));
									}
#endif
									if (affected_vid_map.Get(idx)) {
									Apply(vid);
#ifndef GB_VERSIONED_ARRAY
									InitializeOV(vid);
#endif
									//ActiveVertices.Write(vid, nwsm_->IsActive(vid));
									reapply_vid_map.Set_Atomic(idx); 
									}
									}, [&](node_t vid) {
									// CALLBACK_AFTER_APLYING (Not used...)
									LOG_ASSERT(false);
									}, [&](node_t vid) {
									// IV_DELTA_WRITE_IF (Not used...)
										LOG_ASSERT(false);
										return false;
									}, [&]() {
										// CallBeforFlushIv
										step4_timer.start_timer(2);

										if (!step2_processed) {
											INVARIANT(skip_step3and4 == 1);
											step4_timer.start_timer(6);
											// if !step2_processed, then
											// TG_DistributedVectorBase::ggb_reapply_flags is empty
											// reapply_vid_map <- affected_vid_map
											// affected_vid_map <- empty
											if (num_prop_changed_vertices != 0) { // affected_vid_map = list form w/o any update
												if (false) { //XXX why is this incorrect?? fuck..
													affected_vid_map.InvokeIfMarkedOnList([&](node_t idx) {
															node_t vid = idx + PartitionStatistics::my_first_internal_vid();
															Apply(vid);
#ifndef GB_VERSIONED_ARRAY
															InitializeOV(vid);
#endif
															reapply_vid_map.Set(idx);
															if (nwsm_->IsActive(vid)) ActiveVertices.GetBitMap().PushIntoList(idx);
															//ActiveVertices.Write(vid, nwsm_->IsActive(vid));
															//ActiveVertices.WriteUnsafe(vid, nwsm_->IsActive(vid));
															});
												} else {
													affected_vid_map.InvokeIfMarked([&](node_t idx) {
															node_t vid = idx + PartitionStatistics::my_first_internal_vid();
															Apply(vid);
#ifndef GB_VERSIONED_ARRAY
															InitializeOV(vid);
#endif
															reapply_vid_map.Set(idx);
															//ActiveVertices.Write(vid, nwsm_->IsActive(vid));
															//ActiveVertices.WriteUnsafe(vid, nwsm_->IsActive(vid));
															}, true);
												}
											}
											step4_timer.stop_timer(6);
											if (UserArguments::USE_PULL) {
												step4_timer.start_timer(8);
												for (auto& vec: vectors_) {
													if (vec->HasOv() && !vec->IsOvCachingAggregator()) {
														TwoLevelBitMap<int64_t>* next_flag = vec->GetOvNextBitMap(true);
														TwoLevelBitMap<int64_t>* first_snapshot_flag = vec->GetOvFirstSnapshotBitMap(true);
														if (next_flag == NULL) continue;
														// Invoke if ~(affected) AND (next U first_snapshot)
#ifndef GB_VERSIONED_ARRAY
														TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
																node_t vid = idx + PartitionStatistics::my_first_internal_vid();
																InitializeOVOfVector(vec, vid);
																}, *next_flag, *first_snapshot_flag);
#endif
													}
												}
												step4_timer.stop_timer(8);
											} else {
												step4_timer.start_timer(9);
												// XXX - tslee copied this from above. is this right?????
												for (auto& vec: vectors_) {
													if (vec->HasOv() && !vec->IsOvCachingAggregator()) {
														TwoLevelBitMap<int64_t>* next_flag = vec->GetOvNextBitMap(true);
														TwoLevelBitMap<int64_t>* first_snapshot_flag = vec->GetOvFirstSnapshotBitMap(true);
														if (next_flag == NULL) continue;
														// Invoke if ~(affected) AND (next U first_snapshot)
#ifndef GB_VERSIONED_ARRAY
														TwoLevelBitMap<int64_t>::InvokeIfMarkedOnUnion([&](node_t idx) {
																node_t vid = idx + PartitionStatistics::my_first_internal_vid();
																InitializeOVOfVector(vec, vid);
																}, *next_flag, *first_snapshot_flag);
#endif
													}
												}
												step4_timer.stop_timer(9);
											}
										} else {
											step4_timer.start_timer(6);
											//fprintf(stdout, "A. affected total = %ld, ggb_reapply total = %ld, reapply total = %ld\n", affected_vid_map.GetTotal(), TG_DistributedVectorBase::ggb_reapply_flags.GetTotal(), reapply_vid_map.GetTotal());
											TwoLevelBitMap<node_t>::Union(affected_vid_map, TG_DistributedVectorBase::ggb_reapply_flags, affected_vid_map);
											TwoLevelBitMap<node_t>::IntersectionToRight(reapply_vid_map, affected_vid_map, true, false);
											TwoLevelBitMap<node_t>::Union(reapply_vid_map, affected_vid_map, reapply_vid_map);
											//fprintf(stdout, "B. affected total = %ld, ggb_reapply total = %ld, reapply total = %ld\n", affected_vid_map.GetTotal(), TG_DistributedVectorBase::ggb_reapply_flags.GetTotal(), reapply_vid_map.GetTotal());
											step4_timer.stop_timer(6);
											step4_timer.start_timer(7);
											affected_vid_map.InvokeIfMarked([&](node_t idx) {
													node_t vid = idx + PartitionStatistics::my_first_internal_vid();
													Apply(vid);
#ifndef GB_VERSIONED_ARRAY
													InitializeOV(vid);
#endif
													//ActiveVertices.Write(vid, nwsm_->IsActive(vid));
													//ActiveVertices.WriteUnsafe(vid, nwsm_->IsActive(vid));
													});
											step4_timer.stop_timer(7);
											if (UserArguments::USE_PULL) {
												step4_timer.start_timer(8);
												for (auto& vec: vectors_) {
													if (vec->HasOv() && !vec->IsOvCachingAggregator()) {
														TwoLevelBitMap<int64_t>* next_flag = vec->GetOvNextBitMap(true);
														TwoLevelBitMap<int64_t>* first_snapshot_flag = vec->GetOvFirstSnapshotBitMap(true);
														if (next_flag == NULL) continue;
														// Invoke if ~(affected) AND (next U first_snapshot)
#ifndef GB_VERSIONED_ARRAY
														TwoLevelBitMap<int64_t>::ComplexOperation5(affected_vid_map, *next_flag, *first_snapshot_flag,
																[&](node_t idx) {
																node_t vid = idx + PartitionStatistics::my_first_internal_vid();
																InitializeOVOfVector(vec, vid);
																});
#endif
													}
												}
												step4_timer.stop_timer(8);
											} else {
												step4_timer.start_timer(9);
												// XXX - tslee copied this from above. is this right?????
												for (auto& vec: vectors_) {
													if (vec->HasOv() && !vec->IsOvCachingAggregator()) {
														TwoLevelBitMap<int64_t>* next_flag = vec->GetOvNextBitMap(true);
														TwoLevelBitMap<int64_t>* first_snapshot_flag = vec->GetOvFirstSnapshotBitMap(true);
														if (next_flag == NULL) continue;
														// Invoke if ~(affected) AND (next U first_snapshot)
#ifndef GB_VERSIONED_ARRAY
														TwoLevelBitMap<int64_t>::ComplexOperation5(affected_vid_map, *next_flag, *first_snapshot_flag,
																[&](node_t idx) {
																node_t vid = idx + PartitionStatistics::my_first_internal_vid();
																InitializeOVOfVector(vec, vid);
																});
#endif
													}
												}
												step4_timer.stop_timer(9);
											}
										}
										affected_vid_map.ClearAll();

										step4_timer.stop_timer(2);

										step4_timer.start_timer(3);
										// If Apply is not re-executed, Clear the dirty flag of IV
										for(auto& vec : vectors_) {
											if (!vec->HasIv()) continue;
											TwoLevelBitMap<int64_t>::Intersection(*vec->GetIvDirtyBitMap(), reapply_vid_map, *vec->GetIvDirtyBitMap());
										}

#ifndef GB_VERSIONED_ARRAY
										if (!step2_processed && sparse_mode) {
											TwoLevelBitMap<int64_t>::InvokeIfMarkedOnIntersection([&](node_t idx) {
													node_t vid = idx + PartitionStatistics::my_first_internal_vid();
													ActiveVertices.Write(vid, ActiveVerticesPrev.Read(vid));
													if (ActiveVerticesPrev.Read(vid)) ActiveVertices.GetBitMap().PushIntoList(idx);
													//ActiveVertices.WriteUnsafe(vid, ActiveVerticesPrev.Read(vid));
													}, reapply_vid_map, *ActiveVertices.GetDirtyFlags(), true, false);
										} else {
											TwoLevelBitMap<int64_t>::InvokeIfMarkedOnIntersection([&](node_t idx) {
													node_t vid = idx + PartitionStatistics::my_first_internal_vid();
													ActiveVertices.Write(vid, ActiveVerticesPrev.Read(vid));
													//ActiveVertices.WriteUnsafe(vid, ActiveVerticesPrev.Read(vid));
													}, reapply_vid_map, *ActiveVertices.GetDirtyFlags(), true, false);
										}
#endif
										TwoLevelBitMap<int64_t>::Intersection(*ActiveVertices.GetDirtyFlags(), reapply_vid_map, *ActiveVertices.GetDirtyFlags());
										step4_timer.stop_timer(3);
									}, skip_step3and4);
									step4_timer.stop_timer(1);

									step4_timer.start_timer(4);
									/*for(auto& vec : vectors_) {
										vec->ClearAllOvDirty();
										vec->ClearAllOvChanged();
										}*/
									register_vector(StartingVertices);
									step4_timer.stop_timer(4);

									time_breaker[12].push_back(step4_timer.get_timer(1));
									time_breaker[13].push_back(step4_timer.get_timer(4));
									ReportNWSMTime(4);

									step4_timer.start_timer(5);
									if (UserArguments::USE_PULL) {
										ClearGeneratedMessages(list_of_vectors);
										UserArguments::USE_PULL = false;
									}
									step4_timer.stop_timer(5);

									//#ifdef PRINT_PROFILING_TIMERS
									fprintf(stdout, "[%ld] (%ld, %ld) Step4 %.4f %.4f %.4f %.4f %.4f %.4f %.4f %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, step4_timer.get_timer(0), step4_timer.get_timer(1), step4_timer.get_timer(2), step4_timer.get_timer(3), step4_timer.get_timer(4), step4_timer.get_timer(5), step4_timer.get_timer(6), step4_timer.get_timer(7), step4_timer.get_timer(8), step4_timer.get_timer(9));
									//#endif
									}

							bool IsIvDirty(node_t vid) {
								for(auto& vec : vectors_) {
									if (vec->IsIvDirty(vid)) return true;
								}
								return false;
							}

							void ClearInputVectorFlags(bool clear_dirty = true, bool clear_changed = true, bool clear_flush = true) {
								for(auto& vec : vectors_) {
									vec->ClearIvFlagsForIncrementalProcessing(clear_dirty, clear_changed);
								}
								if (clear_dirty) ActiveVertices.ClearAllDirty();
								if (clear_changed) ActiveVertices.ClearAllChanged();
								if (clear_flush) ActiveVertices.ClearAllFlush();
							}

							bool CheckIfStep2Required() {
								return true;

								bool has_non_distributive_ov = false;
								for(auto& vec : vectors_) {
									if (vectors1.count(vec) != 0) continue; 
									if (!vec->IsOvDistributive(true))    // XXX - Assumed insertion
										has_non_distributive_ov = true;
								}
								if (has_non_distributive_ov) return true;
								return false;
							}

							bool CheckIfStep3Required() {
								bool has_vertex_that_requires_aggregation = false;
								if (has_vertex_that_requires_aggregation) return true;
								return false;
							}

							EdgeType GetEdgeTypeForStep1() {
								//if (true) {
								if (UserArguments::IS_GRAPH_DIRECTED_GRAPH_OR_NOT) {
									// if directed graph
									switch (UserArguments::EDGE_DB_TYPE) {
										case INOUTEDGE:
											return INOUTEDGE;
										case OUTEDGE:
											return INEDGE;
										case INEDGE:
											return OUTEDGE;
										default:
											abort();
									}
								} else {
									// if undirected graph
									switch (UserArguments::EDGE_DB_TYPE) {
										case INOUTEDGE:
											return INOUTEDGE;
										case OUTEDGE:
											return OUTEDGE;
										case INEDGE:
											return INEDGE;
										default:
											abort();
									}
								}
							}

							EdgeType GetEdgeTypeForStep2() {
								switch (UserArguments::EDGE_DB_TYPE) {
									case INOUTEDGE:
										return INOUTEDGE;
									case OUTEDGE:
										return OUTEDGE;
									case INEDGE:
										return INEDGE;
									default:
										abort();
								}
							}

							void LogVersionedVerticesStates() {
#ifdef INCREMENTAL_PROFILING
								int64_t cnts[9] = {0};
								if (UserArguments::INC_STEP != -1)  {

									ProcessVertices(&ActiveVertices, &ActiveVerticesPrev, [&](node_t vid) {
											node_t idx = vid - PartitionStatistics::my_first_internal_vid();
											if (prop_changed_vid_map.Get(idx)) { cnts[0]++; }
											//if (de_vid_map[0].Get(idx)) { cnts[1]++; }
											if (de_vid_map[1][INSERT].Get(idx)) { cnts[1]++; }
											if (prop_changed_vid_map.Get(idx) && ActiveVertices.Read(vid)) { cnts[2]++; }
											//if (de_vid_map[0].Get(idx) && ActiveVertices.Read(vid)) { cnts[3]++; }
											if (de_vid_map[1][INSERT].Get(idx) && ActiveVertices.Read(vid)) { cnts[3]++; }
											if (affected_vid_map.Get(idx)) { cnts[5]++; }
											if (genOld_vid_map.Get(idx) && genNew_vid_map.Get(idx)) { cnts[6]++; }
											if (!genOld_vid_map.Get(idx) && genNew_vid_map.Get(idx)) { cnts[7]++; }
											if (genOld_vid_map.Get(idx) && !genNew_vid_map.Get(idx)) { cnts[8]++; }

											//fprintf(stdout, "vid: %ld, prop_changed: %ld, afected: %ld, genNew: %ld, genOld: %ld\n", vid, prop_changed_vid_map.Get(idx), affected_vid_map.Get(idx), genNew_vid_map.Get(idx), genOld_vid_map.Get(idx));


											}, false, false);
									for (int k = 0; k < 9; k++ ) { 
										MPI_Allreduce(MPI_IN_PLACE, &cnts[k], 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
									}
									system_fprintf(0, stdout, "Superstep: %ld, |dV_prop|: %ld\n", iteration, (int64_t) cnts[0]);
									system_fprintf(0, stdout, "Superstep: %ld, |dV_e|: %ld\n", iteration, (int64_t) cnts[1]);
									system_fprintf(0, stdout, "Superstep: %ld, |dV_prop ^ V_active|: %ld\n", iteration, (int64_t) cnts[2]);
									system_fprintf(0, stdout, "Superstep: %ld, |dV_e ^ V_active|: %ld\n", iteration, (int64_t) cnts[3]);
									system_fprintf(0, stdout, "Superstep: %ld, |dV_active|: %ld\n", iteration, (int64_t) cnts[4]);
									system_fprintf(0, stdout, "Superstep: %ld, |V_affected|: %ld\n", iteration, (int64_t) cnts[5]);
									system_fprintf(0, stdout, "Superstep: %ld, |genOld_AND_genNew|: %ld\n", iteration, (int64_t) cnts[6]);
									system_fprintf(0, stdout, "Superstep: %ld, |NotGenOld_AND_genNew|: %ld\n", iteration, (int64_t) cnts[7]);
									system_fprintf(0, stdout, "Superstep: %ld, |genOld_AND_NotGenNew|: %ld\n", iteration, (int64_t) cnts[8]);
								}
								for (int k = 0; k < 9; k++ ) { 
									versioned_vertices[k].push_back(cnts[k]);
								}
#endif
							}

							/* Auto-compiled (For OutEdge)*/
							virtual void BW_Scatter_OutEdge(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
								//fprintf(stdout, "\tBW_Scatter_OutEdge src_vid=%ld\n", src_vid);
								StartingVertices.Write(src_vid, true);
#ifdef INCREMENTAL_LOGGING
								for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
									node_t nbr_vid = GetVisibleDstVid(dst_vids[0][idx]);
									if (nbr_vid == -1) continue;

									if (true && (INCREMENTAL_DEBUGGING_TARGET(nbr_vid))) {
										std::string db_type = (UserArguments::INC_STEP == 1 || TG_ThreadContexts::run_delta_nwsm) ? (TG_ThreadContexts::ctxt->db_type == INSERT ? "INSERT" : "DELETE") : "MAIN";
										fprintf(stdout, "\t[%ld] %ld %ld [%s] BW_Scatter_OutEdge src_vid=%ld, dst_vid=%ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, db_type.c_str(), src_vid, nbr_vid);
									}
								}
#endif
								return;
							}


							/* Auto-compiled (For InEdge)*/
							virtual void BW_Scatter_InEdge(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
								//fprintf(stdout, "\tBW_Scatter_InEdge src_vid=%ld, |dst_vids|=%ld\n", src_vid, num_nbrs[0]);
								for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
									node_t nbr_vid = GetVisibleDstVid(dst_vids[0][idx]);
									if (nbr_vid == -1) continue;

#ifdef INCREMENTAL_LOGGING
									if (true && (INCREMENTAL_DEBUGGING_TARGET(nbr_vid))) {
										std::string db_type = (UserArguments::INC_STEP == 1 || TG_ThreadContexts::run_delta_nwsm) ? (TG_ThreadContexts::ctxt->db_type == INSERT ? "INSERT" : "DELETE") : "MAIN";
										fprintf(stdout, "\t[%ld] %ld %ld [%s] BW_Scatter_InEdge src_vid=%ld, dst_vid=%ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, db_type.c_str(), src_vid, nbr_vid);
									}
#endif
									StartingVertices.Update(nbr_vid, true);
								}
								return;
							}

							/* Auto-compiled (For InEdge)*/
							virtual void BW_Scatter_InEdge2(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
								de_vid_map[0][ALL].Set_Atomic(src_vid - PartitionStatistics::my_first_internal_vid());
								/*if (TG_ThreadContexts::ctxt->db_type == INSERT) {
									de_vid_map[0][0].Set_Atomic(src_vid - PartitionStatistics::my_first_internal_vid());
								} else {
									de_vid_map[0][1].Set_Atomic(src_vid - PartitionStatistics::my_first_internal_vid());
								}*/

								for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
									node_t nbr_vid = GetVisibleDstVid(dst_vids[0][idx]);
									if (nbr_vid == -1) continue;
									StartingVertices.Update(nbr_vid, true);
								}
								return;
							}

							inline void BfsScatterBackward(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
								int32_t val = BfsLevel.Read(src_vid) - 1;
								for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
									node_t dst_vid = GetVisibleDstVid(dst_vids[0][idx]);
									if (dst_vid == -1) continue;
									BfsLevel.Update(dst_vid, val);
								}
							}

							inline void BfsScatter(node_t src_vid, node_t* dst_vids[], node_t num_nbrs[]) {
								int32_t val = BfsLevel.Read(src_vid) + 1;
								for (node_t idx = 0; idx < num_nbrs[0]; idx++) {
									node_t dst_vid = GetVisibleDstVid(dst_vids[0][idx]);
									if (dst_vid == -1) continue;
									BfsLevel.Update(dst_vid, val);
								}
							}


							void ResetNumProcessedEdges() {
								for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
									per_thread_num_processed_edges_cnts[tid].idx = 0;
								}
							}

							void ReportPerStep(int64_t step, double sec) {
								ReportNumProcessedEdgesPerSec(step, sec);
								ReportNumProcessedPagesPerStep(step);
								ReportEdgesInPagesPerStep(step);
								ReportIOPerStep(step);
							}

							void ReportNumProcessedEdgesPerSec(int64_t step, double sec) {
								//#ifdef INCREMENTAL_PROFILING
								int64_t num_edges = 0;
								for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
									num_edges += per_thread_num_processed_edges_cnts[tid].idx;
									per_thread_num_processed_edges_cnts[tid].idx = 0;
								}
								//MPI_Allreduce(MPI_IN_PLACE, &num_edges, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								//system_fprintf(0, stdout, "Superstep: %ld\t %ld edges/sec (%ld edges, %.2f sec)\n", UserArguments::SUPERSTEP, (int64_t) (sec == 0) ? -1.0f : ((double)num_edges/sec), num_edges, sec);
								num_processed_edges_of_steps[step].push_back(num_edges);
								//#endif
							}

							void ReportNumProcessedPagesPerStep(int64_t step) {
								//#ifdef INCREMENTAL_PROFILING
								int64_t num_pages = 0;
								int64_t num_empty_pages = 0;
								for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
									num_pages += per_thread_num_processed_pages_cnts[tid].idx;
									per_thread_num_processed_pages_cnts[tid].idx = 0;
									num_empty_pages += per_thread_num_processed_empty_pages_cnts[tid].idx;
									per_thread_num_processed_empty_pages_cnts[tid].idx = 0;
								}
								//MPI_Allreduce(MPI_IN_PLACE, &num_pages, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								num_processed_pages_of_steps[step].push_back(num_pages);
								num_processed_empty_pages_of_steps[step].push_back(num_empty_pages);
								//#endif
							}

							void ReportEdgesInPagesPerStep(int64_t step) {
#ifdef INCREMENTAL_PROFILING
								int64_t num_edges = 0;
								for (int tid = 0; tid < UserArguments::NUM_THREADS; tid++) {
									num_edges += per_thread_num_edges_in_pages_cnts[tid].idx;
									per_thread_num_edges_in_pages_cnts[tid].idx = 0;
								}
								MPI_Allreduce(MPI_IN_PLACE, &num_edges, 1, MPI_LONG_LONG_INT, MPI_SUM, MPI_COMM_WORLD);
								num_edges_in_pages_of_steps[step].push_back(num_edges);
#endif
							}

							void ReportIOPerStep(int64_t step) {
								int64_t page_read_io = 0, page_write_io = 0, versioned_array_read_io = 0, versioned_array_write_io = 0, network_sent_io = 0, network_recv_io = 0;
								diskaio::DiskAioStats dstats = DiskAioFactory::GetPtr()->GetStats();

								// Network I/O
								int64_t accum_network_sent_bytes = turbo_tcp::GetBytesSent();
								int64_t accum_network_recv_bytes = turbo_tcp::GetBytesReceived();
								network_sent_io = accum_network_sent_bytes;
								network_recv_io = accum_network_recv_bytes;
								network_io_of_steps[step].push_back(network_sent_io + network_recv_io - prev_network_io);
								prev_network_io = network_sent_io + network_recv_io;

								// Edge Page I/O
								page_read_io = TurboDB::GetBufMgr()->GetPageReadIO();
								page_write_io = TurboDB::GetBufMgr()->GetPageWriteIO();
								page_io_of_steps[step].push_back(page_read_io + page_write_io - prev_page_io);
								prev_page_io = page_read_io + page_write_io;

								// Versioned Array I/O
								versioned_array_read_io = dstats.num_read_bytes - page_read_io;
								versioned_array_write_io = dstats.num_write_bytes - page_write_io;
								versioned_array_io_of_steps[step].push_back(versioned_array_read_io + versioned_array_write_io - prev_versioned_array_io);
								prev_versioned_array_io = versioned_array_read_io + versioned_array_write_io;
							}

							void ReportNWSMTime(int i) {
								nwsm_->ReportNWSMTime(nwsm_time_breaker[i]);
							}

							void CompletePendingAioRequests() {
								ActiveVertices.WaitForIoRequests(true, true);
								for (auto& vec : vectors_) {
									vec->ProcessPendingAioRequests();
								}
								//fprintf(stdout, "[%ld] num_entries(per_thread_buffer_queue_) = %ld\n", PartitionStatistics::my_machine_id(), RequestRespond::per_thread_buffer_queue_.size_approx());
							}

								public:
							int iteration;
							TG_NWSM_Base* nwsm_;
							int64_t num_vertices;

							PaddedIdx* per_thread_num_processed_edges_cnts;
							PaddedIdx* per_thread_cnts1;
							PaddedIdx* per_thread_cnts2;
							PaddedIdx* per_thread_num_pages_io_cnts;
							PaddedIdx* per_thread_num_processed_pages_cnts;
							PaddedIdx* per_thread_num_edges_in_pages_cnts;
							PaddedIdx* per_thread_num_processed_empty_pages_cnts;

								protected:
							Range<int> main_ver;
							Range<int> prev_main_ver;
							Range<int> delta_ver;

							int skip_step3and4;
							bool step2_processed;
							bool sparse_mode;
							bool need_OutDegree[3];
							TG_DistributedVector<int32_t, PLUS> OutDegree[3];
							TG_DistributedVector<int32_t, PLUS> OutDegreeTemp[3];
							TG_DistributedVector<int32_t, PLUS>& vec_out_degree = OutDegree[0];

#ifndef GB_VERSIONED_ARRAY
							VersionedBitMap<bool, LOR> ActiveVertices;
							VersionedBitMap<bool, LOR>& vec_active = ActiveVertices;
							VersionedBitMap<bool, LOR> ActiveVerticesPrev;
#else
							GBVersionedBitMap<bool, LOR> ActiveVertices;
							GBVersionedBitMap<bool, LOR>& vec_active = ActiveVertices;
							GBVersionedBitMap<bool, LOR> ActiveVerticesPrev;
#endif
							TG_DistributedVector<bool, LOR> ActiveVerticesTemp;
							TG_DistributedVector<bool, LOR> StartingVertices;
							TG_DistributedVector<int32_t, MIN> BfsLevel;

							std::set<TG_DistributedVectorBase*> vectors1;       
							tbb::concurrent_unordered_map<EdgePair, int> edge_map;
							tbb::concurrent_unordered_map<node_t, node_t> edge_src_map;

							TwoLevelBitMap<node_t> prop_changed_vid_map;
							TwoLevelBitMap<node_t> affected_vid_map;
							TwoLevelBitMap<node_t> reapply_vid_map;
							TwoLevelBitMap<node_t> genOld_vid_map;
							TwoLevelBitMap<node_t> genNew_vid_map;
							TwoLevelBitMap<node_t> Vdv_vid_map;
							TwoLevelBitMap<node_t>** de_vid_map;

							TwoLevelBitMap<node_t> pull_vid_map;

							// Local Logging
							std::vector<std::vector<std::vector<json11::Json>>> nwsm_time_breaker;
							std::vector<std::vector<json11::Json>> time_breaker;

							// Global Logging (need to be allreduce)
							std::vector<std::vector<int64_t>> page_io_of_steps;
							std::vector<std::vector<int64_t>> network_io_of_steps;
							std::vector<std::vector<int64_t>> versioned_array_io_of_steps;
							std::vector<std::vector<int64_t>> num_processed_edges_of_steps;
							std::vector<std::vector<int64_t>> num_processed_pages_of_steps;
							std::vector<std::vector<int64_t>> num_processed_empty_pages_of_steps;
							std::vector<std::vector<json11::Json>> page_io_of_steps_json;
							std::vector<std::vector<json11::Json>> network_io_of_steps_json;
							std::vector<std::vector<json11::Json>> versioned_array_io_of_steps_json;
							std::vector<std::vector<json11::Json>> num_processed_edges_of_steps_json;
							std::vector<std::vector<json11::Json>> num_processed_pages_of_steps_json;
							std::vector<std::vector<json11::Json>> num_processed_empty_pages_of_steps_json;
							std::vector<json11::Json> page_io_of_steps_per_machine_json;
							std::vector<json11::Json> network_io_of_steps_per_machine_json;
							std::vector<json11::Json> num_processed_edges_of_steps_per_machine_json;
							std::vector<json11::Json> num_processed_pages_of_steps_per_machine_json;

							// Global Logging (need not be allreduce / allreduce while debugging)
							std::vector<json11::Json> snapshots;
							std::vector<json11::Json> supersteps;
							std::vector<json11::Json> active_vertices;
							std::vector<json11::Json> accum_delta_size;
							std::vector<json11::Json> portion_of_active_vertices;
							std::vector<std::vector<json11::Json>> versioned_vertices;
							std::vector<std::vector<json11::Json>> execution_times_of_steps;
							std::vector<std::vector<json11::Json>> active_vertices_of_steps;
							std::vector<std::vector<json11::Json>> num_edges_in_pages_of_steps;

							int64_t accm_versionned_array_io_in_bytes[2] = {0};

							node_t num_active_vertices;
							double nwsm_time;
							double no_nwsm_time;

							int64_t prev_page_io = 0;
							int64_t prev_network_io = 0;
							int64_t prev_versioned_array_io = 0;

							int64_t num_prop_changed_vertices = 0;

							cxxopts::Options options;
							};

#endif
