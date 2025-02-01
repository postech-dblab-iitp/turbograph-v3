#pragma once

#include <vector>
#include <omp.h>
#include <atomic>
#include <fstream>

#include "analytics/datastructure/page.hpp"
#include "analytics/core/turbo_dist_internal.hpp"

template <typename T>
class TwoLevelBitMap;

template <typename T>
class BitMap;

int64_t ComputeMemorySizeOfAdjacencyLists(BitMap<node_t>& vids, Range<node_t>& vid_range);

int64_t CountNumPagesToStream(TwoLevelBitMap<node_t>& active_vertices, Range<int64_t>& dst_edge_partitions, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);
void GroupNodeListsIntoPageListsForStreaming(TwoLevelBitMap<node_t>& main_vids, TwoLevelBitMap<node_t>& delta_vids, Range<node_t>& input_vid_range, Range<int64_t>& dst_edge_partitions, TwoLevelBitMap<PageID>& pids, PageID MaxNumPagesToPin, Range<node_t>& ProcessedVertices, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);
void GroupNodeListsIntoPageLists(TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, Range<int64_t>& dst_edge_partitions, TwoLevelBitMap<PageID>& pids, PageID MaxNumPagesToPin, Range<node_t>& ProcessedVertices);

int64_t GroupNodeLists(TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, int64_t MaxBytesToPin, Range<node_t>& ProcessedVertices);
int64_t GroupNodeLists(int target_machine, TwoLevelBitMap<node_t>& vids, Range<node_t>& vid_range, int64_t MaxBytesToPin, Range<node_t>& ProcessedVertices);


class TG_AdjacencyListIterator {
  public:
    TG_AdjacencyListIterator() : mBeginPtr(NULL), mCurrentOffset(-1), mStartOffset(0), mNumEntries(-1), mSrcLid(-1) {}

    TG_AdjacencyListIterator(node_t src_lid, char* begin_ptr, node_t num_entries):mSrcLid(src_lid), mNumEntries(num_entries) {
      begin_ptr = (char*) begin_ptr;
    }

    node_t GetSrcLid() {
      return mSrcLid;
    }
    node_t GetSrcVid() {
      return mSrcVid;
    }

    node_t GetNumEntries() {
      return mNumEntries;
    }
    // Initialize cursor with certain start vertex
    ReturnStatus InitCursor(node_t start_dst_vid);

    // Move cursor to certain destination vertex
    ReturnStatus MoveCursor(node_t dst_vid);

    ReturnStatus GetNext(node_t& neighbor);

    // Initialize adj iterator
    void Init(node_t src_lid, node_t src_vid, const node_t* begin_ptr, node_t num_entries) {
      ALWAYS_ASSERT (src_lid >= 0);
      ALWAYS_ASSERT (src_vid >= 0);
      ALWAYS_ASSERT (num_entries > 0);
      mBeginPtr=(node_t *)begin_ptr;
      mSrcLid = src_lid;
      mSrcVid = src_vid;
      mNumEntries=num_entries;
      mCurrentOffset=-1;
      mStartOffset=0;
    }

    // Initialize adj iterator
    void Init(node_t src_vid, const node_t* begin_ptr, node_t num_entries) {
      ALWAYS_ASSERT (num_entries > 0);
      mBeginPtr=(node_t *)begin_ptr;
      mSrcLid = LocalStatistics::Vid2Lid(src_vid);
      mSrcVid = src_vid;
      mNumEntries=num_entries;
      mCurrentOffset=-1;
      mStartOffset=0;
    }

    node_t* GetData() {
      ALWAYS_ASSERT (mCurrentOffset == -1);
      return mBeginPtr;
    }

    // public:
  protected:
    node_t mSrcVid;
    node_t mSrcLid;
    int mCurrentOffset;
    int mStartOffset;
    node_t mNumEntries;
    node_t* mBeginPtr;
};


class TG_AdjacencyMatrixPageIterator {
  public:
    TG_AdjacencyMatrixPageIterator() : mCurrentPid(-1), mCurrentSlotIdx(-1), mNumSlots(-1), mCurrentLid(-1), mBuffer(NULL) {
    }

    ReturnStatus ReadNextPage (BitMap<PageID>& ActivePages);

    ReturnStatus GetNextAdjList (TG_AdjacencyListIterator& iter);
    ReturnStatus GetNextAdjList (BitMap<node_t>& ActiveNodes, TG_AdjacencyListIterator& iter);
    ReturnStatus GetNextAdjList2 (BitMap<node_t>& ActiveNodes, TG_AdjacencyListIterator& iter);
    ReturnStatus PrintStat (BitMap<node_t>& ActiveNodes);

    ReturnStatus SetCurrentPage (PageID pid, Page* page);
    ReturnStatus SetCurrentPage (Page* page);

    void Init(Range<PageID> page_range) {
      mCurrentPid=-1;
      mCurrentSlotIdx=-1;
      mCurrentLid=-1;
    }

  protected:
    Page* mBuffer;
    PageID mCurrentPid;
    int64_t mCurrentSlotIdx;
    int64_t mNumSlots;
    node_t mCurrentLid;
    node_t PageFirstLid;
    node_t PageLastLid;
    node_t FirstVid;
    bool is_sparse;
};

// Count the number of common neighbors of v1 and v2 with the partial order constraint (CheckPartialOrder)
int64_t CountNumberOfCommonNeighbors(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v2, node_t* v2_data, int64_t v2_sz, std::function<bool(node_t, node_t)> CheckPartialOrder);
//void FindCommonNeighbors(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v2, node_t* v2_data, int64_t v2_sz, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t)> AggregateFunc);
int64_t TriangleIntersection_Hybrid(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx, std::function<bool(node_t, node_t)> CheckPartialOrder);
int64_t TriangleIntersection(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx, std::function<bool(node_t, node_t)> CheckPartialOrder);
template <CompareType v1_ct, CompareType v2_ct>
void TriangleIntersection_HybridTemp(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);
template<>
void TriangleIntersection_HybridTemp<LT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);
template<>
void TriangleIntersection_HybridTemp<GT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);
//template <CompareType v1_ct, CompareType v2_ct>
//void TriangleIntersectionTemp(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);
int64_t TriangleIntersectionWithoutPartialOrder(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, Range<int32_t>* binary_splitting_idx);

template <CompareType v1_ct, CompareType v2_ct>
void TriangleIntersectionTemp(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template <>
void TriangleIntersectionTemp<NONE, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx); 

template <>
void TriangleIntersectionTemp<LT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);


template <>
void TriangleIntersectionTemp<NONE, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template <>
void TriangleIntersectionTemp<LT, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template<>
void TriangleIntersectionTemp<GT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template<>
void TriangleIntersectionTemp<NONE, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template<>
void TriangleIntersectionTemp<GT, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template <>
void TriangleIntersectionTemp<GT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

template <>
void TriangleIntersectionTemp<LT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);



template <CompareType v1_ct, CompareType v2_ct>
void FindCommonNeighbors(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);
 /* {
if (v1_sz == 0 || v2_sz == 0) return;

DummyUDFDataStructure ds;
Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

node_t v1_degreeorder;
node_t v2_degreeorder;
if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
v1_degreeorder = v1;
v2_degreeorder = v2;
} else {
v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
}
TriangleIntersectionTemp<v1_ct, v2_ct>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc);
}*/
template <>
void FindCommonNeighbors<NONE, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<GT, GT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<LT, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<GT, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<LT, GT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<NONE, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<LT, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);

template <>
void FindCommonNeighbors<GT, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc);



// 2-way Set Intersection with the partial order constraint
template<>
void TriangleIntersection_HybridTemp<LT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

// 2-way Set Intersection with the partial order constraint
template<>
void TriangleIntersection_HybridTemp<GT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx);

