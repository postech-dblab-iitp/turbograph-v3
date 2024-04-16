#ifndef SSJ_ALLPAIRS_CLUSTER_H
#define SSJ_ALLPAIRS_CLUSTER_H

/* Copyright 2014-2015 Willi Mann
 *
 * This file is part of set_sim_join.
 *
 * set_sim_join is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Foobar is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with set_sim_join.  If not, see <http://www.gnu.org/licenses/>.
 */

// TODO change GPL license??

#include <algorithm>
#include <vector>
#include "classes.h"
#include "output.h"
#include "verify.h"
#include "data.h"
#include "lengthfilter.h"
#include "frequencysorting.h"
#include "indexes.h"
#include "allpairs_policies.h"
#include "candidateset.h"
#include "emptypairs.h"

#define SET_SIZE_DIFF_THRESHOLD 3 // TODO

template <typename AllPairsSimilarity/* = Jaccard*/,
		 typename AllPairsIndexingStrategyPolicy = IndexOnTheFlyPolicy,
		 typename AllPairsLengthFilterPolicy = DefaultLengthFilterPolicy
		 >
class AllPairsCluster: public Algorithm {
	public:
		typedef typename AllPairsSimilarity::threshold_type threshold_type;
		const threshold_type threshold;

		/* Terminology:
		   ForeignRecords .. Only for foreign joins - contain sets to probe against index
		   IndexedRecords .. Records that will be indexed - in case of self-joins, identical to probing set
		   */
		typedef IntRecord BaseRecord;
		typedef IntRecord ForeignRecord;
		typedef IntRecords ForeignRecords;

		struct CandidateData {
			unsigned int count;
			void reset() {
				count = 0;
			}
			CandidateData() : count(0) {}
		};

		class IndexedRecord : public ForeignRecord {
			public:
				typename AllPairsIndexPolicy::IndexStructureRecordData structuredata;
				IndexedRecord() {}
				inline void cleanup() {
				}
				CandidateData candidateData;
				int min_set_size;
				int union_set_size;
		};
		typedef std::vector<IndexedRecord> IndexedRecords;
	private:
		ForeignRecords foreignrecords;
		IndexedRecords indexedrecords;
		IndexedRecords clusterrecords;
		algo_handle_records_freq_sort<IndexedRecords, ForeignRecords> inputhandler;


	public:
		typedef AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy> self_type;

		typedef std::vector<IntRecord> Records;
		typedef IntRecord Record;

		typedef CandidateSet<CandidateData, IndexedRecords> CandidateSet_;
		typedef AllPairsSimilarity Similarity;
		typedef AllPairsIndexPolicy IndexStructurePolicy;
		typedef typename IndexStructurePolicy::template IndexStructure<self_type> IndexStructure;
		typedef AllPairsLengthFilterPolicy LengthFilterPolicy;
		typedef AllPairsIndexingStrategyPolicy IndexingStrategyPolicy;
		typedef typename AllPairsIndexingStrategyPolicy::template Index<self_type> Index;

		Index index;

		// tslee added for clustering
		Index cluster_index;
		std::vector<std::vector<int32_t>> cluster_to_rid_lists;


	public:
		//constructor
		AllPairsCluster(threshold_type threshold) : threshold(threshold), inputhandler(indexedrecords, foreignrecords) {}

		//addrecord and addforeignrecord must use swap to get the integer vector from record,
		//such that record refers to an empty record after the respective calls
		void addrecord(IntRecord & record);
		void addforeignrecord(IntRecord & record);

		//addrawrecord and addrawforeignrecord must use swap to get the integer vector from record,
		//such that record refers to an empty record after the respective calls
		void addrawrecord(IntRecord & record);
		void addrawforeignrecord(IntRecord & record);

		//multi-step process to measure the individual steps from outside
		void preparerecords();
		void prepareforeignrecords();
		void preparefinished();
		void doindex();
		void dojoin(
				HandleOutput * handleoutput);
		void docluster();
		std::vector<std::vector<int32_t>> &getctorlists();
		IntTokens &getclustertokens(unsigned int recordid);
		bool check_can_be_merged(const IndexedRecord &indexrecord, std::vector<unsigned int> &tokens2, int min_set_size);

		virtual ~AllPairsCluster();

	private:
		inline size_t proberecordssize() {
			if(Index::SELF_JOIN) {
				return indexedrecords.size();
			} else {
				return foreignrecords.size();
			}
		}
		GetProbeRecord<Index::SELF_JOIN,  ForeignRecords, IndexedRecords,
			IndexedRecord, typename Index::ProbeRecord> getproberecord;
};


template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::addrecord(IntRecord & record) {
	inputhandler.addrecord(record);
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::addforeignrecord(IntRecord & record) {
	inputhandler.addforeignrecord(record);
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::addrawrecord(IntRecord & record) {
	inputhandler.addrawrecord(record);
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::addrawforeignrecord(IntRecord & record) {
	inputhandler.addrawforeignrecord(record);
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::preparerecords() {
	inputhandler.prepareindex();

}
template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::prepareforeignrecords() {
	inputhandler.prepareforeign();

}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::preparefinished() {
	inputhandler.cleanup();

}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
std::vector<std::vector<int32_t>> &AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::getctorlists() {
	return cluster_to_rid_lists;
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
IntTokens &AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::getclustertokens(unsigned int recordid) {
	return clusterrecords[recordid].tokens;
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity,AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::doindex() {
	index.largest_tokenid(inputhandler.get_largest_tokenid());
	index.index_records(indexedrecords, threshold);

	cluster_index.largest_tokenid(inputhandler.get_largest_tokenid());
	cluster_index.index_records(indexedrecords, threshold); // do nothing; because of IndexOnTheFlyPolicy
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::dojoin(
				HandleOutput * handleoutput) {

	CandidateSet_ candidateSet(indexedrecords);

	std::vector<unsigned int> minoverlapcache;
	unsigned int lastprobesize = 0;

	// foreach record...
	for (unsigned recind = 0; recind < proberecordssize(); ++recind) {
		typename Index::ProbeRecord & record = getproberecord(indexedrecords, foreignrecords, recind);
		unsigned int reclen = record.tokens.size();

		// TODO: This only works for self joins
		outputempty(record, recind, indexedrecords, candidateSet, Similarity::outputall_le(threshold));

		//Minimum size of records in index
		unsigned int minsize = Similarity::minsize(reclen, threshold);

		// Check whether cache is to renew
		if(lastprobesize != reclen) {
			lastprobesize = reclen;
			unsigned int maxel = Index::SELF_JOIN ? reclen : Similarity::maxsize(reclen, threshold);
			minoverlapcache.resize(maxel + 1);
			for(unsigned int i = minsize; i <= maxel; ++i) {
				minoverlapcache[i] = Similarity::minoverlap(reclen, i, threshold);
			}
		}

		// Length of probing prefix
		unsigned int maxprefix = Similarity::maxprefix(reclen, threshold);

		typename AllPairsIndexingStrategyPolicy::template maxsizechecker<self_type> 
			maxsizechecker(reclen, threshold);


		// foreach elem in probing prefix
		for (unsigned recpos = 0; recpos < maxprefix; ++recpos) {
			unsigned int token = record.tokens[recpos];

			// get iterator and do min length filtering at the start
			typename Index::iterator ilit = index.getiterator(token);

			statistics.lookups.inc();

			maxsizechecker.updateprobepos(recpos);

			// First, apply min-length filter
			while(!ilit.end()) {
				// record from index
				IndexedRecord & indexrecord = indexedrecords[ilit->recordid];
				unsigned int indreclen = indexrecord.tokens.size();

				//Length filter - check whether minsize is satisfied
				if(ilit.lengthfilter(indreclen, minsize)) {
					break;
				}
				//Note: the iterator is increased by lengthfilter
			}

			// for each record in inverted list 
			while(!ilit.end() ) {

				if(!AllPairsIndexingStrategyPolicy::recindchecker::istocheck(recind, ilit->recordid)) {
					break;
				}

				// record from index 
				IndexedRecord & indexrecord = indexedrecords[ilit->recordid];
				unsigned int indreclen = indexrecord.tokens.size();

				//Length filter 2 - maxlength above tighter length filter (if enabled)
				if(maxsizechecker.isabove(indreclen)) {
					break;
				}

				statistics.indexEntriesSeen.inc();

				// insert candidate if it was not already seen
				CandidateData & candidateData = candidateSet.getCandidateData(ilit->recordid);

				if(candidateData.count == 0) {
					candidateSet.addRecord(ilit->recordid);
				}
				candidateData.count += 1;

				ilit.next();
			}
		}
		statistics.candidatesP1.add(candidateSet.size());

		//Now, verify candidates
		typename CandidateSet_::iterator candit = candidateSet.begin();
		for( ; candit != candidateSet.end(); ++candit) {
			CandidateData & candidateData = candidateSet.getCandidateData(*candit);

			statistics.candidatesVery.inc();
#ifndef CAND_ONLY
			// record from index
			const IndexedRecord & indexrecord = indexedrecords[*candit];
			unsigned int indreclen = indexrecord.tokens.size();
			
			unsigned int minoverlap = minoverlapcache[indreclen];

			//First position after last position by index lookup in indexed record
			unsigned int lastposind = IndexStructure::verify_indexrecord_start(indexrecord, indreclen, this);
			
			//First position after last position by index lookup in probing record
			unsigned int lastposprobe = LengthFilterPolicy::verify_record_start(reclen, maxprefix, minoverlap);

			unsigned int recpreftoklast = record.tokens[lastposprobe - 1];
			unsigned int indrecpreftoklast = indexrecord.tokens[lastposind - 1];

			unsigned int recpos, indrecpos;

			if(recpreftoklast > indrecpreftoklast) {

				recpos = candidateData.count;
				//first position after minprefix / lastposind
				indrecpos = lastposind;
			} else {
				// First position after maxprefix / lastposprobe
				recpos = lastposprobe;
				indrecpos = candidateData.count;
			}

			if(verifypair(record.tokens, indexrecord.tokens, minoverlap, recpos, indrecpos, candidateData.count)) {
				handleoutput->addPair(record, indexrecord);
			}
#endif
			candidateData.reset();
		}
		candidateSet.clear();

		index.index_record(record, recind, reclen, threshold);

	}
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
bool AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::check_can_be_merged(const IndexedRecord &indexrecord, std::vector<unsigned int> &tokens2, int min_set_size) {
	std::vector<unsigned int> merged_tokens;
	auto tokens1 = indexrecord.tokens;
	merged_tokens.reserve(tokens1.size() + tokens2.size());
	std::merge(tokens1.begin(), tokens1.end(),
		tokens2.begin(), tokens2.end(),
		std::back_inserter(merged_tokens));
	merged_tokens.erase(std::unique(merged_tokens.begin(), merged_tokens.end()), merged_tokens.end());

	int min = min_set_size > tokens2.size() ? tokens2.size() : min_set_size;

	// printf("merged_tokens.size() = %ld, min = %ld, SET_SIZE_DIFF_THRESHOLD = %ld\n", merged_tokens.size(), min, SET_SIZE_DIFF_THRESHOLD);
	
	return merged_tokens.size() - min <= SET_SIZE_DIFF_THRESHOLD;
}

template <typename AllPairsSimilarity, class AllPairsIndexingStrategyPolicy, class AllPairsLengthFilterPolicy>
void AllPairsCluster<AllPairsSimilarity, AllPairsIndexingStrategyPolicy, AllPairsLengthFilterPolicy>::docluster() {

	CandidateSet_ candidateSet(indexedrecords);

	std::vector<unsigned int> minoverlapcache;
	unsigned int lastprobesize = 0;

	// foreach record...
	for (unsigned recind = 0; recind < proberecordssize(); ++recind) {
		typename Index::ProbeRecord & record = getproberecord(indexedrecords, foreignrecords, recind);
		unsigned int reclen = record.tokens.size();
		// printf("\nRecord %d\n", recind);

		// TODO: This only works for self joins
		outputempty(record, recind, indexedrecords, candidateSet, Similarity::outputall_le(threshold));

		// Minimum size of records in index
		unsigned int minsize = Similarity::minsize(reclen, threshold);

		// Check whether cache is to renew
		// if (lastprobesize < reclen) {
		// 	lastprobesize = reclen;
		// 	unsigned int maxel = Index::SELF_JOIN ? reclen : Similarity::maxsize(reclen, threshold);
		// 	minoverlapcache.resize(maxel + 1);
		// 	for(unsigned int i = minsize; i <= maxel; ++i) {
		// 		minoverlapcache[i] = Similarity::minoverlap(reclen, i, threshold);
		// 	}
		// }

		// Length of probing prefix
		unsigned int maxprefix = Similarity::maxprefix(reclen, threshold);

		typename AllPairsIndexingStrategyPolicy::template maxsizechecker<self_type> 
			maxsizechecker(reclen, threshold);

		// foreach elem in probing prefix
		for (unsigned recpos = 0; recpos < maxprefix; ++recpos) {
			unsigned int token = record.tokens[recpos];

			// get iterator and do min length filtering at the start
			typename Index::iterator ilit = index.getiterator(token);

			statistics.lookups.inc();

			maxsizechecker.updateprobepos(recpos);

			// First, apply min-length filter
			while(!ilit.end()) {
				// record from index
				// IndexedRecord & indexrecord = indexedrecords[ilit->recordid];
				IndexedRecord & indexrecord = clusterrecords[ilit->recordid];
				unsigned int indreclen = indexrecord.tokens.size();

				// printf("Check length filter indreclen %d, minsize %d\n", indreclen, minsize);
				//Length filter - check whether minsize is satisfied
				if (ilit.lengthfilter(indreclen, minsize)) {
					break;
				}
				//Note: the iterator is increased by lengthfilter
			}

			// for each record in inverted list 
			while (!ilit.end()) {
				if (!AllPairsIndexingStrategyPolicy::recindchecker::istocheck(recind, ilit->recordid)) {
					break;
				}

				// record from index 
				// IndexedRecord & indexrecord = indexedrecords[ilit->recordid];
				IndexedRecord & indexrecord = clusterrecords[ilit->recordid];
				unsigned int indreclen = indexrecord.tokens.size();

				//Length filter 2 - maxlength above tighter length filter (if enabled)
				if (maxsizechecker.isabove(indreclen)) {
					break;
				}

				statistics.indexEntriesSeen.inc();

				// insert candidate if it was not already seen
				CandidateData & candidateData = candidateSet.getCandidateData(ilit->recordid);

				if (candidateData.count == 0) {
					candidateSet.addRecord(ilit->recordid);
				}
				candidateData.count += 1;

				ilit.next();
			}
		}
		// printf("candidateSet.size() = %ld\n", candidateSet.size());
		statistics.candidatesP1.add(candidateSet.size());

		// Now, verify candidates
		bool find_cluster = false;
		int best_cluster_id;
		double best_sim = threshold;
		double cur_sim;
		typename CandidateSet_::iterator candit = candidateSet.begin();
		for( ; candit != candidateSet.end(); ++candit) {
			CandidateData & candidateData = candidateSet.getCandidateData(*candit);

			statistics.candidatesVery.inc();
#ifndef CAND_ONLY
			// record from index
			// const IndexedRecord & indexrecord = indexedrecords[*candit];
			const IndexedRecord & indexrecord = clusterrecords[*candit];
			unsigned int indreclen = indexrecord.tokens.size();

			// printf("Cluster %d (%ld - %ld) > %ld ?\n", indexrecord.recordid, indexrecord.union_set_size, indexrecord.min_set_size, (uint64_t) SET_SIZE_DIFF_THRESHOLD);
			// if (indexrecord.union_set_size - indexrecord.min_set_size > SET_SIZE_DIFF_THRESHOLD) continue;
			
			// unsigned int minoverlap = minoverlapcache[indreclen];
			unsigned int minoverlap = Similarity::minoverlap(reclen, indreclen, threshold);
			// printf("minoverlap at minoverlapcache[%d] = %d\n", indreclen, minoverlap);

			//First position after last position by index lookup in indexed record
			unsigned int lastposind = IndexStructure::verify_indexrecord_start(indexrecord, indreclen, this);
			
			//First position after last position by index lookup in probing record
			unsigned int lastposprobe = LengthFilterPolicy::verify_record_start(reclen, maxprefix, minoverlap);

			unsigned int recpreftoklast = record.tokens[lastposprobe - 1];
			unsigned int indrecpreftoklast = indexrecord.tokens[lastposind - 1];

			unsigned int recpos, indrecpos;

			if (recpreftoklast > indrecpreftoklast) {

				recpos = candidateData.count;
				//first position after minprefix / lastposind
				indrecpos = lastposind;
			} else {
				// First position after maxprefix / lastposprobe
				recpos = lastposprobe;
				indrecpos = candidateData.count;
			}

			if (verifypairandgetsim(record.tokens, indexrecord.tokens, minoverlap, cur_sim, recpos, indrecpos, candidateData.count)) {
				// handleoutput->addPair(record, indexrecord);
				// find_cluster = true;
				// printf("cur_sim = %.3f, best_sim = %.3f\n", cur_sim, best_sim);
				if (cur_sim == 1.0) { // find home cluster
					find_cluster = true; // consider exact match case first
					best_cluster_id = indexrecord.recordid;
					break;
				} else if (cur_sim >= best_sim) {
					if (check_can_be_merged(indexrecord, record.tokens, indexrecord.min_set_size)) {
						find_cluster = true;
						best_cluster_id = indexrecord.recordid;
						best_sim = cur_sim;
					}
				}
			}
#endif
			candidateData.reset();
		}
		candidateSet.clear();

		// find_cluster = false;
		if (find_cluster) {
			bool update_index = false;
			// insert record into the best cluster
			cluster_to_rid_lists[best_cluster_id].push_back(recind);
			assert(recind == record.recordid);

			// expand the existing cluster's schema. TODO: need to be improved
			IndexedRecord & rec = clusterrecords[best_cluster_id];
			unsigned int prev_midprefix = Similarity::midprefix(rec.tokens.size(), threshold);
			std::vector<unsigned int> merged_tokens;
			rec.min_set_size = rec.min_set_size > record.tokens.size() ? record.tokens.size() : rec.min_set_size;
			if (best_sim != 1.0) {
				auto &tokens1 = rec.tokens;
				auto &tokens2 = record.tokens;
				merged_tokens.reserve(tokens1.size() + tokens2.size());
				// printf("[Merge - 1] ");
				// for (auto i = 0; i < tokens1.size(); i++) {
				// 	printf("%d ", tokens1[i]);
				// }
				// printf(" + [Merge - 2] ");
				// for (auto i = 0; i < tokens2.size(); i++) {
				// 	printf("%d ", tokens2[i]);
				// }
				// printf("\n");
				std::merge(tokens1.begin(), tokens1.end(),
					tokens2.begin(), tokens2.end(),
					std::back_inserter(merged_tokens));
				merged_tokens.erase(std::unique(merged_tokens.begin(), merged_tokens.end()), merged_tokens.end());
				// printf("=> [Merged] ");
				// for (auto i = 0; i < merged_tokens.size(); i++) {
				// 	printf("%d ", merged_tokens[i]);
				// }
				// printf("\n");
				rec.tokens.swap(merged_tokens);
			}
			rec.union_set_size = rec.tokens.size();

			// index record
			unsigned int midprefix = Similarity::midprefix(rec.tokens.size(), threshold);
			if (prev_midprefix == midprefix) {
				if (cur_sim == 1.0) {
					update_index = false;
				} else {
					for (auto i = 0; i < midprefix; i++) {
						if (rec.tokens[i] != merged_tokens[i]) {
							update_index = true;
							break;
						}
					}
				}
			} else {
				update_index = true;
			}
			if (update_index) {
				// unsigned int minsize_ = Similarity::minsize(rec.tokens.size(), threshold);
				index.index_record(rec, best_cluster_id, rec.tokens.size(), threshold, false);
				// if (lastprobesize < rec.tokens.size()) {
				// 	lastprobesize = rec.tokens.size();
				// 	unsigned int maxel = Index::SELF_JOIN ? rec.tokens.size() : Similarity::maxsize(rec.tokens.size(), threshold);
				// 	minoverlapcache.resize(maxel + 1);
				// 	for (unsigned int i = minsize_; i <= maxel; ++i) {
				// 		minoverlapcache[i] = Similarity::minoverlap(rec.tokens.size(), i, threshold);
				// 	}
				// }
			}

			// printf("Find best cluster %d for record %d / sim = %.3f\n", best_cluster_id, recind, best_sim);
		} else { // create new cluster
			// get new cluster id & create new cluster to rid lists
			int new_cluster_id = cluster_to_rid_lists.size();
			cluster_to_rid_lists.push_back(std::vector<int32_t>());
			cluster_to_rid_lists[new_cluster_id].push_back(recind);
			assert(recind == record.recordid);

			// create new cluster record
			clusterrecords.push_back(IndexedRecord());
			IndexedRecord & rec = clusterrecords[clusterrecords.size() - 1];
			// rec.tokens.swap(record.tokens);
			for (int i = 0; i < record.tokens.size(); i++)
				rec.tokens.push_back(record.tokens[i]);
			rec.recordid = new_cluster_id;
			rec.min_set_size = rec.tokens.size();
			rec.union_set_size = rec.tokens.size();

			// for (auto i = 0; i < rec.tokens.size(); i++) {
			// 	printf("%d ", rec.tokens[i]);
			// }
			// printf("\n");

			// index record
			index.index_record(rec, new_cluster_id, reclen, threshold);

			// printf("Create new cluster %d for record %d\n", new_cluster_id, recind);
		}
		// index.index_record(record, recind, reclen, threshold);

	}
	printf("Finished, # clusters = %ld\n", clusterrecords.size());
	for (int i = 0; i < clusterrecords.size(); i++) {
		printf("Cluster %d: ", i);
		IndexedRecord &clust_rec = clusterrecords[i];
		for (int token_idx = 0; token_idx < clust_rec.tokens.size(); token_idx++) {
			printf("%d ", clust_rec.tokens[token_idx]);
		}
		printf("\n");
		for (int j = 0; j < cluster_to_rid_lists[i].size(); j++) {
			printf("\tRecord %d: ", cluster_to_rid_lists[i][j]);
			auto &rec = indexedrecords[cluster_to_rid_lists[i][j]];
			// getproberecord(indexedrecords, foreignrecords, cluster_to_rid_lists[i][j]);
			for (int token_idx = 0; token_idx < rec.tokens.size(); token_idx++) {
				printf("%d ", rec.tokens[token_idx]);
			}
			printf("\n");
		}
	}
}

template <typename MpJoinSimilarity, class MpJoinIndexingStrategyPolicy, class MpJoinLengthFilterPolicy>
AllPairsCluster<MpJoinSimilarity, MpJoinIndexingStrategyPolicy, MpJoinLengthFilterPolicy>::~AllPairsCluster() {
}


#endif
