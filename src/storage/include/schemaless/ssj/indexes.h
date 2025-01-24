#ifndef SSJ_INDEXES_H
#define SSJ_INDEXES_H

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

#include <vector>
#include "mpltricks.h"

struct GlobalGenericIndexListEntry {
	int recordid;
	GlobalGenericIndexListEntry(unsigned int recordid) : recordid(recordid) {}

	friend bool operator==(GlobalGenericIndexListEntry const &a, GlobalGenericIndexListEntry const &b) {
		return a.recordid == b.recordid;
	}
};

template <class AlgoIndexListEntry>
struct GenericIndexListHeader {
	typedef std::vector<AlgoIndexListEntry > AlgoIndexList;

	AlgoIndexList indexlist;
};

class IndexOnTheFlyPolicy {
	public:

		template <class Algorithm>
		struct Index {
			enum {
				SELF_JOIN = true,
				INDEXFIRST = false
			};
			typedef typename Algorithm::IndexStructure IndexStructure;
			typedef typename Algorithm::IndexStructure::iterator iterator;
			typedef typename Algorithm::Similarity Similarity;
			typedef typename Algorithm::IndexedRecords IndexedRecords;
			typedef typename Algorithm::IndexedRecord IndexedRecord;
			typedef typename Algorithm::BaseRecord BaseRecord;

			typedef typename Algorithm::IndexedRecord ProbeRecord;
			typedef typename Algorithm::IndexedRecords ProbeRecords;

			IndexStructure index;

			typedef typename Similarity::threshold_type threshold_type;

			void largest_tokenid(unsigned int size_universe) {
				index.largest_tokenid(size_universe);
			}
			void index_records(const IndexedRecords & records, threshold_type threshold) {}

			inline void index_record(IndexedRecord & record, unsigned int recind, unsigned int reclen, threshold_type threshold, bool new_record=true) {
				//Put record to index
				unsigned int midprefix = Similarity::midprefix(reclen, threshold);
				record.structuredata.setInitialPrefSize(midprefix);
				// if (!add_token) return;
				for(unsigned int recpos = 0; recpos < midprefix; ++recpos) {
					unsigned int token = record.tokens[recpos];
					index.addtoken(token, recind, recpos, new_record);
				}
			}

			inline iterator getiterator(unsigned int token) {
				return index.getiterator(token);
			}

			inline iterator end() {
				return index.end();
			}
		};

		template <class Algorithm>
		struct maxsizechecker {
			unsigned int maxlen;
			unsigned int curlen;

			typedef typename Algorithm::Similarity::threshold_type threshold_type;
			threshold_type threshold;
			inline maxsizechecker(unsigned int curlen, threshold_type threshold) : curlen(curlen), threshold(threshold) {
				if(Algorithm::LengthFilterPolicy::POS) {
					maxlen = Algorithm::Similarity::maxsize(curlen, threshold);
				}
			}

			inline bool isabove(unsigned int len) {
				if(Algorithm::LengthFilterPolicy::POS) {
					return len > maxlen;
				} else {
					return false;
				}
			}
			
			inline void updateprobepos(unsigned int pos) {
				if(Algorithm::LengthFilterPolicy::POS) {
					maxlen = Algorithm::Similarity::maxsize(curlen, pos, threshold);
				}
			}

		};
		
		struct recindchecker {
			inline static bool istocheck(unsigned int reclen, unsigned int indreclen) {
				return true;
			}
		};


};

template <bool foreign>
class IndexFirstPolicy {
	public:

		template <class Algorithm>
		struct Index {
			enum {
				SELF_JOIN = !foreign,
				INDEXFIRST = true
			};
			typedef typename Algorithm::IndexStructure IndexStructure;
			typedef typename Algorithm::IndexStructure::iterator iterator;
			typedef typename Algorithm::Similarity Similarity;
			typedef typename Algorithm::IndexedRecords IndexedRecords;
			typedef typename Algorithm::IndexedRecord IndexedRecord;
			typedef typename Algorithm::BaseRecord BaseRecord;

			IndexStructure index;

			typedef typename Similarity::threshold_type threshold_type;

			// Template metaprogramming trickery to select right ProbeRecord type
			typedef typename IF<foreign, typename Algorithm::ForeignRecord, typename Algorithm::IndexedRecord>::RET ProbeRecord;
			typedef typename IF<foreign, typename Algorithm::ForeignRecords, typename Algorithm::IndexedRecords>::RET ProbeRecords;

			void largest_tokenid(unsigned int size_universe) {
				index.largest_tokenid(size_universe);
			}

			inline unsigned int indexPrefixSize(unsigned int reclen, threshold_type threshold) const {
			 return foreign ? Similarity::maxprefix(reclen, threshold) : Similarity::midprefix(reclen, threshold);
			}

			void index_records(IndexedRecords & records, threshold_type threshold) {
				unsigned int recind = 0;
				for(; recind < records.size(); ++recind) {
					IndexedRecord & record = records[recind];
					unsigned int reclen = record.tokens.size();
					unsigned int indexprefixsize = indexPrefixSize(reclen, threshold);
					record.structuredata.setInitialPrefSize(indexprefixsize);
					for(unsigned int recpos = 0; recpos < indexprefixsize; ++recpos) {
						unsigned int token = record.tokens[recpos];
						index.addtoken(token, recind, recpos);
					}
				}
			}

			inline void index_record(const BaseRecord & record, unsigned int recind, 
					unsigned int reclen, threshold_type threshold, bool add_token=true) {}

			inline iterator getiterator(unsigned int token) {
				return index.getiterator(token);
			}

			inline iterator end() {
				return index.end();
			}
		};
		
		template <class Algorithm>
		struct maxsizechecker {
			unsigned int curlen;

			typedef typename Algorithm::Similarity::threshold_type threshold_type;
			threshold_type threshold;

			unsigned int maxlen;
			inline maxsizechecker(unsigned int curlen, threshold_type threshold) : curlen(curlen), threshold(threshold) {
				maxlen = Algorithm::Similarity::maxsize(curlen, threshold);
			}

			inline void updateprobepos(unsigned int pos) {
				if(Algorithm::LengthFilterPolicy::POS) {
					maxlen = Algorithm::Similarity::maxsize(curlen, pos, threshold);
				}
			}

			inline bool isabove(unsigned int len) {
				return len > maxlen;
			}
		};

		struct recindchecker {
			inline static bool istocheck(unsigned int recid, unsigned int indrecid) {
				return foreign ? true : recid > indrecid;
			}
		};


};

#endif
