#ifndef SSJ_ALLPAIRS_POLICIES_H
#define SSJ_ALLPAIRS_POLICIES_H

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

#include "inv_index.h"

class AllPairsIndexPolicy {

	protected:
		struct IndexListEntry : GlobalGenericIndexListEntry {
// This is for the experimental position filter added here
#ifdef POS_FILTER
			unsigned int position;
			IndexListEntry(unsigned int recordid, unsigned int position) :
				GlobalGenericIndexListEntry(recordid), position(position) {}
#else
			IndexListEntry(unsigned int recordid) : GlobalGenericIndexListEntry(recordid) {}
#endif
		};

		struct IndexListHeader {
			std::vector<IndexListEntry> ilist;
			int firsttocheck;
			IndexListHeader() : firsttocheck(0) {}
		};

	public:

		struct IndexStructureRecordData {
			unsigned int prefsize;
			inline IndexStructureRecordData() : prefsize(INT_MAX) {}
			inline void setInitialPrefSize(unsigned int prefsize) {
				this->prefsize = prefsize;
			}
		};

		template <typename Algorithm>
		struct IndexStructure {

			typedef InvIndexVector::template IndexMap<IndexListHeader> Index;
			typedef typename Algorithm::IndexedRecord IndexedRecord;
			Index index;

			struct iterator {
				unsigned int curelem;
				unsigned int elemcount;

				IndexListHeader * ilheader;

				// Empty iterator if there are no entries
				inline iterator() : curelem(0), elemcount(0) {}

				inline iterator(IndexListHeader * ilheader) :
					ilheader(ilheader) {
						curelem = ilheader->firsttocheck;
						elemcount = ilheader->ilist.size();
					}

				inline bool end() const {
					return curelem == elemcount;
				}

				inline void next() {
					assert(curelem >= 0);
					curelem += 1;
				}

				inline bool lengthfilter(unsigned int indreclen, unsigned int minsize) {
					if(indreclen < minsize) {
						curelem += 1;
						ilheader->firsttocheck = curelem;
						return false;
					}
					return true;
				}

				inline IndexListEntry & operator*() {
					assert(curelem >= 0);
					return ilheader->ilist[curelem];
				}

				inline IndexListEntry * operator->() {
					assert(curelem >= 0);
					return &ilheader->ilist[curelem];
				}

			};


			inline void addtoken(unsigned int token, unsigned int recind, unsigned int recpos, bool new_record) {
				Index::value_type & ilhead = index.get_list_create(token);
#ifdef POS_FILTER
				ilhead.ilist.push_back(IndexListEntry(recind, recpos));
#else
				if (new_record) {
					ilhead.ilist.push_back(IndexListEntry(recind));
				} else {
					ilhead.ilist.push_back(IndexListEntry(recind));
					std::sort(ilhead.ilist.begin(), ilhead.ilist.end(), 
						[&](const IndexListEntry &a, const IndexListEntry &b){
							return a.recordid < b.recordid;
						});
					ilhead.ilist.erase(std::unique(ilhead.ilist.begin(), ilhead.ilist.end()), ilhead.ilist.end());
				}
#endif
			}

			inline iterator getiterator(unsigned int token) {
				typename Index::value_type * ilhead = index.get_list(token);
				if( ilhead != NULL ) {
					return iterator(ilhead);
					//If the header is null, we are the end
				} else {
					return iterator();
				}
			}

			static inline unsigned int verify_indexrecord_start(const IndexedRecord & indexrecord, unsigned int indreclen, Algorithm * algo) {
				return indexrecord.structuredata.prefsize;
			}

			void largest_tokenid(unsigned int tokenid) {
				index.resize(tokenid + 1);
			}
		};
};

#endif
