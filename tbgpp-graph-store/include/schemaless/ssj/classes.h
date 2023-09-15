#ifndef SSJ_CLASSES_H
#define SSJ_CLASSES_H

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

#include<vector>
//#include "sparsehash/dense_hash_map"
#include "similarity.h"
#include "statistics.h"
#include "mpltricks.h"

template <class elementtype>
class Record {
	public:
		typedef std::vector<elementtype> Tokens;
		typedef unsigned int recordid_type;
		recordid_type recordid;
		Tokens tokens;
};

// forward-declare HandleOutput class
class HandleOutput;

typedef Record<unsigned int> IntRecord;
typedef typename IntRecord::Tokens IntTokens;
typedef std::vector<IntRecord > IntRecords;
typedef std::vector<std::pair<int, int> > Result;

class Algorithm {

	public:
		typedef Statistics<RealIncreaser> _Statistics;
		_Statistics statistics;

		//addrecord and addforeignrecord must use swap to get the integer vector from record,
		//such that record refers to an empty record after the respective calls
		virtual void addrecord(IntRecord & record) = 0;
		virtual void addforeignrecord(IntRecord & record) = 0;

		//addrawrecord and addrawforeignrecord must use swap to get the integer vector from record,
		//such that record refers to an empty record after the respective calls
		virtual void addrawrecord(IntRecord & record) = 0;
		virtual void addrawforeignrecord(IntRecord & record) = 0;

		//multi-step process to measure the individual steps from outside
		virtual void preparerecords() = 0;
		virtual void prepareforeignrecords() = 0;
		//preparation of input finished - method can be used to clean up data structures from preparation phase
		virtual void preparefinished() {};
		virtual void doindex() = 0;
		virtual void dojoin(
				HandleOutput * handleoutput) = 0;
		virtual void docluster() = 0;
		virtual std::vector<std::vector<int32_t>> &getctorlists() = 0;
		virtual IntTokens &getclustertokens(unsigned int recordid) = 0;
		virtual ~Algorithm() {}
};

template<bool isselfjoin, class ForeignRecords, class IndexedRecords, class IndexedRecord, class ProbeRecord>
class GetProbeRecord {
	private:

		struct _getproberecord_self_join {
			static inline ProbeRecord & _get(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords, unsigned int getid) {
				return indexedrecords[getid];
			}
		};
		struct _getproberecord_foreign_join {
			static inline ProbeRecord & _get(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords, unsigned int getid) {
				return foreignrecords[getid];
			}
		};

	public:
		inline ProbeRecord & operator()(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords, unsigned int getid) {
			typedef typename IF<isselfjoin, _getproberecord_self_join, _getproberecord_foreign_join>::RET _get_proberecord;

			return _get_proberecord::_get(indexedrecords, foreignrecords, getid);
		}
};

template<bool isselfjoin, class ForeignRecords, class IndexedRecords>
class GetProbeRecords {
	private:

		struct _getproberecords_self_join {
			static inline IndexedRecords & _get(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords) {
				return indexedrecords;
			}
		};
		struct _getproberecords_foreign_join {
			static inline ForeignRecords & _get(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords) {
				return foreignrecords;
			}
		};

	public:
		typedef typename IF<isselfjoin, IndexedRecords, ForeignRecords>::RET ProbeRecords;
		typedef typename IF<isselfjoin, _getproberecords_self_join, _getproberecords_foreign_join>::RET _get_proberecords;

		inline ProbeRecords & operator()(IndexedRecords & indexedrecords, ForeignRecords & foreignrecords) {

			return _get_proberecords::_get(indexedrecords, foreignrecords);
		}
};

#endif
