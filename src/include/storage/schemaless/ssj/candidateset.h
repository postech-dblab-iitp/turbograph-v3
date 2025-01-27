#include<vector>

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

template <class CandidateData, class IndexedRecords>
class CandidateSet {
	private:
		typedef std::vector<unsigned int> CandidateIndices;
		CandidateIndices candidateIndices;
		IndexedRecords & indexedrecords;
	public:
		typedef CandidateIndices::iterator iterator;
		CandidateSet(IndexedRecords & indexedrecords) : indexedrecords(indexedrecords) {}
		inline CandidateData & getCandidateData(unsigned int indexrecordid) {
			return indexedrecords[indexrecordid].candidateData;
		}

		// This method should only be called once per 
		inline void addRecord(unsigned int candrecid) {
			candidateIndices.push_back(candrecid);
		}

		inline size_t size() const {
			return candidateIndices.size();
		}

		inline iterator begin() {
			return candidateIndices.begin();
		}
		
		inline iterator end() {
			return candidateIndices.end();
		}

		inline void clear() {
			candidateIndices.clear();
		}
};
