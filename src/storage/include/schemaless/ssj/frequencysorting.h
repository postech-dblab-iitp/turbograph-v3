#ifndef SSJ_FREQUENCYSORTING_J
#define SSJ_FREQUENCYSORTING_J

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


struct NoLexSort {
	template <typename Record>
	static unsigned int sortprefixsize(const Record & r) {
		return 0;
	}
};

template<typename IndexedRecords, typename ForeignRecords, typename LexSortLength=NoLexSort>
struct algo_handle_records_freq_sort {
	
	struct inttokenpair {
		unsigned int origpos;
		unsigned int count;
		inttokenpair(unsigned int origpos, unsigned int count) : origpos(origpos), count(count) {}
	};

	struct frequencysort {
		bool operator()(const inttokenpair & ip1, const inttokenpair & ip2) const {
			return ip1.count < ip2.count;
		}
	};

	struct CmpTokenCount {
		bool lexsort;
		CmpTokenCount(bool lexsort=false) : lexsort(lexsort) {}

		template <class RecordType>
		bool operator()(const RecordType & r1, const RecordType & r2) {
			if(!lexsort || r1.tokens.size() != r2.tokens.size()) {
				return r1.tokens.size() < r2.tokens.size();
			}
			assert(r1.tokens.size() == r2.tokens.size());
			unsigned int tokens = r1.tokens.size();

			IntTokens::const_iterator ir1 = r1.tokens.begin();
			IntTokens::const_iterator ir2 = r2.tokens.begin();
			IntTokens::const_iterator ir1stop = ir1 + LexSortLength::sortprefixsize(r1);
			for(; ir1 != ir1stop; ++ir1, ++ir2) {
				if(*ir1 != *ir2) {
					return *ir1 < *ir2;
				}
			}
			return false;
		}
	};

	typedef typename IndexedRecords::value_type IndexedRecord;
	typedef typename ForeignRecords::value_type ForeignRecord;

	//Counts how often a particular token has been seen
	typedef std::vector<inttokenpair> Tokensseen;
	Tokensseen tokensseen;

	//maps input tokens to final (frequency-ordered) tokenids
	typedef std::vector<unsigned int> Tokenmap;
	Tokenmap tokenmap;

	IndexedRecords & records;
	ForeignRecords & foreignrecords;

	IndexedRecord IndexedDummy;
	IndexedRecord ForeignDummy;

	unsigned int maxtokenid;
	unsigned int nextrecordid;
	unsigned int nextforeignrecordid;

	algo_handle_records_freq_sort(IndexedRecords & records, ForeignRecords & foreignrecords) : 
		records(records),
		foreignrecords(foreignrecords),
		maxtokenid(0),
   		nextrecordid(0),
		nextforeignrecordid(0)	{
			//Push a token that occurs 0 times in indexed sets (for foreign join)
			tokensseen.push_back(inttokenpair(tokensseen.size(), 0));
	}

	inline IndexedRecord & addrecord(IntRecord & record) {
		// if the record has zero length, ignore it but increment
		// nextrecordid such that the output can still be easily mapped to the input
		if(record.tokens.size() == 0) {
			nextrecordid += 1;
			return IndexedDummy;
		}

		// For each token, update the tokensseen vector // tslee we don't need this
		typename IntRecord::Tokens::iterator toit = record.tokens.begin();
		for(; toit != record.tokens.end(); ++toit) {
			if(*toit > maxtokenid) {
				// printf("%d, %d\n", *toit - 1, maxtokenid);
				// assert(*toit - 1 == maxtokenid);
				maxtokenid += *toit;
				// maxtokenid += 1;
				// tokensseen.push_back(inttokenpair(tokensseen.size(), 1));
			} else {
				// tokensseen[*toit].count += 1;
			}
		}
		records.push_back(IndexedRecord());
		IndexedRecord & rec = records[records.size() - 1];
		rec.tokens.swap(record.tokens);
		rec.recordid = nextrecordid;
		nextrecordid += 1;
		return rec;
	}

	inline ForeignRecord & addforeignrecord(IntRecord & record) {

		// if the record has zero length, ignore it but increment
		// nextforeignrecordid such that the output can still be easily mapped
		// to the input
		if(record.tokens.size() == 0) {
			nextforeignrecordid += 1;
			return ForeignDummy;
		}

		foreignrecords.push_back(ForeignRecord());
		ForeignRecord & rec = foreignrecords[foreignrecords.size() - 1];
		rec.tokens.swap(record.tokens);
		rec.recordid = nextforeignrecordid;
		nextforeignrecordid += 1;
		return rec;
	}

	inline IndexedRecord & addrawrecord(IntRecord & record) {
		for(unsigned int token : record.tokens) {
			maxtokenid = std::max(maxtokenid, token);
		}
		records.push_back(IndexedRecord());
		IndexedRecord & rec = records[records.size() - 1];
		rec.tokens.swap(record.tokens);
		rec.recordid = nextrecordid;
		nextrecordid += 1;
		return rec;
	}

	inline ForeignRecord & addrawforeignrecord(IntRecord & record) {
		foreignrecords.push_back(ForeignRecord());
		ForeignRecord & rec = foreignrecords[foreignrecords.size() - 1];
		rec.tokens.swap(record.tokens);
		rec.recordid = nextrecordid;
		nextforeignrecordid += 1;
		return rec;
	}

	// Prepare the record set to index
	inline void prepareindex(bool lexsort=false) {
		// Sort tokensseen by the count field (inttokenpair)
		std::sort(tokensseen.begin(), tokensseen.end(), frequencysort());

		//prepare vector to map orig token ids to new ones
		tokenmap.resize(tokensseen.size());

		// Fill the map
		for(unsigned int i = 0; i != tokensseen.size(); ++i) {
			tokenmap[tokensseen[i].origpos] = i;
		}

		// Map the tokens
		for(unsigned int recid = 0; recid < records.size(); ++recid) {
			IndexedRecord & rec = records[recid];
			for(unsigned int recpos = 0; recpos < rec.tokens.size(); ++recpos) {
				rec.tokens[recpos] = tokenmap[rec.tokens[recpos]];
			}
			std::sort(rec.tokens.begin(), rec.tokens.end());
		}
		std::sort(records.begin(), records.end(), CmpTokenCount(lexsort));
	}

	inline void prepareforeign(bool lexsort=false) {
		// Map the tokens
		for(unsigned int recid = 0; recid < foreignrecords.size(); ++recid) {
			ForeignRecord & rec = foreignrecords[recid];
			for(unsigned int recpos = 0; recpos < rec.tokens.size(); ++recpos) {
				rec.tokens[recpos] = tokenmap[rec.tokens[recpos]];
			}
			std::sort(rec.tokens.begin(), rec.tokens.end());
		}
		std::sort(foreignrecords.begin(), foreignrecords.end(), CmpTokenCount(lexsort));

	}
	
	void cleanup() {
		Tokenmap().swap(tokenmap);
		Tokensseen().swap(tokensseen);
	}

	unsigned int get_largest_tokenid() const {
		return maxtokenid;
	}
};
#endif
