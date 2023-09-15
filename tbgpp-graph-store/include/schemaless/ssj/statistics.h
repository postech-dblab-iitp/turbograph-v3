#ifndef SSJ_STATISTICS_H
#define SSJ_STATISTICS_H

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

#include<ostream>
#include<iomanip>
#include<iostream>
#include<sstream>


#ifdef NO_STAT_COUNTERS
struct RealIncreaser {
	inline static void inc(unsigned long ) {}
	inline static void add(unsigned long, unsigned long) {}
};
#else
struct RealIncreaser {
	inline static void inc(unsigned long & val) {
		val += 1;
	}
	inline static void add(unsigned long & val, unsigned long a) {
		val += a;
	}
};
#endif

#ifdef EXT_STATISTICS
struct RealIncreaserExt {
	inline static void inc(unsigned long & val) {
		val += 1;
	}
	inline static void add(unsigned long & val, unsigned long a) {
		val += a;
	}
};
#else
struct RealIncreaserExt {
	inline static void inc(unsigned long ) {}
	inline static void add(unsigned long, unsigned long) {}
};
#endif

template <typename Increaser>
class Statistics {
	public:
		enum {
			MAX_ADAPTJOIN_EXT = 101
		};
		struct StatItem {
			unsigned long value;
			inline void inc() {
				Increaser::inc(value);
			}
			inline void add(unsigned int a) {
				Increaser::add(value, a);
			}
			StatItem() : value(0) {}
		};

		struct StatItem candidatesP1;
		struct StatItem lookups;
		struct StatItem indexEntriesSeen;
		struct StatItem candidatesVery;
#ifdef CYCLE_COUNT
		struct StatItem verifyTrueCycles;
		struct StatItem verifyFalseCycles;
		struct StatItem sfPassingCycles;
		struct StatItem sfFilteredCycles;
#endif
		struct StatItem adaptjoinlastext[MAX_ADAPTJOIN_EXT];
		struct StatItem lshL;

		template<typename statitem>
			static inline void printitem(const std::string & descr, const statitem & item) {
				if(item.value != 0) {
					std::cout << std::setw(20) << descr << std::setw(14) << item.value << std::endl;
				}
			}


		friend std::ostream & operator<<(std::ostream & os, const Statistics<Increaser> & statistics) {
			std::cout << "Statistics:" << std::endl;
			printitem("lookups", statistics.lookups);
			printitem("indexEntriesSeen", statistics.indexEntriesSeen);
			printitem("candidatesP1", statistics.candidatesP1);
			printitem("candidatesVerify", statistics.candidatesVery);
#ifdef CYCLE_COUNT
			printitem("verifyTrueCycles", statistics.verifyTrueCycles);
			printitem("verifyFalseCycles", statistics.verifyFalseCycles);

			printitem("sfPassingCycles", statistics.sfPassingCycles);
			printitem("sfFilteredCycles", statistics.sfFilteredCycles);
#endif
			for(unsigned int i = 0; i < MAX_ADAPTJOIN_EXT; ++i) {
				std::stringstream text;
				text << "adaptjoinlastext" << i;
				printitem(text.str(), statistics.adaptjoinlastext[i]);
			}
			printitem("lshL", statistics.lshL);
			return os;
		}
};

template <typename Increaser>
class ExtStatistics {
	public:
		struct StatItem {
			unsigned long value;
			inline void inc() {
				Increaser::inc(value);
			}
			inline void add(unsigned int a) {
				Increaser::add(value, a);
			}
			StatItem() : value(0) {}
		};

		struct StatItem verifyTrueSteps;
		struct StatItem verifyTrueSetSizeSum;
		struct StatItem verifyTrueSetSizeCnt;
		struct StatItem verifyFalseSteps;
		struct StatItem verifyFalseSetSizeSum;
		struct StatItem verifyFalseSetSizeCnt;
		struct StatItem verifyLoop0True;
		struct StatItem verifyLoop0False;

		template<typename statitem>
			static inline void printitem(const std::string & descr, const statitem & item) {
				if(item.value != 0) {
					std::cout << std::setw(23) << descr << std::setw(14) << item.value << std::endl;
				}
			}


		friend std::ostream & operator<<(std::ostream & os, const ExtStatistics<Increaser> & statistics) {
			std::cout << "Extended Statistics:" << std::endl;

			printitem("verifyTrueSteps", statistics.verifyTrueSteps);
			printitem("verifyTrueSetSizeSum", statistics.verifyTrueSetSizeSum);
			printitem("verifyTrueSetSizeCnt", statistics.verifyTrueSetSizeCnt);

			printitem("verifyFalseSteps", statistics.verifyFalseSteps);
			printitem("verifyFalseSetSizeSum", statistics.verifyFalseSetSizeSum);
			printitem("verifyFalseSetSizeCnt", statistics.verifyFalseSetSizeCnt);

			printitem("verifyLoop0True", statistics.verifyLoop0True);
			printitem("verifyLoop0False", statistics.verifyLoop0False);
			return os;
		}
};

extern ExtStatistics<RealIncreaserExt> extStatistics;

#endif
