#ifndef SSJ_VERIFY_H
#define SSJ_VERIFY_H

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

#include "classes.h"

#ifndef LONG_VERIFICATION
template <typename T>
bool inline verifypair(const T & r1, const T & r2, unsigned int overlapthres, unsigned int posr1=0, unsigned int posr2=0, unsigned int foundoverlap=0) {
	unsigned int maxr1 = r1.size() - posr1 + foundoverlap;
	unsigned int maxr2 = r2.size() - posr2 + foundoverlap;

	unsigned int steps = 0;

	while(maxr1 >= overlapthres && maxr2 >= overlapthres && foundoverlap < overlapthres) {
		steps++;
		if(r1[posr1] == r2[posr2]) {
			++posr1;
			++posr2;
			++foundoverlap;
		} else if (r1[posr1] < r2[posr2]) {
			++posr1;
			--maxr1;
		} else {
			++posr2;
			--maxr2;
		}
	}
	// if(foundoverlap >= overlapthres) {
	// 	extStatistics.verifyTrueSteps.add(steps);
	// 	if(steps == 0) {
	// 		extStatistics.verifyLoop0True.inc();
	// 	}
	// 	extStatistics.verifyTrueSetSizeSum.add(r1.size() + r2.size());
	// 	extStatistics.verifyTrueSetSizeCnt.add(2);

	// } else {
	// 	extStatistics.verifyFalseSteps.add(steps);
	// 	if(steps == 0) {
	// 		extStatistics.verifyLoop0False.inc();
	// 	}
	// 	extStatistics.verifyFalseSetSizeSum.add(r1.size() + r2.size());
	// 	extStatistics.verifyFalseSetSizeCnt.add(2);
	// }

	return foundoverlap >= overlapthres;

}

template <typename T>
bool inline verifypairandgetsim(const T & r1, const T & r2, unsigned int overlapthres, double &similarity, unsigned int posr1=0, unsigned int posr2=0, unsigned int foundoverlap=0) {
	unsigned int maxr1 = r1.size() - posr1 + foundoverlap;
	unsigned int maxr2 = r2.size() - posr2 + foundoverlap;
	unsigned int posr1_old = posr1;
	unsigned int posr2_old = posr2;
	unsigned int foundoverlap_old = foundoverlap;

	unsigned int steps = 0;

	bool exact_match = true;
	if (r1.size() != r2.size()) exact_match = false;
	// printf("[verify-before] r1.size = %ld, r2.size = %ld, posr1 = %d, posr2 = %d, maxr1 = %d, maxr2 = %d, overlapthres = %d, foundoverlap = %d\n", 
	// 	r1.size(), r2.size(), posr1, posr2, maxr1, maxr2, overlapthres, foundoverlap);
	
	// printf("Compare: ");
	// for (int i = 0; i < r1.size(); i++) {
	// 	if (posr1 == i) printf("[ ");
	// 	printf("%d ", r1[i]);
	// }
	// printf("] vs ");
	// for (int i = 0; i < r2.size(); i++) {
	// 	if (posr2 == i) printf("[ ");
	// 	printf("%d ", r2[i]);
	// }
	// printf("]\n");

	// while(maxr1 >= overlapthres && maxr2 >= overlapthres && foundoverlap < overlapthres) {
	while (maxr1 >= overlapthres && maxr2 >= overlapthres && posr1 < r1.size() && posr2 < r2.size()) {
		steps++;
		if(r1[posr1] == r2[posr2]) {
			++posr1;
			++posr2;
			++foundoverlap;
		} else if (r1[posr1] < r2[posr2]) {
			++posr1;
			--maxr1;
		} else {
			++posr2;
			--maxr2;
		}
	}

	// printf("[verify-after] r1.size = %ld, r2.size = %ld, posr1 = %d, posr2 = %d, maxr1 = %d, maxr2 = %d, overlapthres = %d, foundoverlap = %d\n", 
	// 	r1.size(), r2.size(), posr1, posr2, maxr1, maxr2, overlapthres, foundoverlap);
	
	if (foundoverlap >= overlapthres) {
		// extStatistics.verifyTrueSteps.add(steps);
		// if(steps == 0) {
		// 	extStatistics.verifyLoop0True.inc();
		// }
		// extStatistics.verifyTrueSetSizeSum.add(r1.size() + r2.size());
		// extStatistics.verifyTrueSetSizeCnt.add(2);
		
		unsigned int real_overlap_cnt = foundoverlap - foundoverlap_old;
		unsigned int i = 0;
		unsigned int j = 0;
		while (i < posr1_old && j < posr2_old) {
			if (r1[i] == r2[j]) {
				++i;
				++j;
				++real_overlap_cnt;
			} else if (r1[i] < r2[j]) {
				++i;
			} else {
				++j;
			}
		}
		similarity = static_cast<double>(real_overlap_cnt) / (r1.size() + r2.size() - real_overlap_cnt); // consider jaccard only

		// if (similarity == 1.0) {
		// 	printf("foundoverlap = %d, r1.size() = %ld, r2.size() = %ld\n", real_overlap_cnt, r1.size(), r2.size());
		// 	printf("Compare: ");
		// 	for (int i = 0; i < r1.size(); i++) {
		// 		printf("%d ", r1[i]);
		// 	}
		// 	printf(" vs ");
		// 	for (int i = 0; i < r2.size(); i++) {
		// 		printf("%d ", r2[i]);
		// 	}
		// 	printf("\n");
		// }
	} else {
		// extStatistics.verifyFalseSteps.add(steps);
		// if(steps == 0) {
		// 	extStatistics.verifyLoop0False.inc();
		// }
		// extStatistics.verifyFalseSetSizeSum.add(r1.size() + r2.size());
		// extStatistics.verifyFalseSetSizeCnt.add(2);
	}

	return foundoverlap >= overlapthres;
}

#else

template <typename T>
bool inline verifypair(const T & r1, const T & r2, unsigned int overlapthres, unsigned int posr1=0, unsigned int posr2=0, unsigned int foundoverlap=0) {
	unsigned int sizer1 = r1.size();
	unsigned int sizer2 = r2.size();

	unsigned int steps = 0;

	while(posr1 < sizer1 && posr2 < sizer2) {
		++steps;
		if(r1[posr1] == r2[posr2]) {
			++posr1;
			++posr2;
			++foundoverlap;
		} else if (r1[posr1] < r2[posr2]) {
			++posr1;
		} else {
			++posr2;
		}
	}
	if(foundoverlap >= overlapthres) {
		extStatistics.verifyTrueSteps.add(steps);
		if(steps == 0) {
			extStatistics.verifyLoop0True.inc();
		}
	} else {
		extStatistics.verifyFalseSteps.add(steps);
		if(steps == 0) {
			extStatistics.verifyLoop0False.inc();
		}
	}
	return foundoverlap >= overlapthres;

}
#endif /* LONG_VERIFICATION */

#endif /* SSJ_VERIFY_H */
