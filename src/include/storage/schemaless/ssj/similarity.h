#ifndef SSJ_SIMILARITY_H
#define SSJ_SIMILARITY_H

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

#include <cmath>

#define PMAXSIZE_EPS 1e-10

template <typename Similarity>
class GenericSimilarity {
	public:
		typedef typename Similarity::threshold_type threshold_type;

		inline static unsigned int maxprefix(unsigned int len, threshold_type threshold) {
			return std::min(len, len - minsize(len, threshold) + 1);
		}

		inline static unsigned int midprefix(unsigned int len, threshold_type threshold) {
			return std::min(len, len - minoverlap(len, len, threshold) + 1);
		}
		
		inline static unsigned int minoverlap(unsigned int len1, unsigned int len2, threshold_type threshold) {
			return std::min(len2, std::min(len1, Similarity::minoverlap(len1, len2, threshold)));
		}

		inline static unsigned int minsize(unsigned int len, threshold_type threshold) {
			return Similarity::minsize(len, threshold);
		}

		inline static unsigned int maxsize(unsigned int len, threshold_type threshold) {
			return Similarity::maxsize(len, threshold);
		}

		inline static unsigned int maxsize(unsigned int len, unsigned int pos, threshold_type threshold) {
			return Similarity::maxsize(len, pos, threshold);
		}
		
		inline static int outputall_le(threshold_type threshold) {
			return Similarity::outputall_le(threshold);
		}
};

class JaccardSimilarity {
	public:
		typedef double threshold_type;
		inline static unsigned int minoverlap(unsigned int len1, unsigned int len2, double threshold) {
			return (unsigned int)(ceil((len1 + len2) * threshold / (1 + threshold)));
		}
		
		inline static unsigned int minsize(unsigned int len, double threshold) {
			return (unsigned int)(ceil(threshold * len));
		}
		
		inline static unsigned int maxsize(unsigned int len, double threshold) {
			return (unsigned int)((len / threshold));
		}
		
		inline static unsigned int maxsize(unsigned int len, unsigned int pos, double threshold) {
			return (unsigned int)((len - ((1.0 - PMAXSIZE_EPS) + threshold) * pos) / threshold);
		}

		inline static int outputall_le(threshold_type threshold) {
			return -1;
		}
		
};

class CosineSimilarity {
	public:
		typedef double threshold_type;
		inline static unsigned int minoverlap(unsigned int len1, unsigned int len2, double threshold) {
			return (unsigned int)ceil(threshold * sqrt(len1 * len2));
		}
		
		inline static unsigned int minsize(unsigned int len, double threshold) {
			return (unsigned int)(ceil(threshold * threshold * len));
		}
		
		inline static unsigned int maxsize(unsigned int len, double threshold) {
			return (unsigned int)(len / (threshold * threshold));
		}
		
		inline static unsigned int maxsize(unsigned int len, unsigned int pos, double threshold) {
			return (unsigned int)(PMAXSIZE_EPS + (len * len - 2 * len * pos + pos * pos)/(len * threshold * threshold));
		}

		inline static int outputall_le(threshold_type threshold) {
			return -1;
		}
		
};

class DiceSimilarity {
	public:
		typedef double threshold_type;
		inline static unsigned int minoverlap(unsigned int len1, unsigned int len2, double threshold) {
			return (unsigned int)ceil(threshold * (len1 + len2) / 2);
		}
		
		inline static unsigned int minsize(unsigned int len, double threshold) {
			return (unsigned int)(ceil(threshold * len / ( 2 - threshold )));
		}
		
		inline static unsigned int maxsize(unsigned int len, double threshold) {
			return (unsigned int)((2 - threshold) * len / threshold);
		}
		
		inline static unsigned int maxsize(unsigned int len, unsigned int pos, double threshold) {
			return (unsigned int)(((2 - threshold) * len - (2 - PMAXSIZE_EPS) * pos) / threshold);
		}

		inline static int outputall_le(threshold_type threshold) {
			return -1;
		}
		
};

class HammingSimilarity {
	public:
		typedef unsigned int threshold_type;
		inline static unsigned int minoverlap(unsigned int len1, unsigned int len2, threshold_type threshold) {
			//Ensure that minoverlap is at least 1
			if(len1 + len2 > threshold) {
				// the + 1 is there to avoid a float cast and a ceil on the whole term
				return (len1 + len2 - threshold + 1) / 2;
			} else {
				return 1;
			}
		}

		inline static unsigned int minsize(unsigned int len, threshold_type threshold) {
			return len > threshold ? len - threshold : 1;
		}

		inline static unsigned int maxsize(unsigned int len, threshold_type threshold) {
			return len + threshold;
		}

		inline static unsigned int maxsize(unsigned int len, unsigned int pos, threshold_type threshold) {
			return len + threshold - 2 * pos;
		}

		inline static int outputall_le(threshold_type threshold) {
			return threshold;
		}

};


typedef GenericSimilarity<JaccardSimilarity> Jaccard;
typedef GenericSimilarity<CosineSimilarity> Cosine;
typedef GenericSimilarity<DiceSimilarity> Dice;
typedef GenericSimilarity<HammingSimilarity> Hamming;

#endif
