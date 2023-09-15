#ifndef SSJ_LENGTH_FILTER_H
#define SSJ_LENGTH_FILTER_H

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


class DefaultLengthFilterPolicy {
	public:
		enum {
			POS = false
		};

		inline static unsigned int verify_record_start(unsigned int reclen, unsigned int maxprefix, unsigned int minoverlap) {
			return maxprefix;
		}

		inline static bool posfilter(unsigned int reclen, unsigned int indreclen,
				unsigned int recpos, unsigned int indrecpos,
				unsigned int minoverlap) {
			return minoverlap <= std::min(reclen - recpos, indreclen - indrecpos);
		}
};

class PlJoinLengthFilterPolicy {
	public:
		enum {
			POS = true
		};

		inline static unsigned int verify_record_start(unsigned int reclen, unsigned int maxprefix, unsigned int minoverlap) {
			return reclen - minoverlap + 1;
		}

		inline static bool posfilter(unsigned int reclen, unsigned int indreclen,
				unsigned int recpos, unsigned int indrecpos,
				unsigned int minoverlap) {
			return minoverlap <= indreclen - indrecpos;
		}
};

#endif
