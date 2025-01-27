#ifndef SSJ_OUTPUT_H
#define SSJ_OUTPUT_H

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

#include "classes.h"

/* We rely on Subclassing instead of template-trickery in this case because
 * the time to call the addPair method is the same for all algorithms.
 * Optimizing here would therefore not change the relative timings of the
 * algorithms, it may only (slightly) influence the absolute timings.
 */

class HandleOutput {
	public:
		virtual void addPair(const IntRecord & rec1, const IntRecord & rec2) = 0;
		virtual unsigned long getCount() const = 0;
		virtual ~HandleOutput() {}
};

class HandleOutputCount : public HandleOutput {
	public:
		unsigned long outputsize;
		HandleOutputCount() : outputsize(0) {}
		void addPair(const IntRecord & rec1, const IntRecord & rec2) {
			++outputsize;
		}
		unsigned long getCount() const { return outputsize;}
};

class HandleOutputPairs : public HandleOutput {
	public:
		typedef typename IntRecord::recordid_type recordid_type;
		typedef std::pair<recordid_type, recordid_type> outputelement_type;
		typedef std::vector<outputelement_type > output_type;
		output_type output;
		HandleOutputPairs() {}
		void addPair(const IntRecord & rec1, const IntRecord & rec2) {
			output.push_back(outputelement_type(rec1.recordid, rec2.recordid));
		}
		const output_type & getOutput() const {
			return output;
		}
		unsigned long getCount() const { return output.size();}
};

#endif
