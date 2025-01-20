#ifndef SSJ_INV_INDEX_H
#define SSJ_INV_INDEX_H

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

class InvIndexHashMap {
	public:
		template<typename list_type, typename token_type=unsigned int>
		class IndexMap {
			private:
				// typedef google::dense_hash_map<unsigned int, list_type, std::tr1::hash<token_type> > Index;
				typedef google::dense_hash_map<unsigned int, list_type, std::hash<token_type> > Index;
				Index index;
			public:
				typedef list_type value_type;
				typedef typename Index::iterator iterator;


				IndexMap() {
					index.set_empty_key(INT_MAX);
				}

				inline list_type * get_list(token_type & token) {
					typename Index::iterator indit = index.find(token);
					if(indit == index.end()) {
						return NULL;
					} else {
						return &indit->second;
					}
				}

				inline list_type & get_list_create(token_type & token) {
					typedef std::pair<typename Index::iterator, bool> RetType;
					RetType retval = index.insert(std::pair<token_type, list_type>(token, list_type()));
					return retval.first->second;
				}

				inline void resize(typename Index::size_type newsize) {
				}

				inline iterator begin() {
					return index.begin();
				}

				inline iterator end() {
					return index.end();
				}

				static inline value_type & it2value(iterator & it) {
					return it->second;
				}
		};
};

class InvIndexVector {
	public:
		template<typename list_type>
		class IndexMap {
			private:
				typedef std::vector<list_type> Index;
				Index index;
			public:
				typedef unsigned int token_type;
				typedef list_type value_type;
				typedef typename Index::iterator iterator;


				inline list_type * get_list(token_type & token) {
					return &index[token];
				}

				inline list_type & get_list_create(token_type & token) {
					return index[token];
				}

				inline void resize(typename Index::size_type newsize) {
					index.resize(newsize);
				}

				inline iterator begin() {
					return index.begin();
				}

				inline iterator end() {
					return index.end();
				}

				static inline value_type & it2value(iterator & it) {
					return *it;
				}
		};
};

#endif
