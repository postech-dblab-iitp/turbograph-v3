#ifndef SSJ_DATA_H
#define SSJ_DATA_H

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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <algorithm>
#include <climits>
#include <ostream>
#include <fstream>
#include "sparsehash/dense_hash_map"
#include "classes.h"

#define LINEBUFSIZE 8192

struct tokenize_whitespace {
	const char * p;
	const char * b;
	bool isend;
	char curtoken[LINEBUFSIZE];

	inline tokenize_whitespace() : p(NULL), b(NULL), isend(true) {}

	void setline(const char * line) {
		p = line;
		while(*p == ' ' || *p == '\t' || *p == '\n') {
			p += 1;
		}
		b = p;
		isend = *p == 0;
	}	

	char inline * next() {
		bool copied = false;
		while(true) {
			if(*p == ' ' || *p == '\t' || *p == 0 || *p == '\n') {
				if(p != b) {
					size_t len = std::min<size_t>(LINEBUFSIZE - 1, p-b);
					memcpy(curtoken, b, len);
					*(curtoken + len) = 0;
					copied = true;
				}
				if(*p == 0) {
					isend = true;
					break;
				}

				p  += 1;
				b = p;
				continue;

			} else if(copied) {
				break;
			}

			++p;
		}
		return curtoken;
	}

	inline bool end() { return isend; }
};

struct tokenize_qgram {
	const char * linep;
	unsigned int endhashstart;
	bool isend;
	const unsigned int gramlen;
	char curtoken[LINEBUFSIZE];

	inline tokenize_qgram(unsigned int gramlen) : linep(NULL),
	                                              isend(true), gramlen(gramlen)
	{
		assert(gramlen < LINEBUFSIZE);
	}

	void setline(const char * line) {
		isend = false;
		linep = line;
		endhashstart = gramlen - 1;
		memset(curtoken, '#', gramlen);
		*(curtoken + gramlen) = 0;
	}
	
	const char inline * next() {
		assert(!isend);
		for(unsigned int i = 0; i < gramlen - 1; ++i) {
			*(curtoken + i) = *(curtoken + i + 1);
		}
		if(*linep != 0) {
			*(curtoken + gramlen - 1) = *linep;
			linep += 1;
		} else {
			*(curtoken + gramlen - 1) = '#';
			endhashstart -= 1;
		}
		isend = endhashstart == 0;
		
		return curtoken;
	}

	inline bool end() { return isend; }
};
struct tokencount {
	std::string token;
	unsigned int count;
	tokencount(const std::string & token, unsigned int count = 1) : token(token), count(count) {}
	tokencount() {}
};


struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
  }
  bool operator()(const std::string & s1, const std::string & s2) const
  {
    return (s1.c_str() == s2.c_str()) || (s1 == s2);
  }
};

struct eqtokcount
{
  bool operator()(const tokencount & s1, const tokencount & s2) const
  {
    return (&s1 == &s2) || (s1.token == s2.token && s1.count == s2.count);
  }
};

struct hashtokencount {
	size_t operator()(const tokencount & s1) const
  {
    // return std::tr1::hash<std::string>()(s1.token) ^ std::tr1::hash<unsigned int>()(s1.count);
	return std::hash<std::string>()(s1.token) ^ std::hash<unsigned int>()(s1.count);
  }
};


typedef std::vector<std::string> tokenvector;
// typedef google::dense_hash_map<std::string, unsigned int, std::tr1::hash<std::string>, eqstr> tokencounterhash;
typedef google::dense_hash_map<std::string, unsigned int, std::hash<std::string>, eqstr> tokencounterhash;
typedef google::dense_hash_map<tokencount, unsigned int, hashtokencount, eqtokcount> tokencountcounterhash;

// Update tokencounter map which counts the number of times a particular token has been seen
struct update_counting_line {

	tokencounterhash tokenpersetcounter;
	
	update_counting_line() {
		tokenpersetcounter.set_empty_key(std::string(""));
	}
	
	void inline update(const char * token, tokencountcounterhash & tokencounter) {

		std::string tokenstr(token);
		tokencounterhash::iterator it = tokenpersetcounter.find(tokenstr);
		tokencount tc(tokenstr);
		if(it != tokenpersetcounter.end()) {
			it->second += 1;
			tc.count = it->second;
		} else {
			tokenpersetcounter[tokenstr] = 1;
		}
		
		tokencounter[tc]++;
	}
};

// Update tokencounter map which counts the number of times a particular token has been seen
struct add_int_records {

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

	tokencounterhash tokenpersetcounter;
	IntRecord currecord;
	unsigned int inc;

	Algorithm * algo;


	add_int_records( Algorithm * algo) : inc(1), algo(algo) {
		tokenpersetcounter.set_empty_key(std::string(""));
		//Push a token that occurs 0 times in indexed sets (for foreign join)
	}
	
	void inline update(const char * token, tokencountcounterhash & tokencounter) {

		std::string tokenstr(token);
		tokencounterhash::iterator it = tokenpersetcounter.find(tokenstr);
		tokencount tc(tokenstr);
		if(it != tokenpersetcounter.end()) {
			it->second += 1;
			tc.count = it->second;
		} else {
			tokenpersetcounter[tokenstr] = 1;
		}
		
		tokencountcounterhash::iterator tcit = tokencounter.find(tc);
		unsigned int tokintval;
		if(tcit != tokencounter.end()) {
			tokintval = tcit->second;
		} else {
			tokintval = inc;
			tokencounter[tc] = inc;
			inc += 1;
		}
		currecord.tokens.push_back(tokintval);
	}

	void inline nextrecord() {
		algo->addrecord(currecord);
		tokenpersetcounter.clear();
	}

	void assign(const char * token, tokencountcounterhash & tokencounter) {
		std::string tokenstr(token);
		tokencounterhash::iterator it = tokenpersetcounter.find(tokenstr);
		tokencount tc(tokenstr);
		if(it != tokenpersetcounter.end()) {
			it->second += 1;
			tc.count = it->second;
		} else {
			tokenpersetcounter[tokenstr] = 1;
		}

		tokencountcounterhash::iterator tcit = tokencounter.find(tc);
		unsigned int tokintval = 0;
		if(tcit != tokencounter.end()) {
			tokintval = tcit->second;
		}
		currecord.tokens.push_back(tokintval);

	}
	void inline nextforeignrecord() {
		algo->addforeignrecord(currecord);
		tokenpersetcounter.clear();
	}
};


struct tokisless {
	inline bool operator()(const tokencountcounterhash::iterator & it1, const tokencountcounterhash::iterator & it2) const {
		return it1->second < it2->second;
	}
};

inline void token2int(tokencountcounterhash & tokencount) {
	std::vector<tokencountcounterhash::iterator> tokensorter;
	tokensorter.reserve(tokencount.size());
	tokencountcounterhash::iterator it = tokencount.begin();
	for(; it != tokencount.end(); ++it) {
		tokensorter.push_back(it);
	}
	std::sort(tokensorter.begin(), tokensorter.end(), tokisless());

	unsigned myint = 0;
	std::vector<tokencountcounterhash::iterator>::iterator vit = tokensorter.begin();
	for( ; vit != tokensorter.end(); ++vit, ++myint) {
		(*vit)->second = myint;
	}
	 
}

struct collect_sets_line {
	Algorithm * algo;
	tokencountcounterhash & tokencounter;
	tokencounterhash tokenpersetcounter;
	IntRecord record;
		
	collect_sets_line(Algorithm * algo, tokencountcounterhash & tokencounter) :
		algo(algo), tokencounter(tokencounter)
	{	
		tokenpersetcounter.set_empty_key(std::string(""));
	}

	void inline update(const char * token) {

		std::string strtoken(token);
		tokencounterhash::iterator it = tokenpersetcounter.find(strtoken);
		tokencount tc(strtoken);
		if(it != tokenpersetcounter.end()) {
			it->second += 1;
			tc.count = it->second;
		} else {
			tokenpersetcounter[strtoken] = 1;
		}
		
		record.tokens.push_back(tokencounter[tc]);
	}

	~collect_sets_line() {
		algo->addrecord(record);
	}
};


// OUTPUT
//
struct print_pairs_lf {

	template<typename I>
	void inline operator()(std::ostream & os, const I & iterator) const {
		os << " (" << iterator->first << ", " << iterator->second << ")" << std::endl;
	}
};

struct print_intrecords_lf {

	template <class I>
	void inline operator()(std::ostream & os, const I & iterator) const {
		typename I::value_type::Tokens::const_iterator tokit = iterator->tokens.begin();
		for(; tokit != iterator->tokens.end(); ++tokit) {
			os << *tokit << " ";
		}
		os << std::endl;
	}
};


template<typename P, typename R>
void inline print_result(const std::string & resultfile, const R & result, const P & prnt) {
	std::ofstream of;
	std::ostream * os = &std::cout;
	if(resultfile != "-") {
		of.open(resultfile, std::ofstream::out);
		os = &of;
	} else {
		*os << "Results:" << std::endl;
	}
	typename R::const_iterator resit = result.begin();
	for( ; resit != result.end(); ++resit) {
		prnt(*os, resit);
	}
	if(os != &std::cout) {
		of.close();
	}
}


#endif
