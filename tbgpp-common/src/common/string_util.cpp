#include "common/string_util.hpp"
#include "common/pair.hpp"
#include "common/to_string.hpp"
#include "common/string_util.hpp"
#include "common/exception.hpp"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>

namespace s62 {

uint8_t ascii_to_upper_map[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
    22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
    44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,
    66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,
    88,  89,  90,  91,  92,  93,  94,  95,  96,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,
    78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  123, 124, 125, 126, 127, 128, 129, 130, 131,
    132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
    154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
    198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
    220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
    242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254};
uint8_t ascii_to_lower_map[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,
    22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,
    44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  97,
    98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
    132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
    154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
    198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
    220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
    242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254};

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

void StringUtil::LTrim(string &str) {
	auto it = str.begin();
	while (CharacterIsSpace(*it)) {
		it++;
	}
	str.erase(str.begin(), it);
}

// Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
void StringUtil::RTrim(string &str) {
	str.erase(find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !CharacterIsSpace(ch); }).base(),
	          str.end());
}

void StringUtil::Trim(string &str) {
	StringUtil::LTrim(str);
	StringUtil::RTrim(str);
}

bool StringUtil::StartsWith(string str, string prefix) {
	if (prefix.size() > str.size()) {
		return false;
	}
	return equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const string &str, const string &suffix) {
	if (suffix.size() > str.size()) {
		return false;
	}
	return equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

string StringUtil::Repeat(const string &str, idx_t n) {
	std::ostringstream os;
	for (idx_t i = 0; i < n; i++) {
		os << str;
	}
	return (os.str());
}

vector<string> StringUtil::Split(const string &str, char delimiter) {
	std::stringstream ss(str);
	vector<string> lines;
	string temp;
	while (getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	}
	return (lines);
}

namespace string_util_internal {

inline void SkipSpaces(const string &str, idx_t &index) {
	while (index < str.size() && std::isspace(str[index])) {
		index++;
	}
}

inline void ConsumeLetter(const string &str, idx_t &index, char expected) {
	if (index >= str.size() || str[index] != expected) {
		throw ParserException("Invalid quoted list: %s", str);
	}

	index++;
}

template <typename F>
inline void TakeWhile(const string &str, idx_t &index, const F &cond, string &taker) {
	while (index < str.size() && cond(str[index])) {
		taker.push_back(str[index]);
		index++;
	}
}

inline string TakePossiblyQuotedItem(const string &str, idx_t &index, char delimiter, char quote) {
	string entry;

	if (str[index] == quote) {
		index++;
		TakeWhile(
		    str, index, [quote](char c) { return c != quote; }, entry);
		ConsumeLetter(str, index, quote);
	} else {
		TakeWhile(
		    str, index, [delimiter, quote](char c) { return c != delimiter && c != quote && !std::isspace(c); }, entry);
	}

	return entry;
}

} // namespace string_util_internal

vector<string> StringUtil::SplitWithQuote(const string &str, char delimiter, char quote) {
	vector<string> entries;
	idx_t i = 0;

	string_util_internal::SkipSpaces(str, i);
	while (i < str.size()) {
		if (!entries.empty()) {
			string_util_internal::ConsumeLetter(str, i, delimiter);
		}

		entries.emplace_back(string_util_internal::TakePossiblyQuotedItem(str, i, delimiter, quote));
		string_util_internal::SkipSpaces(str, i);
	}

	return entries;
}

string StringUtil::Join(const vector<string> &input, const string &separator) {
	return StringUtil::Join(input, input.size(), separator, [](const string &s) { return s; });
}

string StringUtil::BytesToHumanReadableString(idx_t bytes) {
	string db_size;
	auto kilobytes = bytes / 1000;
	auto megabytes = kilobytes / 1000;
	kilobytes -= megabytes * 1000;
	auto gigabytes = megabytes / 1000;
	megabytes -= gigabytes * 1000;
	auto terabytes = gigabytes / 1000;
	gigabytes -= terabytes * 1000;
	if (terabytes > 0) {
		return to_string(terabytes) + "." + to_string(gigabytes / 100) + "TB";
	} else if (gigabytes > 0) {
		return to_string(gigabytes) + "." + to_string(megabytes / 100) + "GB";
	} else if (megabytes > 0) {
		return to_string(megabytes) + "." + to_string(kilobytes / 100) + "MB";
	} else if (kilobytes > 0) {
		return to_string(kilobytes) + "KB";
	} else {
		return to_string(bytes) + " bytes";
	}
}

string StringUtil::Upper(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::toupper(c); });
	return (copy);
}

string StringUtil::Lower(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::tolower(c); });
	return (copy);
}

vector<string> StringUtil::Split(const string &input, const string &split) {
	vector<string> splits;

	idx_t last = 0;
	idx_t input_len = input.size();
	idx_t split_len = split.size();
	while (last <= input_len) {
		idx_t next = input.find(split, last);
		if (next == string::npos) {
			next = input_len;
		}

		// Push the substring [last, next) on to splits
		string substr = input.substr(last, next - last);
		if (substr.empty() == false) {
			splits.push_back(substr);
		}
		last = next + split_len;
	}
	return splits;
}

string StringUtil::Replace(string source, const string &from, const string &to) {
	idx_t start_pos = 0;
	while ((start_pos = source.find(from, start_pos)) != string::npos) {
		source.replace(start_pos, from.length(), to);
		start_pos += to.length(); // In case 'to' contains 'from', like
		                          // replacing 'x' with 'yx'
	}
	return source;
}

vector<string> StringUtil::TopNStrings(vector<pair<string, idx_t>> scores, idx_t n, idx_t threshold) {
	if (scores.empty()) {
		return vector<string>();
	}
	sort(scores.begin(), scores.end(),
	     [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) -> bool { return a.second < b.second; });
	vector<string> result;
	result.push_back(scores[0].first);
	for (idx_t i = 1; i < MinValue<idx_t>(scores.size(), n); i++) {
		if (scores[i].second > threshold) {
			break;
		}
		result.push_back(scores[i].first);
	}
	return result;
}

struct LevenshteinArray {
	LevenshteinArray(idx_t len1, idx_t len2) : len1(len1) {
		dist = unique_ptr<idx_t[]>(new idx_t[len1 * len2]);
	}

	idx_t &Score(idx_t i, idx_t j) {
		return dist[GetIndex(i, j)];
	}

private:
	idx_t len1;
	unique_ptr<idx_t[]> dist;

	idx_t GetIndex(idx_t i, idx_t j) {
		return j * len1 + i;
	}
};

// adapted from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C++
idx_t StringUtil::LevenshteinDistance(const string &s1, const string &s2) {
	idx_t len1 = s1.size();
	idx_t len2 = s2.size();
	if (len1 == 0) {
		return len2;
	}
	if (len2 == 0) {
		return len1;
	}
	LevenshteinArray array(len1 + 1, len2 + 1);
	array.Score(0, 0) = 0;
	for (idx_t i = 0; i <= len1; i++) {
		array.Score(i, 0) = i;
	}
	for (idx_t j = 0; j <= len2; j++) {
		array.Score(0, j) = j;
	}
	for (idx_t i = 1; i <= len1; i++) {
		for (idx_t j = 1; j <= len2; j++) {
			// d[i][j] = std::min({ d[i - 1][j] + 1,
			//                      d[i][j - 1] + 1,
			//                      d[i - 1][j - 1] + (s1[i - 1] == s2[j - 1] ? 0 : 1) });
			int equal = s1[i - 1] == s2[j - 1] ? 0 : 1;
			idx_t adjacent_score1 = array.Score(i - 1, j) + 1;
			idx_t adjacent_score2 = array.Score(i, j - 1) + 1;
			idx_t adjacent_score3 = array.Score(i - 1, j - 1) + equal;

			idx_t t = MinValue<idx_t>(adjacent_score1, adjacent_score2);
			array.Score(i, j) = MinValue<idx_t>(t, adjacent_score3);
		}
	}
	return array.Score(len1, len2);
}

vector<string> StringUtil::TopNLevenshtein(const vector<string> &strings, const string &target, idx_t n,
                                           idx_t threshold) {
	vector<pair<string, idx_t>> scores;
	scores.reserve(strings.size());
	for (auto &str : strings) {
		scores.emplace_back(str, LevenshteinDistance(str, target));
	}
	return TopNStrings(scores, n, threshold);
}

string StringUtil::CandidatesMessage(const vector<string> &candidates, const string &candidate) {
	string result_str;
	if (!candidates.empty()) {
		result_str = "\n" + candidate + ": ";
		for (idx_t i = 0; i < candidates.size(); i++) {
			if (i > 0) {
				result_str += ", ";
			}
			result_str += "\"" + candidates[i] + "\"";
		}
	}
	return result_str;
}

string StringUtil::CandidatesErrorMessage(const vector<string> &strings, const string &target,
                                          const string &message_prefix, idx_t n) {
	auto closest_strings = StringUtil::TopNLevenshtein(strings, target, n);
	return StringUtil::CandidatesMessage(closest_strings, message_prefix);
}

// Jenkins hash function: https://en.wikipedia.org/wiki/Jenkins_hash_function
uint64_t StringUtil::CIHash(const string &str) {
	uint32_t hash = 0;
	for (auto c : str) {
		hash += StringUtil::CharacterToLower(c);
		hash += hash << 10;
		hash ^= hash >> 6;
	}
	hash += hash << 3;
	hash ^= hash >> 11;
	hash += hash << 15;
	return hash;
}

bool StringUtil::CIEquals(const string &l1, const string &l2) {
	if (l1.size() != l2.size()) {
		return false;
	}
	const auto charmap = ascii_to_lower_map;
	for (idx_t c = 0; c < l1.size(); c++) {
		if (charmap[(uint8_t)l1[c]] != charmap[(uint8_t)l2[c]]) {
			return false;
		}
	}
	return true;
}

bool StringUtil::CILessThan(const string &s1, const string &s2) {
	const auto charmap = ascii_to_upper_map;

	unsigned char u1, u2;

	idx_t length = MinValue<idx_t>(s1.length(), s2.length());
	length += s1.length() != s2.length();
	for (idx_t i = 0; i < length; i++) {
		u1 = (unsigned char)s1[i];
		u2 = (unsigned char)s2[i];
		if (charmap[u1] != charmap[u2]) {
			break;
		}
	}
	return (charmap[u1] - charmap[u2]) < 0;
}

} // namespace s62
