#include "parser/keyword_helper.hpp"
#include "parser/parser.hpp"
#include "common/string_util.hpp"

namespace s62 {

bool KeywordHelper::IsKeyword(const string &text) {
	return Parser::IsKeyword(text);
}

bool KeywordHelper::RequiresQuotes(const string &text) {
	for (size_t i = 0; i < text.size(); i++) {
		if (i > 0 && (text[i] >= '0' && text[i] <= '9')) {
			continue;
		}
		if (text[i] >= 'a' && text[i] <= 'z') {
			continue;
		}
		if (text[i] == '_') {
			continue;
		}
		return true;
	}
	return IsKeyword(text);
}

string KeywordHelper::WriteOptionallyQuoted(const string &text, char quote) {
	if (!RequiresQuotes(text)) {
		return text;
	}
	return string(1, quote) + StringUtil::Replace(text, string(1, quote), string(2, quote)) + string(1, quote);
}

} // namespace s62
