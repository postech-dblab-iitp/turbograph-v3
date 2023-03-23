
// Generated from Cypher.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    COPY = 45, FROM = 46, NODE = 47, TABLE = 48, DROP = 49, PRIMARY = 50, 
    KEY = 51, REL = 52, TO = 53, EXPLAIN = 54, PROFILE = 55, UNION = 56, 
    ALL = 57, OPTIONAL = 58, MATCH = 59, UNWIND = 60, CREATE = 61, SET = 62, 
    DELETE = 63, WITH = 64, RETURN = 65, DISTINCT = 66, STAR = 67, AS = 68, 
    ORDER = 69, BY = 70, L_SKIP = 71, LIMIT = 72, ASCENDING = 73, ASC = 74, 
    DESCENDING = 75, DESC = 76, WHERE = 77, RANGE = 78, OR = 79, XOR = 80, 
    AND = 81, NOT = 82, INVALID_NOT_EQUAL = 83, MINUS = 84, FACTORIAL = 85, 
    STARTS = 86, ENDS = 87, CONTAINS = 88, IS = 89, NULL_ = 90, TRUE = 91, 
    FALSE = 92, EXISTS = 93, CASE = 94, ELSE = 95, END = 96, WHEN = 97, 
    THEN = 98, StringLiteral = 99, EscapedChar = 100, DecimalInteger = 101, 
    HexLetter = 102, HexDigit = 103, Digit = 104, NonZeroDigit = 105, NonZeroOctDigit = 106, 
    ZeroDigit = 107, RegularDecimalReal = 108, UnescapedSymbolicName = 109, 
    IdentifierStart = 110, IdentifierPart = 111, EscapedSymbolicName = 112, 
    SP = 113, WHITESPACE = 114, Comment = 115, Unknown = 116
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

