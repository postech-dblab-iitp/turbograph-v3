
// Generated from Cypher.g4 by ANTLR 4.13.1

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
    DESCENDING = 75, DESC = 76, WHERE = 77, SHORTESTPATH = 78, ALLSHORTESTPATHS = 79, 
    RANGE = 80, OR = 81, XOR = 82, AND = 83, NOT = 84, INVALID_NOT_EQUAL = 85, 
    MINUS = 86, FACTORIAL = 87, IN = 88, STARTS = 89, ENDS = 90, CONTAINS = 91, 
    IS = 92, NULL_ = 93, TRUE = 94, FALSE = 95, EXISTS = 96, CASE = 97, 
    ELSE = 98, END = 99, WHEN = 100, THEN = 101, StringLiteral = 102, EscapedChar = 103, 
    DecimalInteger = 104, HexLetter = 105, HexDigit = 106, Digit = 107, 
    NonZeroDigit = 108, NonZeroOctDigit = 109, ZeroDigit = 110, RegularDecimalReal = 111, 
    UnescapedSymbolicName = 112, IdentifierStart = 113, IdentifierPart = 114, 
    EscapedSymbolicName = 115, SP = 116, WHITESPACE = 117, Comment = 118, 
    Unknown = 119
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

