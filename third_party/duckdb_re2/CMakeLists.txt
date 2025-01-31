# Copyright 2015 The RE2 Authors.  All Rights Reserved. Use of this source code
# is governed by a BSD-style license that can be found in the LICENSE file.

cmake_minimum_required(VERSION 3.5...3.29)

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

project(RE2 CXX)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include(CTest)

# CMake seems to have no way to enable/disable testing per subproject, so we
# provide an option similar to BUILD_TESTING, but just for RE2.
option(RE2_BUILD_TESTING "enable testing for RE2" OFF)

set(EXTRA_TARGET_LINK_LIBRARIES)

add_compile_options(-std=c++11)

add_definitions(-DRE2_ON_VALGRIND)

set(RE2_SOURCES

    duckdb_re2/bitmap256.cc
    duckdb_re2/compile.cc
    duckdb_re2/bitstate.cc
    duckdb_re2/dfa.cc
    duckdb_re2/filtered_re2.cc
    duckdb_re2/mimics_pcre.cc
    duckdb_re2/nfa.cc
    duckdb_re2/onepass.cc
    duckdb_re2/parse.cc
    duckdb_re2/perl_groups.cc
    duckdb_re2/prefilter.cc
    duckdb_re2/prefilter_tree.cc
    duckdb_re2/prog.cc
    duckdb_re2/re2.cc
    duckdb_re2/regexp.cc
    duckdb_re2/set.cc
    duckdb_re2/simplify.cc
    duckdb_re2/stringpiece.cc
    duckdb_re2/tostring.cc
    duckdb_re2/unicode_casefold.cc
    duckdb_re2/unicode_groups.cc
    util/rune.cc
    util/strutil.cc
)

add_library(duckdb_re2 STATIC ${RE2_SOURCES})

target_include_directories(
  duckdb_re2
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})