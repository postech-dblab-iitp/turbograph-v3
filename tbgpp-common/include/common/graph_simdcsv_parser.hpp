#pragma once

#include "common/common.hpp"
#include "common/assert.hpp"
#include "common/unordered_map.hpp"
#include "common/types/data_chunk.hpp"
#include "common/enums/graph_component_type.hpp"
#include "common/types/date.hpp"
#include "third_party/csv-parser/csv.hpp"
#include <unistd.h> // for getopt

#include <iostream>
#include <vector>
// #include <string_view>
#include <charconv>

#include "common_defs.h"
#include "csv_defs.h"
#include "io_util.h"
#include "timing.h"
#include "mem_util.h"
#include "portability.h"

using namespace turbograph_simdcsv;

namespace duckdb {

struct ParsedCSV {
  uint64_t n_indexes{0};
  uint64_t *indexes;
};

struct simd_input {
#ifdef __AVX2__
  __m256i lo;
  __m256i hi;
#elif defined(__ARM_NEON)
  uint8x16_t i0;
  uint8x16_t i1;
  uint8x16_t i2;
  uint8x16_t i3;
#else
#error "It's called SIMDcsv for a reason, bro"
#endif
};

really_inline simd_input fill_input(const uint8_t * ptr) {
  struct simd_input in;
#ifdef __AVX2__
  in.lo = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 0));
  in.hi = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 32));
#elif defined(__ARM_NEON)
  in.i0 = vld1q_u8(ptr + 0);
  in.i1 = vld1q_u8(ptr + 16);
  in.i2 = vld1q_u8(ptr + 32);
  in.i3 = vld1q_u8(ptr + 48);
#endif
  return in;
}

// a straightforward comparison of a mask against input. 5 uops; would be
// cheaper in AVX512.
really_inline uint64_t cmp_mask_against_input(simd_input in, uint8_t m) {
#ifdef __AVX2__
  const __m256i mask = _mm256_set1_epi8(m);
  __m256i cmp_res_0 = _mm256_cmpeq_epi8(in.lo, mask);
  uint64_t res_0 = static_cast<uint32_t>(_mm256_movemask_epi8(cmp_res_0));
  __m256i cmp_res_1 = _mm256_cmpeq_epi8(in.hi, mask);
  uint64_t res_1 = _mm256_movemask_epi8(cmp_res_1);
  return res_0 | (res_1 << 32);
#elif defined(__ARM_NEON)
  const uint8x16_t mask = vmovq_n_u8(m); 
  uint8x16_t cmp_res_0 = vceqq_u8(in.i0, mask); 
  uint8x16_t cmp_res_1 = vceqq_u8(in.i1, mask); 
  uint8x16_t cmp_res_2 = vceqq_u8(in.i2, mask); 
  uint8x16_t cmp_res_3 = vceqq_u8(in.i3, mask); 
  return neonmovemask_bulk(cmp_res_0, cmp_res_1, cmp_res_2, cmp_res_3);
#endif
}


// return the quote mask (which is a half-open mask that covers the first
// quote in a quote pair and everything in the quote pair) 
// We also update the prev_iter_inside_quote value to
// tell the next iteration whether we finished the final iteration inside a
// quote pair; if so, this  inverts our behavior of  whether we're inside
// quotes for the next iteration.

really_inline uint64_t find_quote_mask(simd_input in, uint64_t &prev_iter_inside_quote) {
  uint64_t quote_bits = cmp_mask_against_input(in, '"');

#ifdef __AVX2__
  uint64_t quote_mask = _mm_cvtsi128_si64(_mm_clmulepi64_si128(
      _mm_set_epi64x(0ULL, quote_bits), _mm_set1_epi8(0xFF), 0));
#elif defined(__ARM_NEON)
  uint64_t quote_mask = vmull_p64( -1ULL, quote_bits);
#endif
  quote_mask ^= prev_iter_inside_quote;

  // right shift of a signed value expected to be well-defined and standard
  // compliant as of C++20,
  // John Regher from Utah U. says this is fine code
  prev_iter_inside_quote =
      static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);
  return quote_mask;
}


// flatten out values in 'bits' assuming that they are are to have values of idx
// plus their position in the bitvector, and store these indexes at
// base_ptr[base] incrementing base as we go
// will potentially store extra values beyond end of valid bits, so base_ptr
// needs to be large enough to handle this
really_inline void flatten_bits(uint64_t *base_ptr, uint64_t &base,
                                uint64_t idx, uint64_t bits) {
  if (bits != 0u) {
    uint64_t cnt = hamming(bits);
    uint64_t next_base = base + cnt;
    base_ptr[base + 0] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 1] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 2] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 3] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 4] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 5] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 6] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    base_ptr[base + 7] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
    bits = bits & (bits - 1);
    if (cnt > 8) {
      base_ptr[base + 8] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 9] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 10] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 11] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 12] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 13] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 14] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
      base_ptr[base + 15] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
      bits = bits & (bits - 1);
    }
    if (cnt > 16) {
      base += 16;
      do {
        base_ptr[base] = static_cast<uint64_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base++;
      } while (bits != 0);
    }
    base = next_base;
  }
}

//
// This optimization option might be helpful
// When it is OFF:
// $ ./simdcsv ../examples/nfl.csv
// Cycles per byte 0.694172
// GB/s: 4.26847
// When it is ON:
// $ ./simdcsv ../examples/nfl.csv
// Cycles per byte 0.55007
// GB/s: 5.29778
// Explanation: It slightly reduces cache misses, but that's probably irrelevant,
// However, it seems to improve drastically the number of instructions per cycle.
#define SIMDCSV_BUFFERING 
bool find_indexes(const uint8_t * buf, size_t len, ParsedCSV & pcsv) {
  // does the previous iteration end inside a double-quote pair?
  uint64_t prev_iter_inside_quote = 0ULL;  // either all zeros or all ones
#ifdef CRLF
  uint64_t prev_iter_cr_end = 0ULL; 
#endif
  size_t lenminus64 = len < 64 ? 0 : len - 64;
  size_t idx = 0;
  uint64_t *base_ptr = pcsv.indexes;
  uint64_t base = 0;
#ifdef SIMDCSV_BUFFERING
  // we do the index decoding in bulk for better pipelining.
#define SIMDCSV_BUFFERSIZE 4 // it seems to be about the sweetspot.
  if(lenminus64 > 64 * SIMDCSV_BUFFERSIZE) {
    uint64_t fields[SIMDCSV_BUFFERSIZE];
    for (; idx < lenminus64 - 64 * SIMDCSV_BUFFERSIZE + 1; idx += 64 * SIMDCSV_BUFFERSIZE) {
      for(size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++){
        size_t internal_idx = 64 * b + idx;
#ifndef _MSC_VER
        __builtin_prefetch(buf + internal_idx + 128);
#endif
        simd_input in = fill_input(buf+internal_idx);
        uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
        uint64_t sep = cmp_mask_against_input(in, '|');
#ifdef CRLF
        uint64_t cr = cmp_mask_against_input(in, 0x0d);
        uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
        uint64_t lf = cmp_mask_against_input(in, 0x0a);
        uint64_t end = lf & cr_adjusted;
        prev_iter_cr_end = cr >> 63;
#else
        uint64_t end = cmp_mask_against_input(in, 0x0a);
#endif
        fields[b] = (end | sep) & ~quote_mask;
      }
      for(size_t b = 0; b < SIMDCSV_BUFFERSIZE; b++){
        size_t internal_idx = 64 * b + idx;
        flatten_bits(base_ptr, base, internal_idx, fields[b]);
      }
    }
  }
  // tail end will be unbuffered
#endif // SIMDCSV_BUFFERING
  for (; idx < lenminus64; idx += 64) {
#ifndef _MSC_VER
      __builtin_prefetch(buf + idx + 128);
#endif
      simd_input in = fill_input(buf+idx);
      uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
      uint64_t sep = cmp_mask_against_input(in, '|');
#ifdef CRLF
      uint64_t cr = cmp_mask_against_input(in, 0x0d);
      uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
      uint64_t lf = cmp_mask_against_input(in, 0x0a);
      uint64_t end = lf & cr_adjusted;
      prev_iter_cr_end = cr >> 63;
#else
      uint64_t end = cmp_mask_against_input(in, 0x0a);
#endif
    // note - a bit of a high-wire act here with quotes
    // we can't put something inside the quotes with the CR
    // then outside the quotes with LF so it's OK to "and off"
    // the quoted bits here. Some other quote convention would
    // need to be thought about carefully
      uint64_t field_sep = (end | sep) & ~quote_mask;
      flatten_bits(base_ptr, base, idx, field_sep);
  }
#undef SIMDCSV_BUFFERSIZE
  pcsv.n_indexes = base;
  return true;
}

// inline Value CSVValToValue(csv::CSVField &val, LogicalType &type) {
// 	switch (type.id()) {
// 		case LogicalTypeId::BOOLEAN:
// 			return Value::BOOLEAN(val.get<bool>());
// 		case LogicalTypeId::TINYINT:
// 			return Value::TINYINT(val.get<int8_t>());
// 		case LogicalTypeId::SMALLINT:
// 			return Value::SMALLINT(val.get<int16_t>());
// 		case LogicalTypeId::INTEGER:
// 			return Value::INTEGER(val.get<int32_t>());
// 		case LogicalTypeId::BIGINT:
// 			return Value::BIGINT(val.get<int64_t>());
// 		case LogicalTypeId::UTINYINT:
// 			return Value::UTINYINT(val.get<uint8_t>());
// 		case LogicalTypeId::USMALLINT:
// 			return Value::USMALLINT(val.get<uint16_t>());
// 		case LogicalTypeId::UINTEGER:
// 			return Value::UINTEGER(val.get<uint32_t>());
// 		case LogicalTypeId::UBIGINT:
// 			return Value::UBIGINT(val.get<uint64_t>());
// 		case LogicalTypeId::HUGEINT:
// 			throw NotImplementedException("Do not support HugeInt");
// 		case LogicalTypeId::DECIMAL:
// 			throw NotImplementedException("Do not support Decimal");
// 		case LogicalTypeId::FLOAT:
// 			return Value::FLOAT(val.get<float>());
// 		case LogicalTypeId::DOUBLE:
// 			return Value::DOUBLE(val.get<double>());
// 		case LogicalTypeId::VARCHAR:
// 			return Value(val.get<>());
// 		default:
// 			throw NotImplementedException("Unsupported type");
// 	}
// }

inline void SetValueFromCSV(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
      D_ASSERT(false);
      break;
      // std::from_chars(p[start_offset], string_size, ((bool *)data_ptr)[current_index]); break;
			//((bool *)data_ptr)[current_index] = val.get<bool>(); break;
		case LogicalTypeId::TINYINT:
      std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::SMALLINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::INTEGER:
      // for (size_t j = start_offset; j < end_offset; j++) {
      //     std::cout << p[j];
      // }
      // std::cout << std::endl;
      std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::BIGINT:
      // for (size_t j = start_offset; j < end_offset; j++) {
      //     std::cout << p[j];
      // }
      // std::cout << std::endl;
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int64_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UTINYINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint8_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::USMALLINT:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint16_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UINTEGER:
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint32_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::UBIGINT:
    case LogicalTypeId::ADJLISTCOLUMN:
      // for (size_t j = start_offset; j < end_offset; j++) {
      //     std::cout << p[j];
      // }
      // std::cout << std::endl;
			std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint64_t *)data_ptr)[current_index]); break;
		case LogicalTypeId::HUGEINT:
			throw NotImplementedException("Do not support HugeInt"); break;
		case LogicalTypeId::DECIMAL:
      uint8_t width, scale;
      type.GetDecimalProperties(width, scale);
      switch(type.InternalType()) {
        case PhysicalType::INT16: {
          int16_t val_before_decimal_point, val_after_decimal_point;
          std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
          std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
          ((int16_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
          break;
        }
        case PhysicalType::INT32: {
          int32_t val_before_decimal_point, val_after_decimal_point;
          std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
          std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
          ((int32_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
          break;
        }
        case PhysicalType::INT64: {
          int64_t val_before_decimal_point, val_after_decimal_point;
          std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset - (scale - 1), val_before_decimal_point);
          std::from_chars((const char*)p.data() + (end_offset - scale), (const char*)p.data() + end_offset, val_after_decimal_point);
          ((int64_t *)data_ptr)[current_index] = (val_before_decimal_point * std::pow(10, scale)) + val_after_decimal_point;
          break;
        }
        case PhysicalType::INT128:
          hugeint_t val;
          throw NotImplementedException("Hugeint type for Decimal");
        default:
          throw InvalidInputException("Unsupported type for Decimal");
      }
      break;
		case LogicalTypeId::FLOAT:
      std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((float *)data_ptr)[current_index]); break;
		case LogicalTypeId::DOUBLE:
      std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((double *)data_ptr)[current_index]); break;
		case LogicalTypeId::VARCHAR:
      // for (size_t j = start_offset; j < end_offset; j++) {
      //     std::cout << p[j];
      // }
      // std::cout << std::endl;
			((string_t *)data_ptr)[current_index] = StringVector::AddStringOrBlob(output.data[i], (const char*)p.data() + start_offset, string_size); break;
    case LogicalTypeId::DATE:
      ((date_t *)data_ptr)[current_index] = Date::FromCString((const char*)p.data() + start_offset, end_offset - start_offset); break;
		default:
			throw NotImplementedException("SetValueFromCSV - Unsupported type");
	}
}

inline void SetValueFromCSV_TINYINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int8_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_SMALLINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int16_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_INTEGER(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int32_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_BIGINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((int64_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_UTINYINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint8_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_USMALLINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint16_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_UINTEGER(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint32_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_UBIGINT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((uint64_t *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_FLOAT(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((float *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_DOUBLE(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, ((double *)data_ptr)[current_index]);
}

inline void SetValueFromCSV_VARCHAR(LogicalType type, DataChunk &output, size_t i, idx_t current_index, 
                            std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset) {
	auto data_ptr = output.data[i].GetData();
  size_t string_size = end_offset - start_offset;
  ((string_t *)data_ptr)[current_index] = StringVector::AddStringOrBlob(output.data[i], (const char*)p.data() + start_offset, string_size);
}

template <typename A, typename B>
void zip(
    const std::vector<A> &a, 
    const std::vector<B> &b, 
    std::vector<std::pair<A,B>> &zipped)
{
    for(size_t i=0; i<a.size(); ++i)
    {
        zipped.push_back(std::make_pair(a[i], b[i]));
    }
}

template <typename A, typename B>
void unzip(
    const std::vector<std::pair<A, B>> &zipped, 
    std::vector<A> &a, 
    std::vector<B> &b)
{
    for(size_t i=0; i<a.size(); i++)
    {
        a[i] = zipped[i].first;
        b[i] = zipped[i].second;
    }
}

class GraphSIMDCSVFileParser {
  typedef void (*set_value_from_csv_func)(LogicalType type, DataChunk &output, size_t i, idx_t current_index, std::basic_string_view<uint8_t> &p, idx_t start_offset, idx_t end_offset);

public:
  GraphSIMDCSVFileParser() {}
  ~GraphSIMDCSVFileParser() {}

  size_t InitCSVFile(const char *csv_file_path, GraphComponentType type_, char delim) {
#ifdef __AVX2__
    fprintf(stdout, "AVX2 defined\n");
#endif
    type = type_;

    // Initialize CSV Reader & iterator
    try {
      p = get_corpus(csv_file_path, CSV_PADDING);
    } catch (const std::exception &e) { // caught by reference to base
      // std::cout << "Could not load the file " << filename << std::endl;
      throw InvalidInputException("Could not load the file");
      //return EXIT_FAILURE;
    }
    // Read only header.. TODO how to read only few lines or just a line?
    csv::CSVFormat csv_form;
      csv_form.delimiter('|')
              .header_row(0);
      csv::CSVReader *reader = new csv::CSVReader(csv_file_path, csv_form);

    // Parse CSV File
    pcsv.indexes = new (std::nothrow) uint64_t[p.size()]; // can't have more indexes than we have data
    if(pcsv.indexes == nullptr) {
      std::cerr << "You are running out of memory." << std::endl;
      // return EXIT_FAILURE;
    }
    find_indexes(p.data(), p.size(), pcsv);

    // Parse header
    vector<string> col_names = move(reader->get_col_names());
    num_columns = col_names.size();
    num_rows = pcsv.n_indexes / num_columns;
    fprintf(stdout, "n_indexes = %ld, num_columns = %ld\n", pcsv.n_indexes, num_columns);
    D_ASSERT(pcsv.n_indexes % num_columns == 0);
    for (size_t i = 0; i < col_names.size(); i++) {
      // Assume each element in the header column is of format 'key:type'
      std::string key_and_type = col_names[i]; 
      std::cout << "\t" << key_and_type << std::endl;
      size_t delim_pos = key_and_type.find(':');
      if (delim_pos == std::string::npos) throw InvalidInputException("D");
      std::string key = key_and_type.substr(0, delim_pos);
      if (key == "") {
        // special case
        std::string type_name = key_and_type.substr(delim_pos + 1);
        LogicalType type = move(StringToLogicalType(type_name, i));
        if (type_name.find("START_ID") != std::string::npos) {
          key_names.push_back(src_key_name + "_src");
        } else {
          key_names.push_back(dst_key_name + "_dst");
        }
        key_types.push_back(move(type));
      } else {
        std::string type_name = key_and_type.substr(delim_pos + 1);
        LogicalType type = move(StringToLogicalType(type_name, i));
        key_names.push_back(move(key));
        key_types.push_back(move(type));
      }
    }

    // Sort key columns vector
    if (key_columns_order.size() > 0) {
      vector<std::pair<int64_t, int64_t>> zipped;
      zip(key_columns, key_columns_order, zipped);

      std::sort(std::begin(zipped), std::end(zipped),
        [&](const auto &a, const auto&b) {
          return a.second < b.second;
        });
      
      unzip(zipped, key_columns, key_columns_order);
      for (size_t i = 0; i < key_columns_order.size(); i++) {
        std::cout << key_columns_order[i] << " : " << key_columns[i] << std::endl;
      }
    }

    // Initialize Cursor
    row_cursor = 1; // After the header
    index_cursor = num_columns;
    delete reader;

    return num_rows;
  }

	bool GetSchemaFromHeader(vector<string> &names, vector<LogicalType> &types) {
		D_ASSERT(names.empty() && types.empty());
		names.resize(key_names.size());
		types.resize(key_types.size());
		std::copy(key_names.begin(), key_names.end(), names.begin());
		std::copy(key_types.begin(), key_types.end(), types.begin());
    // for (int i = 0; i < types.size(); i++) {
    //   fprintf(stdout, "%d column(%s): %s -> %s\n", i, key_names[i].c_str(), key_types[i].ToString().c_str(), types[i].ToString().c_str());
    // }
		return true;
	}

	vector<int64_t> GetKeyColumnIndexFromHeader() {
		D_ASSERT(type == GraphComponentType::VERTEX);
		return key_columns;
	}

	void GetSrcColumnIndexFromHeader(int64_t &src_column_idx, string &src_column_name) {
		D_ASSERT(type == GraphComponentType::EDGE);
		src_column_idx = src_column;
		src_column_name = src_key_name;
		return;
	}

	void GetDstColumnIndexFromHeader(int64_t &dst_column_idx, string &dst_column_name) {
		D_ASSERT(type == GraphComponentType::EDGE);
		dst_column_idx = dst_column;
		dst_column_name = dst_key_name;
		return;
	}

	bool ReadCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
		D_ASSERT(required_keys.size() == types.size());
		D_ASSERT(required_keys.size() == output.ColumnCount());

		if (type == GraphComponentType::VERTEX) {
			return ReadVertexCSVFile(required_keys, types, output);
		} else if (type == GraphComponentType::EDGE) {
			return ReadEdgeCSVFile(required_keys, types, output);
		}
		return true;
	}

	bool ReadVertexCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
		if (row_cursor == num_rows) return true;

    idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
		for (auto &key: required_keys) {
			// Find keys in the schema and extract idxs
			auto key_it = std::find(key_names.begin(), key_names.end(), key);
			if (key_it != key_names.end()) {
				idx_t key_idx = key_it - key_names.begin();
				required_key_column_idxs.push_back(key_idx);
			} else {
				throw InvalidInputException("A");
			}
		}
    // What's the best? Cache miss vs branch prediction cost..

    // Row-oriented manner
    std::cout << "num_rows: " << num_rows << std::endl;
		for (; row_cursor < num_rows; row_cursor++) {
			if (current_index == STANDARD_VECTOR_SIZE) break;
			for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
        idx_t target_index = index_cursor + required_key_column_idxs[i];
        idx_t start_offset = pcsv.indexes[target_index-1] + 1;
        idx_t end_offset = pcsv.indexes[target_index];
				SetValueFromCSV(types[i], output, i, current_index, p, start_offset, end_offset);
			}
			current_index++;
      index_cursor += num_columns;
		}
    std::cout << "???\n";

    // Column-oriented manner
    // idx_t cur_base_row_cursor = row_cursor;
    // idx_t cur_base_index_cursor = index_cursor;
    // for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
    //   index_cursor = cur_base_index_cursor;
    //   set_value_from_csv_func set_func;
    //   current_index = 0;
    //   switch (types[i].id()) {
    //   case LogicalTypeId::BOOLEAN:
    //     D_ASSERT(false); break;
    //   case LogicalTypeId::TINYINT:
    //     set_func = SetValueFromCSV_TINYINT; break;
    //   case LogicalTypeId::SMALLINT:
    //     set_func = SetValueFromCSV_SMALLINT; break;
    //   case LogicalTypeId::INTEGER:
    //     set_func = SetValueFromCSV_INTEGER; break;
    //   case LogicalTypeId::BIGINT:
    //     set_func = SetValueFromCSV_BIGINT; break;
    //   case LogicalTypeId::UTINYINT:
    //     set_func = SetValueFromCSV_UTINYINT; break;
    //   case LogicalTypeId::USMALLINT:
    //     set_func = SetValueFromCSV_USMALLINT; break;
    //   case LogicalTypeId::UINTEGER:
    //     set_func = SetValueFromCSV_UINTEGER; break;
    //   case LogicalTypeId::UBIGINT:
    //     set_func = SetValueFromCSV_UBIGINT; break;
    //   case LogicalTypeId::HUGEINT:
    //     throw NotImplementedException("Do not support HugeInt"); break;
    //   case LogicalTypeId::DECIMAL:
    //     throw NotImplementedException("Do not support Decimal"); break;
    //   case LogicalTypeId::FLOAT:
    //     set_func = SetValueFromCSV_FLOAT; break;
    //   case LogicalTypeId::DOUBLE:
    //     set_func = SetValueFromCSV_DOUBLE; break;
    //   case LogicalTypeId::VARCHAR:
    //     set_func = SetValueFromCSV_VARCHAR; break;
    //   default:
    //     throw NotImplementedException("Unsupported type");
    //   }

    //   for (row_cursor = cur_base_row_cursor; row_cursor != pcsv.n_indexes; row_cursor++) {
    //     if (current_index == STANDARD_VECTOR_SIZE) break;
        
    //     idx_t target_index = index_cursor + required_key_column_idxs[i];
    //     idx_t start_offset = pcsv.indexes[target_index-1];
    //     idx_t end_offset = pcsv.indexes[target_index];
    //     set_func(types[i], output, i, current_index, p, start_offset, end_offset);
        
    //     current_index++;
    //     index_cursor += num_columns;
    //   }
		// }
		
		output.SetCardinality(current_index);
		return false;
	}

	// Same Logic as ReadVertexJsonFile
	bool ReadEdgeCSVFile(vector<string> &required_keys, vector<LogicalType> &types, DataChunk &output) {
    if (row_cursor == num_rows) return true;
    
		idx_t current_index = 0;
		vector<idx_t> required_key_column_idxs;
		for (auto &key: required_keys) {
			// Find keys in the schema and extract idxs
			auto key_it = std::find(key_names.begin(), key_names.end(), key);
			if (key_it != key_names.end()) {
				idx_t key_idx = key_it - key_names.begin();
				required_key_column_idxs.push_back(key_idx);
			} else {
				throw InvalidInputException("B");
			}
		}
		// for (; csv_it != iter_end; csv_it++) {
		// 	if (current_index == STANDARD_VECTOR_SIZE) break;
		// 	auto &row = *csv_it;
		// 	for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
		// 		csv::CSVField csv_field = move(row[required_key_column_idxs[i]]);
		// 		output.SimpleSetValue(i, current_index, CSVValToValue(csv_field, types[i]));
		// 		// SetValueFromCSV(types[i], output, i, current_index, csv_field);
		// 		//output.SimpleSetValue<types[i]>(i, current_index, csv_field);
		// 	}
		// 	current_index++;
		// }

    // Row-oriented manner
    std::cout << "num_rows: " << num_rows << std::endl;
		for (; row_cursor < num_rows; row_cursor++) {
			if (current_index == STANDARD_VECTOR_SIZE) break;
			for (size_t i = 0; i < required_key_column_idxs.size(); i++) {
        idx_t target_index = index_cursor + required_key_column_idxs[i];
        idx_t start_offset = pcsv.indexes[target_index-1] + 1;
        idx_t end_offset = pcsv.indexes[target_index];
				SetValueFromCSV(types[i], output, i, current_index, p, start_offset, end_offset);
			}
			current_index++;
      index_cursor += num_columns;
		}
		
		output.SetCardinality(current_index);
		return false;
	}
private:
  LogicalType StringToLogicalType(std::string &type_name, size_t column_idx) {
		const auto end = m.end();
		auto it = m.find(type_name);
		if (it != end) {
      LogicalType return_type = it->second;
			return return_type;
		} else {
			if (type_name.find("ID") != std::string::npos) {
				// ID Column
				if (type == GraphComponentType::VERTEX) {
          auto last_pos = type_name.find_first_of('(');
          string id_name = type_name.substr(0, last_pos - 1);
          auto delimiter_pos = id_name.find("_");

          if (delimiter_pos != std::string::npos) {
            // Multi key
            auto key_order = std::stoi(type_name.substr(delimiter_pos + 1, last_pos - delimiter_pos - 1));
            key_columns_order.push_back(key_order);
            key_columns.push_back(column_idx);
          } else {
            // Single key
            key_columns.push_back(column_idx);
          }
					// D_ASSERT(key_column == -1);
					// key_column = column_idx;
				} else { // type == GraphComponentType::EDGE
					D_ASSERT((src_column == -1) || (dst_column == -1));
					auto first_pos = type_name.find_first_of('(');
					auto last_pos = type_name.find_last_of(')');
					string label_name = type_name.substr(first_pos + 1, last_pos - first_pos - 1);
          std::cout << type_name << std::endl;
          if (type_name.find("START_ID") != std::string::npos) {
            src_key_name = move(label_name);
						src_column = column_idx;
          } else { // "END_ID"
            dst_key_name = move(label_name);
						dst_column = column_idx;
          }
				}
				return LogicalType::UBIGINT;
      } else if (type_name.find("ADJLIST") != std::string::npos) {
        D_ASSERT(type == GraphComponentType::VERTEX);
        return LogicalType::ADJLISTCOLUMN;
      } else if (type_name.find("DECIMAL") != std::string::npos) {
        auto first_pos = type_name.find_first_of('(');
        auto comma_pos = type_name.find_first_of(',');
				auto last_pos = type_name.find_last_of(')');
        int width = std::stoi(type_name.substr(first_pos + 1, comma_pos - first_pos - 1));
        int scale = std::stoi(type_name.substr(comma_pos + 1, last_pos - comma_pos - 1));
        return LogicalType::DECIMAL(width, scale);
			} else {
        fprintf(stdout, "%s\n", type_name.c_str());
				throw InvalidInputException("Unsupported Type");
			}
		}
  }

private:
  GraphComponentType type;
  vector<string> key_names;
	string src_key_name;
	string dst_key_name;
  vector<LogicalType> key_types;
	int64_t key_column = -1;
  vector<int64_t> key_columns;
  vector<int64_t> key_columns_order;
	int64_t src_column = -1;
	int64_t dst_column = -1;
	int64_t num_columns;
  int64_t num_rows;
  idx_t row_cursor;
  idx_t index_cursor;
  idx_t index_size;
	std::basic_string_view<uint8_t> p;
	ParsedCSV pcsv;

	unordered_map<string, LogicalType> m {
		{"STRING", LogicalType(LogicalTypeId::VARCHAR)},
		{"STRING[]", LogicalType(LogicalTypeId::VARCHAR)},
		{"INT"   , LogicalType(LogicalTypeId::INTEGER)},
    {"INTEGER"   , LogicalType(LogicalTypeId::INTEGER)},
		{"LONG"  , LogicalType(LogicalTypeId::BIGINT)},
    {"ULONG"  , LogicalType(LogicalTypeId::UBIGINT)},
    {"DATE"  , LogicalType(LogicalTypeId::DATE)},
    {"DECIMAL"  , LogicalType(LogicalTypeId::DECIMAL)},
    {"ADJLIST"  , LogicalType(LogicalTypeId::ADJLISTCOLUMN)},
	};
};

} // namespace duckdb
