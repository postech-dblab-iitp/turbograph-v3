#include "common/graph_simdcsv_parser.hpp"
#include "parallel/graph_partition.hpp"

namespace duckdb{


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
bool find_indexes(const uint8_t * buf, size_t len, ParsedCSV & pcsv) { //Made this "inline" to avoid multiple definition error. Does it matter?
  // does the previous iteration end inside a double-quote pair?
  uint64_t prev_iter_inside_quote = 0ULL;  // either all zeros or all ones
#ifdef CRLF
  uint64_t prev_iter_cr_end = 0ULL; 
#endif
  size_t lenminus64 = len < 64 ? 0 : len - 64;
  size_t idx = 0;
  uint32_t *base_ptr = pcsv.indexes;
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

bool GraphSIMDCSVFileParser::ReadVertexCSVFileUsingHash(GraphPartitioner* graphpartitioner, int32_t num_processes, std::vector<std::string> hash_columns) {
	//below code is copied from ReadVertexCSVFile().

	if (row_cursor == num_rows) return true; //maybe never happen unless file is empty.
    idx_t current_index = 0;

    //DO not need to compute "required key". Store all columns.
    // vector<idx_t> required_key_column_idxs;
    // for (auto &key: key_names) {
    //     // Find keys in the schema and extract idxs
    //     auto key_it = std::find(key_names.begin(), key_names.end(), key);
    //     if (key_it != key_names.end()) {
    //         idx_t key_idx = key_it - key_names.begin();
    //         required_key_column_idxs.push_back(key_idx);
    //     } else {
    //         throw InvalidInputException("A");
    //     }
    // }

    D_ASSERT(num_columns == key_names.size()); //Assume that key_names contains all columns.
    vector<idx_t> hash_column_idxs;
    for (auto &key: hash_columns) { //For hash columns
        auto key_it = std::find(key_names.begin(), key_names.end(), key);
        if (key_it != key_names.end()) {
            idx_t key_idx = key_it - key_names.begin();
            hash_column_idxs.push_back(key_idx);
        } else {
            throw InvalidInputException("A");
        }
    }
    
    //This function intended not to return until the given file is completely processed.
    //Initially allocate Datachunks. 
    std::vector<DataChunk*> datachunks;
    for(int32_t proc_idx = 0; proc_idx < num_processes; proc_idx++) {
        datachunks.push_back(graphpartitioner->AllocateNewChunk(proc_idx));
    }

    std::vector<int32_t> index_per_chunk(num_processes, 0);

    auto apply_hash = [&](LogicalType type, idx_t start_offset, idx_t end_offset){
        switch(type.id()) { //Currently only consider integer-like types. Could be added later.
            case LogicalTypeId::TINYINT:
                int8_t numi8;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numi8); 
                return graphpartitioner->PartitionHash(numi8);
            case LogicalTypeId::SMALLINT:
                int16_t numi16;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numi16);
                return graphpartitioner->PartitionHash(numi16);
            case LogicalTypeId::INTEGER:
                int32_t numi32;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numi32);
                return graphpartitioner->PartitionHash(numi32);
            case LogicalTypeId::BIGINT:
                int64_t numi64;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numi64);
                return graphpartitioner->PartitionHash(numi64);
            case LogicalTypeId::UTINYINT:
                uint8_t numu8;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numu8);
                return graphpartitioner->PartitionHash(numu8);
            case LogicalTypeId::USMALLINT:
                uint16_t numu16;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numu16);
                return graphpartitioner->PartitionHash(numu16);
            case LogicalTypeId::UINTEGER:
                uint32_t numu32;
                std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numu32);
                return graphpartitioner->PartitionHash(numu32);
            case LogicalTypeId::UBIGINT:
            case LogicalTypeId::ID:
            case LogicalTypeId::ADJLISTCOLUMN:
                uint64_t numu64;
			    std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, numu64); 
                return graphpartitioner->PartitionHash(numu64);
            default:
                printf("Not implemented hash column type.\n");
                return 1;
        }
    };

    for (; row_cursor < num_rows; row_cursor++) {
        uint64_t dest_rank = 0; //If there are more than one hash columns, simple add all hash values. This is the simplist approach, so can be changed.
        for (size_t i = 0; i < hash_column_idxs.size(); i++) {

            idx_t target_index = index_cursor + hash_column_idxs[i];
            idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
            idx_t end_offset = pcsv.indexes[target_index];
            //apply hash
            dest_rank += apply_hash(key_types[hash_column_idxs[i]], start_offset, end_offset);
        }
        dest_rank = (dest_rank % (num_processes-1)) + 1; //Root (rank=0) do not store anything. Need to change this if want to change master-segment policy.
        for (size_t i = 0; i < key_names.size(); i++) {
            idx_t target_index = index_cursor + i;
            idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
            idx_t end_offset = pcsv.indexes[target_index];
            
            SetValueFromCSV(key_types[i], *datachunks[dest_rank], i, current_index, p, start_offset, end_offset);
        }
        current_index++;
        index_cursor += num_columns;
        if(++index_per_chunk[dest_rank] == STORAGE_STANDARD_VECTOR_SIZE) {
            datachunks[dest_rank]->SetCardinality(index_per_chunk[dest_rank]);
            datachunks[dest_rank] = graphpartitioner->AllocateNewChunk(dest_rank);
            index_per_chunk[dest_rank] = 0;
        }
    }		
    // output.SetCardinality(current_index);
    for(int32_t proc_idx = 1; proc_idx < num_processes; proc_idx++) {
        D_ASSERT(index_per_chunk[0] == 0); //root should store nothing.
        datachunks[proc_idx]->SetCardinality(index_per_chunk[proc_idx]);
    }
    D_ASSERT(row_cursor == num_rows);
    return true;
}

}
