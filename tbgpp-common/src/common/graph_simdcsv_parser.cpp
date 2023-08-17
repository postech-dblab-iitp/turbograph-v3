#include "graph_simdcsv_parser.hpp"

namespace duckdb{
bool GraphSIMDCSVFileParser::ReadVertexCSVFileUsingHash(GraphPartitioner* graphpartitioner, int32_t num_processes, std::vector<std::string> hash_columns) {
	//below code is copied from ReadVertexCSVFile().

	// if (row_cursor == num_rows) return true; //maybe never happen unless file is empty.
    // idx_t current_index = 0;

    // //DO not need to compute "required key". Store all columns.
    // // vector<idx_t> required_key_column_idxs;
    // // for (auto &key: key_names) {
    // //     // Find keys in the schema and extract idxs
    // //     auto key_it = std::find(key_names.begin(), key_names.end(), key);
    // //     if (key_it != key_names.end()) {
    // //         idx_t key_idx = key_it - key_names.begin();
    // //         required_key_column_idxs.push_back(key_idx);
    // //     } else {
    // //         throw InvalidInputException("A");
    // //     }
    // // }

    // assert(num_columns == key_names.size()); //Assume that key_names contains all columns.
    // vector<idx_t> hash_column_idxs;
    // for (auto &key: hash_columns) { //For hash columns
    //     auto key_it = std::find(key_names.begin(), key_names.end(), key);
    //     if (key_it != key_names.end()) {
    //         idx_t key_idx = key_it - key_names.begin();
    //         hash_column_idxs.push_back(key_idx);
    //     } else {
    //         throw InvalidInputException("A");
    //     }
    // }
    
    // //This function intended not to return until the given file is completely processed.
    // //Initially allocate Datachunks. 
    // std::vector<DataChunk*> datachunks;
    // for(int32_t proc_idx = 0; proc_idx < num_processes; proc_idx++) {
    //     datachunks.push_back(graphpartitioner->AllocateNewChunk(proc_idx));
    // }

    // std::vector<int32_t> index_per_chunk(num_processes, 0);

    // auto apply_hash = [&](LogicalType type, idx_t start_offset, idx_t end_offset){
    //     switch(types[hash_column_idxs[i]].id()) { //Currently only consider integer-like types. Could be added later.
    //         case LogicalTypeId::TINYINT:
    //             int8_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num); 
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::SMALLINT:
    //             int16_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::INTEGER:
    //             int32_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::BIGINT:
    //             int64_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::UTINYINT:
    //             uint8_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::USMALLINT:
    //             uint16_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::UINTEGER:
    //             uint32_t num;
    //             std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num);
    //             return graphpartitioner->PartitionHash(num);
    //         case LogicalTypeId::UBIGINT:
    //         case LogicalTypeId::ID:
    //         case LogicalTypeId::ADJLISTCOLUMN:
    //             uint64_t num;
	// 		    std::from_chars((const char*)p.data() + start_offset, (const char*)p.data() + end_offset, num); 
    //             return graphpartitioner->PartitionHash(num);
    //     }

    // };

    // for (; row_cursor < num_rows; row_cursor++) {
    //     uint64_t dest_rank = 0; //If there are more than one hash columns, simple add all hash values. This is the simplist approach, so can be changed.
    //     for (size_t i = 0; i < hash_column_idxs.size(); i++) {

    //         idx_t target_index = index_cursor + hash_column_idxs[i];
    //         idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
    //         idx_t end_offset = pcsv.indexes[target_index];
    //         //apply hash
    //         dest_rank += apply_hash(key_types[hash_column_idxs[i]], start_offset, end_offset);
    //     }
    //     dest_rank %= num_processes;
    //     for (size_t i = 0; i < key_names.size(); i++) {
    //         idx_t target_index = index_cursor + i;
    //         idx_t start_offset = pcsv.indexes[target_index - 1] + 1;
    //         idx_t end_offset = pcsv.indexes[target_index];
            
    //         SetValueFromCSV(key_types[i], *datachunks[dest_rank], i, current_index, p, start_offset, end_offset);
    //     }
    //     current_index++;
    //     index_cursor += num_columns;
    //     if(++index_per_chunk[dest_rank] == STORAGE_STANDARD_VECTOR_SIZE) {
    //         datachunks[dest_rank]->SetCardinality(index_per_chunk[dest_rank])
    //         datachunks[dest_rank] = graphpartitioner->AllocateNewChunk(dest_rank);
    //         index_per_chunk[dest_rank] = 0;
    //     }
    // }		
    // // output.SetCardinality(current_index);
    // for(int32_t proc_idx = 0; proc_idx < num_processes; proc_idx++) {
    //     datachunks[proc_idx]->SetCardinality(index_per_chunk[proc_idx]);
    // }
    // assert(row_cursor == num_rows);
    // return true;
}

}
