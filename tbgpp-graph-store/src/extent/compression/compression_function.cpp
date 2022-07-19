#include "common/types/vector.hpp"
#include "extent/compression/compression_function.hpp"

namespace duckdb {

// Dictionary
bool LookupString(string_t str, std::unordered_map<string_t, uint32_t, StringHash, StringCompare> &current_string_map, uint32_t &latest_lookup_result) {
    auto search = current_string_map.find(str);
    auto has_result = search != current_string_map.end();

    if (has_result) {
        latest_lookup_result = search->second;
    }
    return has_result;
}

bool HasEnoughSpace(bool new_string, size_t string_size, idx_t index_pos, bitpacking_width_t &next_width) {
    /*if (new_string) {
        next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
    }*/
    return true; // We use variable sized page.. so maybe we always have enough space
    /*    next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
        return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1,
                                                            index_buffer.size() + 1,
                                                            current_dictionary.size + string_size, next_width);
    } else {
        return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
                                                            current_dictionary.size, current_width);
    }*/
}

void AddNewString(string_t &str, data_ptr_t &string_data_pointer, idx_t &string_data_pos,
                  uint32_t *selection_buffer, idx_t &selection_pos, uint32_t *index_buffer, idx_t &index_pos,
                  std::unordered_map<string_t, uint32_t, StringHash, StringCompare> &current_string_map) {
    // UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

    // Copy string to dict
    idx_t string_size = str.GetSize();
    memcpy(string_data_pointer, str.GetDataUnsafe(), string_size);

    string_data_pointer += string_size;
    string_data_pos += string_size;

    // Update buffers and map
    index_buffer[index_pos++] = (uint32_t)string_data_pos;
    selection_buffer[selection_pos++] = index_pos - 1;
    current_string_map.insert({str, index_pos - 1});
    //DictionaryCompressionStorage::SetDictionary(*current_segment, *current_handle, current_dictionary);
}

void Verify() {
}

void DictionaryCompress(data_ptr_t buf_ptr, size_t buf_size, data_ptr_t data_to_compress, size_t data_size) {
    // Input data to compress
    auto data = (string_t *)data_to_compress;

    // Output buffers, map
	uint32_t *selection_buffer = (uint32_t *)buf_ptr;
    uint32_t *index_buffer = (uint32_t *)(buf_ptr + data_size * sizeof(uint32_t));
    data_ptr_t string_data_pointer = buf_ptr + data_size * 2 * sizeof(uint32_t);
    idx_t selection_pos = 0;
    idx_t index_pos = 0;
    idx_t string_data_pos = data_size * 2 * sizeof(uint32_t);
    std::unordered_map<string_t, uint32_t, StringHash, StringCompare> current_string_map;

    uint32_t latest_lookup_result;

    for (idx_t i = 0; i < data_size; i++) {
        //auto idx = vdata.sel->get_index(i);
        size_t string_size = 0;
        bool new_string = false;
        //auto row_is_valid = vdata.validity.RowIsValid(idx);

        string_size = data[i].GetSize();
        //if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
        if (string_size >= 4096U) {
            throw InvalidInputException("");
            // Big strings not implemented for dictionary compression
            //return false;
            return;
        }
        new_string = !LookupString(data[i], current_string_map, latest_lookup_result);

        bool fits = true;//HasEnoughSpace(new_string, string_size, index_buffer);
        if (!fits) {
            D_ASSERT(false);
            // Flush();
            // new_string = true;
            // D_ASSERT(HasEnoughSpace(new_string, string_size));
        }

        //if (!row_is_valid) {
        //    AddNull();
        if (new_string) {
            AddNewString(data[i], string_data_pointer, string_data_pos, selection_buffer, selection_pos,
                         index_buffer, index_pos, current_string_map);
        } else {
            D_ASSERT(latest_lookup_result < index_pos);
            selection_buffer[selection_pos++] = latest_lookup_result;
            //AddLastLookup();
        }

        Verify();
    }
}

void DictionaryDecompress (data_ptr_t buf_ptr, size_t buf_size, Vector &output, size_t data_size) {}
    auto strings = FlatVector::GetData<string_t>(output);
    uint32_t string_len;
    size_t offset = 0;
    size_t output_idx;
    uint32_t *selection_buffer = (uint32_t *)buf_ptr;
    uint32_t *index_buffer = (uint32_t *)(buf_ptr + data_size * sizeof(uint32_t));
    data_ptr_t string_data_pointer = buf_ptr + data_size * 2 * sizeof(uint32_t);
    idx_t base_string_pos = data_size * 2 * sizeof(uint32_t);
    idx_t cur_string_pos;

    for (output_idx = 0; output_idx < data_size; output_idx++) {
        idx_t index_pos = selection_buffer[output_idx];
        cur_string_pos = index_buffer[index_pos];
        D_ASSERT(cur_string_pos >= base_string_pos);
        string_len = index_pos == 0 ? cur_string_pos - base_string_pos : cur_string_pos - index_buffer[index_pos - 1];
        //memcpy(&string_len, buf_ptr + offset, sizeof(uint32_t));
        //offset += sizeof(uint32_t);
        //auto buffer = unique_ptr<data_t[]>(new data_t[string_len]);
        string string_val((char*)(buf_ptr + cur_string_pos), string_len);
        //Value str_val = Value::BLOB_RAW(string_val);
        //memcpy(buffer.get(), io_requested_buf_ptrs[prev_toggle][i] + offset, string_len);
        
        //std::string temp((char*)buffer.get(), string_len);
        //output.data[i].SetValue(output_idx, str_val);
        strings[output_idx] = StringVector::AddString(output.data[i], (char*)(io_requested_buf_ptrs[prev_toggle][i] + offset), string_len);
        offset += string_len;
        D_ASSERT(offset <= io_requested_buf_sizes[prev_toggle][i]);
    }
}
