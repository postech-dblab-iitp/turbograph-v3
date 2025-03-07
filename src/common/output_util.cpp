#include "common/output_util.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "tblr.h"

namespace duckdb {

void OutputUtil::PrintQueryOutput(PropertyKeys &col_names,
                                  std::vector<std::shared_ptr<DataChunk>> &resultChunks,
                                  bool show_top_10_only)
{
    int LIMIT = 10;
    size_t num_total_tuples = 0;
    size_t num_tuples_printed = 0;
    for (auto &it : resultChunks)
        num_total_tuples += it->size();

    tblr::Table t;
    t.layout(tblr::unicode_box_light_headerline());

    for (int i = 0; i < col_names.size(); i++) {
        t << col_names[i];
    }
    t << tblr::endr;

    std::cout << "==================================================="
              << std::endl;
    std::cout << "[ResultSetSummary] Total " << num_total_tuples << " tuples. ";
    if (LIMIT < num_total_tuples) {
        std::cout << "Showing top " << LIMIT << ":" << std::endl;
    }
    else {
        std::cout << std::endl;
    }

    if (num_total_tuples != 0) {
        int num_tuples_to_print;
        int cur_offset_in_chunk = 0;
        int chunk_idx = 0;
        int num_tuples_to_skip;
        bool skip_tuples = false;
        while (chunk_idx < resultChunks.size()) {
            auto &chunk = resultChunks[chunk_idx];

            if (skip_tuples) {
                if ((chunk->size() - cur_offset_in_chunk) <
                    num_tuples_to_skip) {
                    num_tuples_to_skip -= (chunk->size() - cur_offset_in_chunk);
                    cur_offset_in_chunk = 0;
                    chunk_idx++;
                    continue;
                }
                else {
                    skip_tuples = false;
                    cur_offset_in_chunk += num_tuples_to_skip;
                }
            }
            num_tuples_to_print =
                std::min((int)(chunk->size()) - cur_offset_in_chunk, LIMIT);
            for (int idx = 0; idx < num_tuples_to_print; idx++) {
                for (int i = 0; i < chunk->ColumnCount(); i++) {
                    t << chunk->GetValue(i, cur_offset_in_chunk + idx)
                             .ToString();
                }
                t << tblr::endr;
            }
            LIMIT -= num_tuples_to_print;

            if (LIMIT == 0) {
                std::cout << "[ " << num_tuples_printed << " / "
                          << num_total_tuples << " ]" << std::endl;
                std::cout << t << std::endl;
                LIMIT = 10;
                num_tuples_printed += 10;
                cur_offset_in_chunk += 10;
                if (show_top_10_only) {
                    break;
                }
                else {
                    bool continue_print;
                    while (true) {
                        string show_more;
                        printf("Show 10 more tuples [y/n/s]: ");
                        std::getline(std::cin, show_more);
                        std::for_each(show_more.begin(), show_more.end(),
                                      [](auto &c) { c = std::tolower(c); });
                        if ((show_more == "y") || (show_more == "")) {
                            continue_print = true;
                            break;
                        }
                        else if (show_more == "n") {
                            continue_print = false;
                            break;
                        }
                        else if (show_more == "s") {
                            string to_be_skipped;
                            printf("Num tuples to skip: ");
                            std::getline(std::cin, to_be_skipped);
                            num_tuples_to_skip = std::stoi(to_be_skipped);
                            num_tuples_printed += num_tuples_to_skip;
                            continue_print = true;
                            skip_tuples = true;
                            break;
                        }
                        else {
                            printf("Please Enter Either Y or N\n");
                        }
                    }

                    if (continue_print) {
                        t = tblr::Table();
                        t.layout(tblr::unicode_box_light_headerline());

                        for (int i = 0; i < col_names.size(); i++) {
                            t << col_names[i];
                        }
                        t << tblr::endr;
                        continue;
                    }
                    else {
                        break;
                    }
                }
            }
            else {
                chunk_idx++;
                cur_offset_in_chunk = 0;
            }
        }
    }
    if (LIMIT != 10) {
        std::cout << t << std::endl;
    }
    std::cout << "==================================================="
              << std::endl;
}

void OutputUtil::PrintAllTuplesInDataChunk(DataChunk &chunk)
{
    tblr::Table t;
	t.layout(tblr::unicode_box_light_headerline());

    // print type & vector info
    for (int i = 0; i < chunk.ColumnCount(); i++) {
        t << chunk.GetTypes()[i].ToString() + " / " +
                 VectorTypeToString(chunk.data[i].GetVectorType()) + " / " +
                 std::to_string(chunk.data[i].GetIsValid());
    }
    t << tblr::endr;

	idx_t num_tuples_to_print = chunk.size();
	for (int idx = 0 ; idx < num_tuples_to_print ; idx++) {
		for (int i = 0; i < chunk.ColumnCount(); i++) {
			t << chunk.GetValue(i, idx).ToString();
		}
		t << tblr::endr;
	}
	std::cout << t << std::endl;
}

void OutputUtil::PrintTop10TuplesInDataChunk(DataChunk &chunk)
{
    int num_columns_to_print_at_once = 5;
    int begin_col_offset = 0;
    int end_col_offset;

    do {
        tblr::Table t;
        t.layout(tblr::unicode_box_light_headerline());
        end_col_offset =
            std::min(begin_col_offset + num_columns_to_print_at_once,
                     (int)chunk.ColumnCount());
        // print type & vector info
        for (int i = begin_col_offset; i < end_col_offset; i++) {
            t << std::to_string(i) + " / " + chunk.GetTypes()[i].ToString() +
                     " / " + VectorTypeToString(chunk.data[i].GetVectorType()) +
                     " / " + std::to_string(chunk.data[i].GetIsValid());
        }
        t << tblr::endr;

        // std::cout << "Schema Info (Type / VectorType / IsValid)" << std::endl;
        // std::cout << t << std::endl;

        // print tuples
        idx_t num_tuples_to_print = std::min(10UL, chunk.size());
        for (int idx = 0; idx < num_tuples_to_print; idx++) {
            for (int i = begin_col_offset; i < end_col_offset; i++) {
                t << chunk.GetValue(i, idx).ToString();
            }
            t << tblr::endr;
        }
        std::cout << "Top 10 Tuples" << std::endl;
        std::cout << t << std::endl;
        begin_col_offset = end_col_offset;
    } while (end_col_offset < chunk.ColumnCount());
}

void OutputUtil::PrintLast10TuplesInDataChunk(DataChunk &chunk)
{
    tblr::Table t;
	t.layout(tblr::unicode_box_light_headerline());
	idx_t num_tuples_to_print = std::min(10UL, chunk.size());
	for (int idx = 0 ; idx < num_tuples_to_print ; idx++) {
		for (int i = 0; i < chunk.ColumnCount(); i++) {
			t << chunk.GetValue(i, chunk.size() - num_tuples_to_print + idx).ToString();
		}
		t << tblr::endr;
	}
	std::cout << t << std::endl;
}

void OutputUtil::PrintProgress(double percentage)
{
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;
    printf("\r%3d%% [%.*s%*s]", val, lpad, PBSTR, rpad, "");
    fflush(stdout);
}

};  // namespace duckdb
