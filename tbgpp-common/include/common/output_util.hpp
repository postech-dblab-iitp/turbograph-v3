#ifndef OUTPUT_UTIL
#define OUTPUT_UTIL

#include <memory>
#include "typedef.hpp"

namespace duckdb {

class DataChunk;

class OutputUtil {
public:
    static void PrintQueryOutput(PropertyKeys &col_names, std::vector<std::unique_ptr<DataChunk>> &resultChunks, bool show_top_10_only);
    static void PrintTop10TuplesInDataChunk(DataChunk &chunk);
};
}

#endif