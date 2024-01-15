#ifndef OUTPUT_UTIL
#define OUTPUT_UTIL

#include "typedef.hpp"

namespace duckdb {

class DataChunk;

class OutputUtil {
public:
    static void PrintQueryOutput(PropertyKeys &col_names, std::vector<DataChunk *> &resultChunks, bool show_top_10_only);
    static void PrintTop10TuplesInDataChunk(DataChunk &chunk);
};
}

#endif