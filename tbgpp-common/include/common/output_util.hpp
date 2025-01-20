#ifndef OUTPUT_UTIL
#define OUTPUT_UTIL

#include <memory>
#include "typedef.hpp"

namespace s62 {

#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60

class DataChunk;

class OutputUtil {
   public:
    static void PrintQueryOutput(
        PropertyKeys &col_names,
        std::vector<std::unique_ptr<DataChunk>> &resultChunks,
        bool show_top_10_only);
    static void PrintAllTuplesInDataChunk(DataChunk &chunk);
    static void PrintTop10TuplesInDataChunk(DataChunk &chunk);
    static void PrintLast10TuplesInDataChunk(DataChunk &chunk);

    static void PrintProgress(double percentage);
};
}  // namespace s62

#endif