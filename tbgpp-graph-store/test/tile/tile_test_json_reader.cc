#include <string>

#include "json_reader.hpp"
#include "common/types/data_chunk.hpp"

using namespace duckdb;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

TEST_CASE ("Json Reader Open File Test", "[tile]") {
  GraphJsonFileReader reader;
  
  reader.InitJsonFile("/home/tslee/turbograph-v3/tbgpp-graph-store/test/tile/person_0_0.json", GraphComponentType::VERTEX);
  DataChunk output;
  reader.ReadJsonFile(output);
}

int main(int argc, char **argv) {
  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  return 0;
}
