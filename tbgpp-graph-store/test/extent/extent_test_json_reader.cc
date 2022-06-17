#include <string>

#include "json_reader.hpp"
#include "common/types/data_chunk.hpp"

using namespace duckdb;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

TEST_CASE ("Json Reader Open File & Read Test", "[tile]") {
  GraphJsonFileReader reader;
  
  reader.InitJsonFile("/home/tslee/turbograph-v3/tbgpp-graph-store/test/tile/person_0_0.json.original", GraphComponentType::VERTEX);
  
  // Assume types are given
  DataChunk output;
  vector<LogicalType> types = {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
  output.Initialize(types);
  vector<string> key_names = {"id", "firstName", "lastName", "gender"};

  while (!reader.ReadJsonFile(key_names, types, output)) {
    fprintf(stdout, "Read JSON File Ongoing..\n");
    fprintf(stdout, "%s", output.ToString().c_str());
  }
  fprintf(stdout, "Read JSON File DONE\n");
  fprintf(stdout, "%s", output.ToString().c_str());
  // tile manager Create Vertex Tiles
}

int main(int argc, char **argv) {
  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  return 0;
}
