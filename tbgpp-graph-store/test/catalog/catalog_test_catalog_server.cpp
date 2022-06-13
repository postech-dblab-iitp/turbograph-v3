#include <signal.h>
#include <iostream>
#include "catalog/catalog_server.hpp"

using namespace duckdb;

void signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  exit(0);
}

int main() {
  if (signal(SIGINT, signal_handler) == SIG_ERR) {
    std::cerr << "cannot register signal handler!" << std::endl;
    exit(-1);
  }
  CatalogServer cat_server("/tmp/catalog_server");
  cat_server.Run();

  fprintf(stdout, "Program exit\n");
  return 0;
}
