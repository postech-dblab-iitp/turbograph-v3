#include <signal.h>
#include <iostream>
#include "catalog/catalog_server.hpp"

using namespace duckdb;

CatalogServer *cat_server;

void cat_signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  cat_server->Exit();
  exit(0);
}

int main() {
  if (signal(SIGINT, cat_signal_handler) == SIG_ERR) {
    std::cerr << "cannot register signal handler!" << std::endl;
    exit(-1);
  }
  cat_server = new CatalogServer("/tmp/catalog_server");
  cat_server->Run();

  fprintf(stdout, "Program exit\n");
  return 0;
}
