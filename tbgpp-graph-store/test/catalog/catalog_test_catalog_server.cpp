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

int main(int argc, char** argv) {
  std::cout << "Here1\n";
  if (signal(SIGINT, cat_signal_handler) == SIG_ERR) {
    std::cerr << "cannot register signal handler!" << std::endl;
    exit(-1);
  }
  std::cout << "Here2\n";
  std::string shm_directory;
  if (argc == 2) {
    shm_directory = std::string(argv[1]);
  } else {
    shm_directory = std::string("/data/");
  }
  std::cout << "Here3\n";
  fprintf(stdout, "Shared Memory Directory: %s\n", shm_directory.c_str());
  cat_server = new CatalogServer("/tmp/catalog_server", shm_directory);
  cat_server->Run();

  fprintf(stdout, "Program exit\n");
  return 0;
}
