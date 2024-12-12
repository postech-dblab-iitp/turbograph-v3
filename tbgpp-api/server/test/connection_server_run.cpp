#include <signal.h>
#include <iostream>
#include "s62_connection_server.hpp"

using namespace duckdb;

S62ConnectionServer* s62_connection_server;

void conn_server_signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  s62_connection_server->exit();
  exit(0);
}

int main(int argc, char** argv) {
  if (signal(SIGINT, conn_server_signal_handler) == SIG_ERR) {
      std::cerr << "cannot register signal handler!" << std::endl;
      exit(-1);
  }

  std::string workspace;
  if (argc == 2) {
    workspace = std::string(argv[1]);
  } else {
    workspace = std::string("/data/");
  }

  fprintf(stdout, "Connection Server Directory: %s\n", workspace.c_str());

  s62_connection_server = new S62ConnectionServer(workspace, PORT);
  s62_connection_server->run();

  fprintf(stdout, "Program exit\n");
  return 0;
}