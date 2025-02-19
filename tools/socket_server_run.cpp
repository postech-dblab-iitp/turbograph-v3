#include <signal.h>
#include <iostream>
#include "main/socket/s62_socket_server.hpp"
#include "common/logger.hpp"

S62SocketServer* s62_socket_server;

void conn_server_signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  s62_socket_server->stop();
  exit(0);
}

int main(int argc, char** argv) {
  SetupLogger();
  setLogLevel(LogLevel::LOGGER_INFO);

  if (signal(SIGINT, conn_server_signal_handler) == SIG_ERR) {
    spdlog::error("Cannot register signal handler!");
    exit(-1);
  }

  std::string workspace;
  if (argc == 2) {
    workspace = std::string(argv[1]);
  } else {
    workspace = std::string("/data/");
  }

  s62_socket_server = new S62SocketServer(workspace, PORT);
  s62_socket_server->start();
  return 0;
}