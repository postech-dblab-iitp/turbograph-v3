#pragma once

#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <thread>
#include <vector>
#include <unistd.h>
#include "nlohmann/json.hpp"

using json = nlohmann::json;

#define BUFFER_SIZE 8192
#define PORT 8080

class S62SocketServer {
public:
    S62SocketServer(std::string workspace, int port) : workspace(workspace), port(port), server_fd(-1) {}

    ~S62SocketServer() {
        stop();
    }

    void start();
    void stop();

private:
    void handleClient(int socket);
    json processRequest(const json& request);
    json handleQuery(const std::string& query);
    json handleLoad(const std::string& filePath, const std::string& label);

    int port;
    int server_fd;
    std::string workspace;
};
