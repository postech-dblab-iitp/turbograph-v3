#pragma once

#include "main/socket/s62_socket_apis.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <unistd.h>
#include <iostream>
#include <queue>
#include "nlohmann/json.hpp"
#include <charconv>

using json = nlohmann::json;

#define API_ID_SIZE 1
#define CLIENT_ID_SIZE 4
#define NUM_MAX_CLIENTS 100
#define BUFFER_SIZE 8192
#define PORT 8080

typedef size_t ResultSetSize;
typedef int32_t ClientId;
typedef char APIId;
typedef std::string Request;
typedef json Response;
typedef std::string Query;

enum Status : int32_t {
    Success = 0,
    Failure = 1,
    Unknown = 2
};

enum API_ID : char {
    PrepareStatement = 0,
    ExecuteStatement = 1,
    Fetch = 2,
    FetchAll = 3
};

namespace duckdb {

class S62SocketServer {
public:
    S62SocketServer(std::string& workspace, int port): port(port), server_fd(-1), connection(workspace) {
        for (int i = 0; i < NUM_MAX_CLIENTS; i++) {
            available_client_ids.push(i);
        }
    }
    ~S62SocketServer() {
        exit();
    }

    void run() {
        sockaddr_in address;
        int opt = 1;
        int addrlen = sizeof(address);
        
        // Create socket
        if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
            perror("Socket creation failed");
            ::exit(EXIT_FAILURE);
        }

        // Socket options
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
            perror("Socket options set failed");
            ::exit(EXIT_FAILURE);
        }
        
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);
        
        // Bind
        if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
            perror("Socket bind failed");
            ::exit(EXIT_FAILURE);
        }

        // Listen
        if (listen(server_fd, 10) < 0) {
            perror("Socket listen failed");
            ::exit(EXIT_FAILURE);
        }

        while (true) {
            int new_socket;
            if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
                perror("Socket accept failed");
                ::exit(EXIT_FAILURE);
            }
            listener(new_socket);
        }
    }

    void listener(int socket) {
        while (true) {
            char buffer[BUFFER_SIZE] = {0};  
            ssize_t bytes_read = read(socket, buffer, sizeof(buffer));

            if (bytes_read <= 0) {
                close(socket);
                return;
            }

            API_ID api_identifier = static_cast<API_ID>(buffer[0]);
    
            Response response;
            switch (api_identifier) {
                case API_ID::PrepareStatement:  // PrepareStatement
                    {
                        Query query(buffer + API_ID_SIZE, bytes_read - API_ID_SIZE);
                        std::cout << query << std::endl;
                        RegisterPreparedStatement(response, query);
                    }
                    break;
                case API_ID::ExecuteStatement:  // ExecuteStatement
                    {
                        ClientId client_id = GetClientID(buffer + API_ID_SIZE);
                        QueryResultSetWrapper query_result_set_wrapper;
                        Query query = client_stmts[client_id];
                        std::cout << query << std::endl;

                        connection.ExecuteStatement(query, query_result_set_wrapper);
                        QueryResultSetWrapperToJson(response, query_result_set_wrapper);
                        UnregisterPreparedStatement(client_id);
                        RegisterResultSet(client_id, query_result_set_wrapper);
                    }
                    break;
                case API_ID::Fetch:
                    {
                        ClientId client_id = GetClientID(buffer + API_ID_SIZE);
                        Fetch(response, client_id);
                    }
                    break;
                case API_ID::FetchAll:
                    {
                        ClientId client_id = GetClientID(buffer + API_ID_SIZE);
                        FetchAll(response, client_id);
                    }
                    break;
                default:
                    response["status"] = Status::Failure;
                    response["error"] = "Invalid API request received!";
                    break;
            }
            
            std::string responseStr = response.dump();  // Convert json to string
            send(socket, responseStr.c_str(), responseStr.size(), 0);
        }
    }

    void exit() {
        if(server_fd != -1) {
            close(server_fd);
        }
    }

private:

    inline ClientId GetClientID(char* payload) {
        ClientId client_id;
        std::memcpy(&client_id, payload, sizeof(ClientId));
        client_id = ntohl(client_id);
        return client_id;
    }

    void RegisterPreparedStatement(Response& response, Query& query) {
        if (available_client_ids.empty()) {
            response["status"] = Status::Failure;
        } else {
            ClientId client_id = available_client_ids.front();
            available_client_ids.pop();
            client_stmts[client_id] = std::move(query);
            response["status"] = Status::Success;
            response["client_id"] = client_id;
        }
    }

    void UnregisterPreparedStatement(ClientId client_id) {
        available_client_ids.push(client_id);
    }

    void RegisterResultSet(ClientId client_id, QueryResultSetWrapper& query_result_set_wrapper) {
        client_result_set_map[client_id] = query_result_set_wrapper;
    }

    void QueryResultSetWrapperToJson(Response& response, const QueryResultSetWrapper& query_result_set_wrapper) {
        response["status"] = Status::Success;
        response["result_set_size"] = query_result_set_wrapper.result_set_size;
        response["property_names"] = query_result_set_wrapper.property_names;
    }

    void Fetch(Response& response, ClientId client_id) {
        auto it = client_result_set_map.find(client_id);
        if (it == client_result_set_map.end()) {
            response["status"] = Status::Failure;
            response["error"] = "No result set found for the client ID.";
            return;
        }

        QueryResultSetWrapper& result_set = it->second;

        if (result_set.cursor >= result_set.result_set_size) {
            response["status"] = Status::Failure;
            response["error"] = "No more rows to fetch.";
            return;
        }

        // Locate the chunk and row
        size_t local_cursor;
        size_t cursor = result_set.cursor;
        size_t acc_rows = 0;

        for (auto& chunk : result_set.result_chunks) {
            if (cursor < acc_rows + chunk->size()) {
                local_cursor = cursor - acc_rows;

                // Format the row
                std::string row;
                auto num_cols = chunk->ColumnCount();
                for (size_t i = 0; i < num_cols; i++) {
                    row += chunk->GetValue(i, local_cursor).ToString();
                    if (i != num_cols - 1) row += "|";
                }

                result_set.cursor++;  // Increment cursor

                response["status"] = Status::Success;
                response["data"] = row;
                return;
            }
            acc_rows += chunk->size();
        }

        response["status"] = Status::Failure;
        response["error"] = "Cursor out of bounds.";
    }

    void FetchAll(Response& response, ClientId client_id) {
        auto it = client_result_set_map.find(client_id);
        if (it == client_result_set_map.end()) {
            response["status"] = Status::Failure;
            response["error"] = "No result set found for the client ID.";
            return;
        }

        QueryResultSetWrapper& result_set = it->second;
        std::vector<std::string> all_rows;

        // Iterate through all rows starting from the cursor
        size_t cursor = result_set.cursor;
        size_t acc_rows = 0;

        for (auto& chunk : result_set.result_chunks) {
            auto num_cols = chunk->ColumnCount();

            for (size_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
                if (cursor > 0) {
                    cursor--;
                    continue;
                }

                // Format the row
                std::string row;
                for (size_t i = 0; i < num_cols; i++) {
                    row += chunk->GetValue(i, row_idx).ToString();
                    if (i != num_cols - 1) row += "|";
                }

                all_rows.push_back(row);
            }
        }

        result_set.cursor = result_set.result_set_size;  // Move cursor to the end

        response["status"] = Status::Success;
        response["data"] = all_rows;
    }

private:
    int port;
    int server_fd;
    Query client_stmts[NUM_MAX_CLIENTS];
    std::unordered_map<ClientId, QueryResultSetWrapper> client_result_set_map;
    std::queue<int> available_client_ids;
    S62SocketAPIs connection;
};

}