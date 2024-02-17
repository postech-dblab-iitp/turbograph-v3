#pragma once

#include "s62_connection_apis.hpp"
#include "connection_types.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <unistd.h>
#include <iostream>
#include <queue>
#include <nlohmann/json.hpp>
#include <charconv>

namespace duckdb {

class S62ConnectionServer {
public:
    S62ConnectionServer(std::string& workspace, int port): port(port), server_fd(-1), connection(workspace) {
        for (int i = 0; i < NUM_MAX_CLIENTS; i++) {
            available_client_ids.push(i);
        }
    }
    ~S62ConnectionServer() {
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
                case API_ID::GetNodesMetadata:  // GetNodesMetadata
                    {
                        NodeMetadataList node_metadata_list;
                        connection.GetNodesMetadata(node_metadata_list);
            
                        NodeMetadataListToJson(response, node_metadata_list);  // Convert to JSON
                    }
                    break;
                case API_ID::GetEdgesMetadata:  // GetEdgesMetadata
                    {
                        EdgeMetadataList edge_metadata_list;
                        connection.GetEdgesMetadata(edge_metadata_list);
                        
                        EdgeMetadataListToJson(response, edge_metadata_list);
                    }
                    break;
                case API_ID::PrepareStatement:  // PrepareStatement
                    {
                        Query query(buffer + API_ID_SIZE, bytes_read - API_ID_SIZE);
                        std::cout << query << std::endl;
                        std::unique_ptr<CypherPreparedStatement> prep_stmt = connection.PrepareStatement(query);
                        RegisterPreparedStatement(response, prep_stmt);
                    }
                    break;
                case API_ID::SetParams:  // SetParams
                    {
                        ClientId client_id = GetClientID(buffer + API_ID_SIZE);
                        std::string payload(buffer + API_ID_SIZE + sizeof(ClientId), bytes_read - API_ID_SIZE - sizeof(ClientId));
                        SetJsonParams(response, client_stmts[client_id], json::parse(payload));
                    }
                    break;
                case API_ID::ExecuteStatement:  // ExecuteStatement
                    {
                        ClientId client_id = GetClientID(buffer + API_ID_SIZE);
                        QueryResultSetMetadata query_result_set_metadata;
                        Query query = client_stmts[client_id]->getQuery();
                        std::cout << query << std::endl;

                        connection.ExecuteStatement(query, query_result_set_metadata);
                        QueryResultSetMetadataToJson(response, query_result_set_metadata);

                        UnregisterPreparedStatement(client_id);
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

    void RegisterPreparedStatement(Response& response, std::unique_ptr<CypherPreparedStatement>& prep_stmt) {
        if (available_client_ids.empty()) {
            response["status"] = Status::Failure;
        } else {
            ClientId client_id = available_client_ids.front();
            available_client_ids.pop();
            client_stmts[client_id] = std::move(prep_stmt);
            response["status"] = Status::Success;
            response["client_id"] = client_id;
        }
    }

    void UnregisterPreparedStatement(ClientId client_id) {
        available_client_ids.push(client_id);
        client_stmts[client_id].release();
    }

    void NodeMetadataListToJson(Response& response, const NodeMetadataList& node_metadata_list) {
        response["status"] = Status::Success;
        response["metadata"] = json::array();
        for (const auto& metadata : node_metadata_list) {
            json node;
            node["label_name"] = metadata.label_name;
            node["property_names"] = metadata.property_names;
            node["property_types"] = metadata.property_types;
            
            response["metadata"].push_back(node);
        }
    }

    void EdgeMetadataListToJson(Response& response, const EdgeMetadataList& edge_metadata_list) {
        response["status"] = Status::Success;
        response["metadata"] = json::array();
        for (const auto& metadata : edge_metadata_list) {
            json edge;
            edge["type_name"] = metadata.type_name;
            edge["property_names"] = metadata.property_names;
            edge["property_types"] = metadata.property_types;
            
            response["metadata"].push_back(edge);
        }
    }

    void QueryResultSetMetadataToJson(Response& response, const QueryResultSetMetadata& query_result_set_metadata) {
        response["status"] = Status::Success;
        response["result_set_size"] = query_result_set_metadata.result_set_size;
        response["property_names"] = query_result_set_metadata.property_names;
        response["property_types"] = query_result_set_metadata.property_types;
    }

    void SetJsonParams(Response& response, std::unique_ptr<CypherPreparedStatement>& stmt, const json& json_params) {
        std::cout << "SetJsonParams" << std::endl;
        try {
            for (auto& [key, value] : json_params.items()) {
                int index = std::stoi(key);
                
                if (value.is_string()) {
                    stmt->setParam(index, value.get<std::string>());
                } else if (value.is_number_integer()) {
                    stmt->setParam(index, value.get<int>());
                } else if (value.is_number_float()) {
                    stmt->setParam(index, value.get<double>());
                } else if (value.is_boolean()) {
                    stmt->setParam(index, value.get<bool>());
                } else {
                    stmt->setParam(index, value.get<std::string>());
                }
            }
            response["status"] = Status::Success;
        } catch (const std::out_of_range& e) {
            response["status"] = Status::Failure;
            response["error"] = e.what();
        }
    }

private:
    int port;
    int server_fd;
    std::unique_ptr<CypherPreparedStatement> client_stmts[NUM_MAX_CLIENTS];
    std::queue<int> available_client_ids;
    S62ConnectionAPIs connection;
};

}