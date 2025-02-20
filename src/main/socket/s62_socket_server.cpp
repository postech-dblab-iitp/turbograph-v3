#include <cstdlib> 
#include <sstream> 
#include <stdexcept> 
#include <iostream> 
#include "main/socket/s62_socket_server.hpp"
#include "main/capi/s62.h"
#include "common/logger.hpp"

void S62SocketServer::start() {
    s62_state conn_state = s62_connect(workspace.c_str());
    if (conn_state != S62_SUCCESS) {
        spdlog::error("[start] Failed to connect to workspace: {}", workspace);
        exit(EXIT_FAILURE);
    }

    sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        spdlog::error("[start] Socket creation failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        spdlog::error("[start] Set socket options failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        spdlog::error("[start] Socket bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        spdlog::error("[start] Socket listen failed");
        exit(EXIT_FAILURE);
    }

    spdlog::info("[start] S62 Database Server listening on port {}", port);

    while (true) {
        int new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (new_socket < 0) {
            perror("[start] Socket accept failed");
            continue;
        }
        handleClient(new_socket);
    }
}

void S62SocketServer::stop() {
    if (server_fd != -1) {
        close(server_fd);
    }
    s62_disconnect();
}

void S62SocketServer::handleClient(int socket) {
    char buffer[BUFFER_SIZE] = {0};
    ssize_t bytes_read = read(socket, buffer, sizeof(buffer));

    if (bytes_read <= 0) {
        close(socket);
        return;
    }

    try {
        json request = json::parse(buffer);
        json response = processRequest(request);
        std::string responseStr = response.dump();

        send(socket, responseStr.c_str(), responseStr.size(), 0);
    } catch (const std::exception& e) {
        json errorResponse = {{"status", "error"}, {"error", e.what()}};
        std::string errorStr = errorResponse.dump();
        send(socket, errorStr.c_str(), errorStr.size(), 0);
    }

    close(socket);
}

json S62SocketServer::processRequest(const json& request) {
    json response;

    try {
        if (request.contains("query")) {
            std::string query = request.at("query");
            response = handleQuery(query);
        } 
        else if (request.contains("load") && request.contains("label")) {
            std::string filePath = request.at("load");
            std::string label = request.at("label");
            response = handleLoad(filePath, label);
        } 
        else {
            response = {{"status", "error"}, {"error", "Invalid request: Missing required fields"}};
        }
    } catch (const std::exception& e) {
        response = {{"status", "error"}, {"error", e.what()}};
    }

    return response;
}

json S62SocketServer::handleQuery(const std::string& query) {
    spdlog::info("[handleQuery] Query: {}", query);

    s62_prepared_statement* prep_stmt = s62_prepare(const_cast<char*>(query.c_str()));
    if (prep_stmt == nullptr) {
        spdlog::error("Failed to prepare query: {}", query);
        json response;
        response["status"] = "error";
        response["error"] = "Failed to prepare query";
        return response;
    }

    json response;
    response["status"] = "success";
    response["error"] = nullptr;

    std::vector<std::string> columns;
    std::vector<s62_type> types;
    s62_property* property = prep_stmt->property;
    while (property) {
        columns.push_back(property->property_name);
        types.push_back(property->property_type);
        property = property->next;
    }
    spdlog::info("[handleQuery] Columns: {}", join_vector(columns));
    response["columns"] = columns;

    s62_resultset_wrapper* result_set_wrp;
    s62_num_rows rows = s62_execute(prep_stmt, &result_set_wrp);

    s62_num_properties num_columns = result_set_wrp->result_set->num_properties;
    s62_result* result = result_set_wrp->result_set->result;

    json data;
    while (s62_fetch_next(result_set_wrp) != S62_END_OF_RESULT) {
        json row;
        for (idx_t i = 0; i < num_columns; ++i) {
            switch (types[i]) {
                case S62_TYPE_BOOLEAN:
                    row.push_back(s62_get_bool(result_set_wrp, i));
                    break;
                case S62_TYPE_INTEGER:
                    row.push_back(s62_get_int32(result_set_wrp, i));
                    break;
                case S62_TYPE_BIGINT:
                    row.push_back(s62_get_int64(result_set_wrp, i));
                    break;
                case S62_TYPE_UINTEGER:
                    row.push_back(s62_get_uint32(result_set_wrp, i));
                    break;
                case S62_TYPE_UBIGINT:
                    row.push_back(s62_get_uint64(result_set_wrp, i));
                    break;
                case S62_TYPE_FLOAT:
                    row.push_back(s62_get_float(result_set_wrp, i));
                    break;
                case S62_TYPE_DOUBLE:
                    row.push_back(s62_get_double(result_set_wrp, i));
                    break;
                case S62_TYPE_VARCHAR:
                    row.push_back(std::string(s62_get_varchar(result_set_wrp, i).data));
                    break;
                case S62_TYPE_DECIMAL:
                    row.push_back(std::string(s62_decimal_to_string(s62_get_decimal(result_set_wrp, i)).data));
                    break;
                case S62_TYPE_ID:
                    row.push_back(s62_get_id(result_set_wrp, i));
                    break;
                default:
                    row.push_back(nullptr);
                    break;
            }
        }
        data.push_back(row);
    }
    response["data"] = data;

    s62_close_resultset(result_set_wrp);
    s62_close_prepared_statement(prep_stmt);
    return response;
}

json S62SocketServer::handleLoad(const std::string& filePath, const std::string& label) {
    json response;

    try {
        std::ostringstream command;
        command << "./bulkload "
                << "--log-level " << "debug" << " " 
                << "--incremental " << "true" << " "
                << "--skip-histogram " << "true" << " "
                << "--output_dir " << workspace << " "
                << "--relationships " << label << " "
                << filePath;

        int ret_code = std::system(command.str().c_str());

        if (ret_code == 0) {
            response["status"] = "success";
            response["message"] = "File loaded successfully";
        } else {
            response["status"] = "error";
            response["error"] = "Bulk load failed with exit code: " + std::to_string(ret_code);
        }
    } catch (const std::exception& e) {
        response["status"] = "error";
        response["error"] = e.what();
    }

    return response;
}