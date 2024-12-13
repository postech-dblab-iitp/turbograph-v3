#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <nlohmann/json.hpp>
#include <charconv>
#include "s62_socket_server.hpp"

using json = nlohmann::json;
typedef int32_t ClientId;

json SendMessageReceieveJson(int sock, std::string& message) {
    static char buffer[8192] = {0};
    memset(buffer, 0, sizeof(buffer));
    send(sock, message.c_str(), message.size(), 0);
    read(sock, buffer, sizeof(buffer));
    std::string jsonResponse(buffer);
    return json::parse(jsonResponse);
}

void PrintPropertyNamesTypes(json& json_data) {
    if (json_data.contains("property_names") && json_data["property_names"].is_array()) {
        std::cout << "Property Names: ";
        for (auto& propName : json_data["property_names"]) {
            std::cout << propName.get<std::string>() << " ";
        }
        std::cout << std::endl;
    }
    
    if (json_data.contains("property_types") && json_data["property_types"].is_array()) {
        std::cout << "Property Types: ";
        for (auto& propType : json_data["property_types"]) {
            std::cout << propType.get<std::string>() << " ";
        }
        std::cout << std::endl;
    }
}

ClientId RunPrepareStatement(int sock) {
    std::string message(1, static_cast<char>(API_ID::PrepareStatement)); 
    std::string query("MATCH (n:Person {id: 65})-[r:IS_LOCATED_IN]->(p:Place) \
		   RETURN \
		   	n.firstName AS firstName, \
			n.lastName AS lastName, \
			n.birthday AS birthday, \
			n.locationIP AS locationIP, \
			n.browserUsed AS browserUsed, \
			p.id AS cityId, \
			n.gender AS gender, \
			n.creationDate AS creationDate;");
    
    message.append(query);
    json receivedDataPrepare = SendMessageReceieveJson(sock, message);

    if (receivedDataPrepare["status"].get<int32_t>() == 1) {
        std::cerr << "Prepare statement failed" << std::endl;
        return -1;
    }

    return receivedDataPrepare["client_id"].get<ClientId>();
}

json RunExecuteStatement(int sock, ClientId client_id) {
    int32_t htonl_client_id = htonl(client_id);
    std::string message = static_cast<char>(API_ID::ExecuteStatement) + 
                            std::string((char*)(&htonl_client_id), 
                            sizeof(ClientId));
    json receivedDataExec = SendMessageReceieveJson(sock, message);

    if (receivedDataExec["status"].get<int32_t>() == 1) {
        std::cerr << "Execute statement failed" << std::endl;
        return {};
    }

    return receivedDataExec;
}

void PrintPrepareSetExecuteStatement(int sock) {
    ClientId client_id = RunPrepareStatement(sock);
    json receivedDataExec = RunExecuteStatement(sock, client_id);

    std::cout << "Result set size: " << receivedDataExec["result_set_size"].get<size_t>() << std::endl;
    PrintPropertyNamesTypes(receivedDataExec);
}

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;
    
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Socket creation error" << std::endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported" << std::endl;
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Connection Failed" << std::endl;
        return -1;
    }

    PrintPrepareSetExecuteStatement(sock);

    close(sock);
    return 0;
}
