#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "nlohmann/json.hpp"
#include <charconv>
#include "api/server/s62_socket_server.hpp"

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
    std::string query("MATCH (item:LINEITEM) \\
        WHERE item.L_SHIPDATE <= date('1998-08-25') \\
        RETURN \\
            item.L_RETURNFLAG AS ret_flag, \\
            item.L_LINESTATUS AS line_stat, \\
            sum(item.L_QUANTITY) AS sum_qty, \\
            sum(item.L_EXTENDEDPRICE) AS sum_base_price, \\
            sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price, \\
            sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge, \\
            avg(item.L_QUANTITY) AS avg_qty, \\
            avg(item.L_EXTENDEDPRICE) AS avg_price,\\
            avg(item.L_DISCOUNT) AS avg_disc, \\
            COUNT(*) AS count_order \\
        ORDER BY \\
            ret_flag, \\
            line_stat;");
    
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
        std::cerr << receivedDataExec["error"] << std::endl;
        return {};
    }

    return receivedDataExec;
}

json RunFetch(int sock, ClientId client_id) {
    int32_t htonl_client_id = htonl(client_id);
    std::string message = static_cast<char>(API_ID::Fetch) + 
                            std::string((char*)(&htonl_client_id), 
                            sizeof(ClientId));
    json receivedDataFetch = SendMessageReceieveJson(sock, message);

    if (receivedDataFetch["status"].get<int32_t>() == 1) {
        std::cerr << "Fetch failed" << std::endl;
        std::cerr << receivedDataFetch["error"] << std::endl;
        return {};
    }

    return receivedDataFetch;
}

json RunFetchAll(int sock, ClientId client_id) {
    int32_t htonl_client_id = htonl(client_id);
    std::string message = static_cast<char>(API_ID::FetchAll) + 
                            std::string((char*)(&htonl_client_id), 
                            sizeof(ClientId));
    json receivedDataFetchAll = SendMessageReceieveJson(sock, message);

    if (receivedDataFetchAll["status"].get<int32_t>() == 1) {
        std::cerr << "FetchAll failed" << std::endl;
        std::cerr << receivedDataFetchAll["error"] << std::endl;
        return {};
    }

    return receivedDataFetchAll;
}

void TestFetchAndFetchAll(int sock) {
    ClientId client_id = RunPrepareStatement(sock);
    if (client_id == -1) {
        return;
    }

    json receivedDataExec = RunExecuteStatement(sock, client_id);
    if (receivedDataExec.empty()) {
        return;
    }

    std::cout << "Testing Fetch:" << std::endl;
    for (size_t i = 0; i < 5; ++i) {  // Fetch first 5 rows (or fewer if result set is smaller)
        json fetchResult = RunFetch(sock, client_id);
        if (fetchResult.empty() || fetchResult["status"].get<int32_t>() != 0) {
            break;
        }
        std::cout << "Row " << i + 1 << ": " << fetchResult["data"].get<std::string>() << std::endl;
    }

    std::cout << "Testing FetchAll:" << std::endl;
    json fetchAllResult = RunFetchAll(sock, client_id);
    if (!fetchAllResult.empty() && fetchAllResult["status"].get<int32_t>() == 0) {
        for (const auto& row : fetchAllResult["data"].get<std::vector<std::string>>()) {
            std::cout << row << std::endl;
        }
    }
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

    TestFetchAndFetchAll(sock);

    close(sock);
    return 0;
}
