#pragma once
#include <string>
#include <nlohmann/json.hpp>

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
    GetNodesMetadata = 0,
    GetEdgesMetadata = 1,
    PrepareStatement = 2,
    SetParams = 3,
    ExecuteStatement = 4
};