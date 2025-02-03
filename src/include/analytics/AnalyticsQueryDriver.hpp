#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "analytics/Turbograph.hpp"
#include "analytics/generated_queries/query_list.hpp"

class AnalyticsQueryDriver {
   private:
    std::unique_ptr<Turbograph> current_query;
    std::unordered_map<std::string,
                       std::function<std::unique_ptr<Turbograph>()>>
        query_factories;

   public:
    AnalyticsQueryDriver()
    {
        // Register available analytics queries
        RegisterQuery("pr_int", []() { return std::make_unique<pr_int>(); });
        // Add more queries as needed
    }

    void RegisterQuery(const std::string &name,
                       std::function<std::unique_ptr<Turbograph>()> factory)
    {
        query_factories[name] = std::move(factory);
    }

    bool SetCurrentQuery(const std::string &query_name)
    {
        auto it = query_factories.find(query_name);
        if (it == query_factories.end()) {
            return false;
        }
        current_query = it->second();
        return true;
    }

    void RunQuery(int argc, char **argv)
    {
        if (!current_query) {
            throw std::runtime_error("No query selected");
        }
        current_query->inc_run(argc, argv);
    }

    Turbograph *GetCurrentQuery() { return current_query.get(); }

    std::vector<std::string> GetAvailableQueries() const
    {
        std::vector<std::string> queries;
        for (const auto &pair : query_factories) {
            queries.push_back(pair.first);
        }
        return queries;
    }

    void LoadConfigurationAndSetArgs(const std::string &config_file, int &argc,
                                     char **&argv)
    {
        std::ifstream config(config_file);
        if (!config.is_open()) {
            throw std::runtime_error("Could not open configuration file: " +
                                     config_file);
        }

        // Allocate memory for argv (11 arguments needed)
        argc = 11;
        argv = new char *[argc];
        for (int i = 0; i < argc; i++) {
            argv[i] = new char[1024];  // Allocate space for each argument
        }

        // Set argv[0] as program name
        strcpy(argv[0], "analytics_query");

        std::string line;
        std::map<std::string, int> arg_positions = {
            {"raw_data_dir", 1},          {"db_working_dir", 2},
            {"num_seq_vector_chunks", 3}, {"buffer_pool_size_mb", 4},
            {"num_threads", 5},           {"max_iterations", 6},
            {"max_memory_size", 7},       {"schedule_type", 8},
            {"num_subchunks", 9},         {"max_version", 10}};

        while (std::getline(config, line)) {
            if (line.empty() || line[0] == '#')
                continue;

            std::istringstream iss(line);
            std::string key, value;
            if (std::getline(iss, key, '=') && std::getline(iss, value)) {
                // Trim whitespace
                key.erase(0, key.find_first_not_of(" \t"));
                key.erase(key.find_last_not_of(" \t") + 1);
                value.erase(0, value.find_first_not_of(" \t"));
                value.erase(value.find_last_not_of(" \t") + 1);

                auto it = arg_positions.find(key);
                if (it != arg_positions.end()) {
                    strcpy(argv[it->second], value.c_str());
                }
            }
        }

        config.close();
    }

    void CleanupArgs(int argc, char **argv)
    {
        if (argv) {
            for (int i = 0; i < argc; i++) {
                delete[] argv[i];
            }
            delete[] argv;
        }
    }

    void Run()
    {
        // Initialize analytics engine with config file
        const int MAX_ARGS = 11;
        char **argv = new char *[MAX_ARGS];
        for (int i = 0; i < MAX_ARGS; i++) {
            argv[i] = new char[256];
        }

        InitializeFromConfig("config.txt", MAX_ARGS, argv);

        // Main query loop
        std::string input;
        bool running = true;

        while (running) {
            std::cout << "\nAvailable commands:" << std::endl;
            std::cout << "1. list - Show available queries" << std::endl;
            std::cout << "2. run <query_name> - Run a specific query"
                      << std::endl;
            std::cout << "3. exit - Exit the program" << std::endl;
            std::cout << "\nEnter command: ";

            std::cin >> input;

            if (input == "list") {
                std::cout << "\nAvailable queries:" << std::endl;
                for (const auto &query : GetAvailableQueries()) {
                    std::cout << query << std::endl;
                }
            }
            else if (input == "run") {
                std::string query_name;
                std::cin >> query_name;

                if (SetCurrentQuery(query_name)) {
                    RunQuery(MAX_ARGS, argv);
                }
                else {
                    std::cerr << "Invalid query name. Use 'list' to see "
                                 "available queries."
                              << std::endl;
                }
            }
            else if (input == "exit") {
                running = false;
            }
            else {
                std::cerr << "Invalid command. Please try again." << std::endl;
            }
        }

        // Cleanup
        CleanupArgs(MAX_ARGS, argv);
    }
};