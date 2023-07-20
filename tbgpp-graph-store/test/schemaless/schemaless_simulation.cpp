#include "schemaless/neo4j_store_simulation.hpp"
#include "schemaless/velox_store_simulation.hpp"
#include "common/common.hpp"
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>

using namespace duckdb;

int main (int argc, char **argv) {
    string csv_path;
    int method; // 0 Neo4J, 1 Full-Columnar, 2 Partial-Columnar, 3 Velox
    int max_allow_edit_distance;
    int max_merge_count;
    int query_num; // 0 full-scan, 1 specific-columns, 2 predicates
    int target_col = 0;
    int simd_mode = 0;
    int data_size = 1;

    if (argc == 3) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
    } else if (argc == 4) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
        data_size = std::atoi(argv[3]);
    } else if (argc == 6) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
        max_allow_edit_distance = std::stoi(argv[3]);
        max_merge_count = std::stoi(argv[4]);
        query_num = std::stoi(argv[5]);
    } else if (argc == 7) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
        max_allow_edit_distance = std::stoi(argv[3]);
        max_merge_count = std::stoi(argv[4]);
        query_num = std::stoi(argv[5]);
        target_col = std::stoi(argv[6]);
    } else if (argc == 8) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
        max_allow_edit_distance = std::stoi(argv[3]);
        max_merge_count = std::stoi(argv[4]);
        query_num = std::stoi(argv[5]);
        target_col = std::stoi(argv[6]);
        simd_mode = std::stoi(argv[7]);
    }

    fprintf(stdout, "Load File %s\n", csv_path.c_str());

    boost::timer::cpu_timer load_timer;
    boost::timer::cpu_timer query_timer;
    double load_time;
    vector<double> query_execution_times;
    if (method == 0) { // Neo4J
        load_timer.start();
        Neo4JPropertyStore ps(1048576, csv_path.c_str());
        load_time = load_timer.elapsed().wall / 1000000.0;
        for (int i = 0; i < 5; i++) {
            query_timer.start();
            if (query_num == 0) {
                DataChunk output;
                ps.FullScanQuery(output);
            } else if (query_num == 1) {
                ps.TargetColScanQuery(target_col);
            }
            auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
            std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
            query_execution_times.push_back(query_exec_time_ms);
        }
    } else if (method == 1) { // Full-Columnar
        if (false) {
            load_timer.start();
            FullColumnarFormatStore fcs(1048576, csv_path.c_str());
            load_time = load_timer.elapsed().wall / 1000000.0;
            for (int i = 0; i < 5; i++) {
                query_timer.start();
                if (query_num == 0) {
                    DataChunk output;
                    if (simd_mode == 1) fcs.SIMDFullScanQuery(output);
                    else fcs.FullScanQuery(output);
                } else if (query_num == 1) {
                    fcs.TargetColScanQuery(target_col);
                }
                auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
                std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
                query_execution_times.push_back(query_exec_time_ms);
            }
        } else {
            FullColumnarFormatStore fcs;
            fcs.test5(data_size);
        }
    } else if (method == 2) { // Partial-Columnar
        load_timer.start();
        PartialColumnarFormatStore pcs(1048576, csv_path.c_str(), max_allow_edit_distance, max_merge_count);
        load_time = load_timer.elapsed().wall / 1000000.0;
        for (int i = 0; i < 5; i++) {
            query_timer.start();
            if (query_num == 0) {
                DataChunk output;
                if (simd_mode == 1) pcs.SIMDFullScanQuery(output);
                else pcs.FullScanQuery(output);
            } else if (query_num == 1) {
                pcs.TargetColScanQuery(target_col);
            }
            auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
            std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
            query_execution_times.push_back(query_exec_time_ms);
        }
    } else if (method == 3) {
        VeloxPropertyStore vps;
        // vps.test();
        // vps.test2_2();
        // for (int i = 1; i <= 1024; i++) {
        //     vps.test2_2(true);
        // }
        vps.test2_2(false);
        // vps.test3();
        // vps.test4();
        // warmup
        // for (int i = 1; i <= 1024; i++) {
        //     vps.test4(true, 256);
        // }
        // vps.test4(false, 256);
        // vps.test5(false, data_size, 256);
    }

    std::cout << "Load Elapsed Time: " << load_time << " ms" << std::endl;

    double max_exec_time = std::numeric_limits<double>::min();
    double min_exec_time = std::numeric_limits<double>::max();
    double accumulated_exec_time = 0.0;
    for (int i = 0; i < query_execution_times.size(); i++) {
        if (max_exec_time < query_execution_times[i]) max_exec_time = query_execution_times[i];
        if (min_exec_time > query_execution_times[i]) min_exec_time = query_execution_times[i];
        accumulated_exec_time += query_execution_times[i];
    }
    if (query_execution_times.size() >= 3) {
        accumulated_exec_time -= max_exec_time;
        accumulated_exec_time -= min_exec_time;
        std::cout << "Average Query Execution Time (w/o min/max exec time): " << accumulated_exec_time / (query_execution_times.size() - 2) << " ms" << std::endl;
    } else {
        std::cout << "Average Query Execution Time: " << accumulated_exec_time / (query_execution_times.size()) << " ms" << std::endl;
    }

    return 0;
}