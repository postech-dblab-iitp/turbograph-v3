#include "schemaless/neo4j_store_simulation.hpp"
#include "schemaless/velox_store_simulation.hpp"
#include "common/common.hpp"
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>

using namespace s62;

int main (int argc, char **argv) {
    string csv_path;
    int method; // 0 Neo4J, 1 Full-Columnar, 2 Partial-Columnar, 3 Velox
    int max_allow_edit_distance;
    int max_merge_count;
    int query_num; // 0 full-scan, 1 specific-columns, 2 predicates
    int target_col = 0;
    int data_size = 1;
    int begin, end;

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
    } else if (argc == 9) {
        csv_path = std::string(argv[1]);
        method = std::atoi(argv[2]);
        max_allow_edit_distance = std::stoi(argv[3]);
        max_merge_count = std::stoi(argv[4]);
        query_num = std::stoi(argv[5]);
        target_col = std::stoi(argv[6]);
        begin = std::stoi(argv[7]);
        end = std::stoi(argv[8]);
    }

    fprintf(stdout, "Load File %s\n", csv_path.c_str());

    int x;

    boost::timer::cpu_timer load_timer;
    boost::timer::cpu_timer query_timer;
    double load_time;
    vector<double> query_execution_times;
    if (method == 0) { // Neo4J
        load_timer.start();
        Neo4JPropertyStore ps(1048576, csv_path.c_str());
        load_time = load_timer.elapsed().wall / 1000000.0;

        int repeat_end;
        if (query_num == 2) repeat_end = 100;
        else repeat_end = 1;

        for (auto repeat = 0; repeat < repeat_end; repeat++) {
            std::random_device rd;
            std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device
            std::uniform_int_distribution<> ran_gen(0, 100);
            int begin_ = ran_gen(mersenne);
            int end_ = ran_gen(mersenne);
            if (begin_ > end_) std::swap(begin_, end_);
            for (int i = 0; i < 5; i++) {
                query_timer.start();
                if (query_num == 0) {
                    ps.FullScanQuery();
                } else if (query_num == 1) {
                    ps.TargetColScanQuery(target_col);
                } else if (query_num == 2) {
                    ps.TargetColRangeQuery(target_col, begin, end);
                }
                auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
                std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
                query_execution_times.push_back(query_exec_time_ms);
            }
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
            query_execution_times.clear();
        }
    } else if (method == 1) { // Full-Columnar
        if (true) {
            load_timer.start();
            FullColumnarFormatStore fcs(1048576, csv_path.c_str());
            load_time = load_timer.elapsed().wall / 1000000.0;
            if (query_num == 1) {
                fcs.generateNonNullExpressionExecutor(target_col);
            }

            int repeat_end;
            if (query_num == 2) repeat_end = 1024;
            else repeat_end = 1;

            for (auto repeat = 0; repeat < repeat_end; repeat++) {
                std::random_device rd;
                std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device
                std::uniform_int_distribution<> ran_gen(0, 100);
                int begin_ = ran_gen(mersenne);
                int end_ = ran_gen(mersenne);
                if (begin_ > end_) std::swap(begin_, end_);

                if (query_num == 2) {
                    fcs.generateExpressionExecutor(target_col, begin_, end_);
                }

                for (int i = 0; i < 5; i++) {
                    query_timer.start();
                    if (query_num == 0) {
                        fcs.FullScanQuery();
                    } else if (query_num == 1) {
                        fcs.TargetColScanQuery(target_col);
                    } else if (query_num == 2) {
                        fcs.TargetColRangeQuery(target_col, begin_, end_);
                    }
                    auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
                    std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
                    query_execution_times.push_back(query_exec_time_ms);
                }
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
                query_execution_times.clear();
            }
        } else {
            // FullColumnarFormatStore fcs;
            // fcs.test5(data_size);
        }
    } else if (method == 2) { // Partial-Columnar
        load_timer.start();
        // PartialColumnarFormatStore pcs(1048576, csv_path.c_str(), max_allow_edit_distance, max_merge_count);
        VeloxPartialColumnarFormatStore pcs(1048576, csv_path.c_str(), max_allow_edit_distance, max_merge_count);
        load_time = load_timer.elapsed().wall / 1000000.0;

        int repeat_end;
        if (query_num == 2) repeat_end = 1024;
        else repeat_end = 1;

        for (auto repeat = 0; repeat < repeat_end; repeat++) {
            std::random_device rd;
            std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device
            std::uniform_int_distribution<> ran_gen(0, 100);
            int begin_ = ran_gen(mersenne);
            int end_ = ran_gen(mersenne);
            if (begin_ > end_) std::swap(begin_, end_);

            // if (query_num == 2) {
            //     fcs.generateExpressionExecutor(target_col, begin_, end_);
            // }

            for (int i = 0; i < 5; i++) {
                query_timer.start();
                if (query_num == 0) {
                    pcs.FullScanQuery();
                } else if (query_num == 1) {
                    pcs.TargetColScanQuery(target_col);
                } else if (query_num == 2) {
                    pcs.TargetColRangeQuery(target_col, begin_, end_);
                }
                auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
                std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
                query_execution_times.push_back(query_exec_time_ms);
            }
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
            query_execution_times.clear();
        }
    } else if (method == 3) {
        load_timer.start();
        // VeloxPropertyStore vps;
        VeloxPropertyStore vps(1048576, csv_path.c_str());
        load_time = load_timer.elapsed().wall / 1000000.0;
        
        for (auto repeat = 0; repeat < 1024; repeat++) {
            std::random_device rd;
            std::mt19937 mersenne(rd()); // Create a mersenne twister, seeded using the random device
            std::uniform_int_distribution<> ran_gen(0, 100);
            int begin_ = ran_gen(mersenne);
            int end_ = ran_gen(mersenne);
            if (begin_ > end_) std::swap(begin_, end_);

            for (int i = 0; i < 5; i++) {
                query_timer.start();
                if (query_num == 0) {
                    DataChunk output;
                    // if (simd_mode == 1) pcs.SIMDFullScanQuery(output);
                    // else pcs.FullScanQuery(output);
                } else if (query_num == 1) {
                    // pcs.TargetColScanQuery(target_col);
                } else if (query_num == 2) {
                    vps.TargetColRangeQuery(target_col, begin_, end_);
                }
                auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
                std::cout << "Query Exec " << i << " elapsed: " << query_exec_time_ms << std::endl;
                query_execution_times.push_back(query_exec_time_ms);
            }
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
            query_execution_times.clear();
        }
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