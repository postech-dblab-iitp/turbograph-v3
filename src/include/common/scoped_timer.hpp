#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include "spdlog/spdlog.h"

class ScopedTimer {
public:
    explicit ScopedTimer(const std::string& name,
                         spdlog::level::level_enum super_log_level = spdlog::level::info,
                         spdlog::level::level_enum sub_log_level = spdlog::level::debug,
                         double* result_var = nullptr)
        : name_(name),
          super_log_level_(super_log_level),
          sub_log_level_(sub_log_level),
          start_time_(std::chrono::high_resolution_clock::now()),
          stopped_(false),
          result_var_(result_var) {
        if (spdlog::default_logger_raw()->should_log(super_log_level_)) {
            spdlog::trace("[TIMER START] {}", name_);
        }
    }

    void start(const std::string& sub_name) {
        if (spdlog::default_logger_raw()->should_log(sub_log_level_)) {
            sub_timers_[sub_name] = std::chrono::high_resolution_clock::now();
            spdlog::trace("[TIMER START] Subtask: {}", sub_name);
        }
    }

    void stop(const std::string& sub_name) {
        if (spdlog::default_logger_raw()->should_log(sub_log_level_) && sub_timers_.count(sub_name)) {
            auto end_time = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration<double, std::milli>(end_time - sub_timers_[sub_name]).count();
            sub_timings_.emplace_back(sub_name, elapsed);
            sub_timers_.erase(sub_name);
            spdlog::trace("[TIMER STOP] Subtask: {} took {:.2f} ms", sub_name, elapsed);
        }
    }

    void stop() {
        if (!stopped_) {
            auto end_time = std::chrono::high_resolution_clock::now();
            double total_elapsed = std::chrono::duration<double, std::milli>(end_time - start_time_).count();

            spdlog::log(super_log_level_, "[TIMER] {} took {:.2f} ms", name_, total_elapsed);
            for (const auto& [sub_name, elapsed] : sub_timings_) {
                spdlog::log(super_log_level_, "  ├─ [{}] {:.2f} ms", sub_name, elapsed);
            }
            spdlog::log(super_log_level_, "  └─ [Total] {:.2f} ms", total_elapsed);

            if (result_var_) {
                *result_var_ = total_elapsed;
            }

            stopped_ = true;
        }
    }

    ~ScopedTimer() {
        stop();
    }

private:
    std::string name_;
    spdlog::level::level_enum super_log_level_;
    spdlog::level::level_enum sub_log_level_;
    std::chrono::high_resolution_clock::time_point start_time_;
    std::unordered_map<std::string, std::chrono::high_resolution_clock::time_point> sub_timers_;
    std::vector<std::pair<std::string, double>> sub_timings_;
    bool stopped_;
    double* result_var_;
};

// Macro Definitions for ScopedTimer Usage
#define SCOPED_TIMER(name, super_level, sub_level, result_var) \
    ScopedTimer name##_timer(#name, super_level, sub_level, &result_var)

#define SCOPED_TIMER_SIMPLE(name, super_level, sub_level) \
    ScopedTimer name##_timer(#name, super_level, sub_level)

#define SUBTIMER_START(name, sub_name) name##_timer.start(sub_name)
#define SUBTIMER_STOP(name, sub_name) name##_timer.stop(sub_name)
