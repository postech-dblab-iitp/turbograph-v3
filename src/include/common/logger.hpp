
#ifndef S62_LOGGER_HPP
#define S62_LOGGER_HPP

#include <sstream>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "common/scoped_timer.hpp"

/**
trace	매우 상세한 디버깅 정보 (매우 자주 호출되는 곳, 반복문 내부 등)
debug	함수 호출 흐름, 주요 변수 값
info	정상적인 실행 흐름에서 중요한 이벤트 (시작, 완료 등)
warn	문제 가능성이 있는 상황 (비정상적인 입력, 비효율적인 실행)
error	실제로 에러가 발생했을 때
critical	프로그램이 더 이상 실행될 수 없을 때 (예: 시스템 자원 부족)
*/

enum class LogLevel { 
    LOGGER_TRACE, 
    LOGGER_DEBUG, 
    LOGGER_INFO, 
    LOGGER_WARN, 
    LOGGER_ERROR, 
    LOGGER_UNKNOWN 
};

LogLevel getLogLevel(const std::string& level_str) {
    static const std::unordered_map<std::string, LogLevel> log_levels = {
        {"trace", LogLevel::LOGGER_TRACE}, {"debug", LogLevel::LOGGER_DEBUG},
        {"info", LogLevel::LOGGER_INFO}, {"warn", LogLevel::LOGGER_WARN}, {"error", LogLevel::LOGGER_ERROR}
    };
    auto it = log_levels.find(level_str);
    return it != log_levels.end() ? it->second : LogLevel::LOGGER_UNKNOWN;
}

void setLogLevel(LogLevel level) {
    switch (level) {
        case LogLevel::LOGGER_TRACE: spdlog::set_level(spdlog::level::trace); break;
        case LogLevel::LOGGER_DEBUG: spdlog::set_level(spdlog::level::debug); break;
        case LogLevel::LOGGER_INFO: spdlog::set_level(spdlog::level::info); break;
        case LogLevel::LOGGER_WARN: spdlog::set_level(spdlog::level::warn); break;
        case LogLevel::LOGGER_ERROR: spdlog::set_level(spdlog::level::err); break;
        default: spdlog::warn("Invalid log level provided. Using default INFO level.");
    }
}

void SetupLogger() {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%Y-%m-%d %H:%M:%S] [%^%l%$] %v");

    auto logger = std::make_shared<spdlog::logger>("console", console_sink);
    spdlog::set_default_logger(logger);
}

template <typename T>
std::string join_vector(const std::vector<T>& vec, const std::string& delimiter = ", ") {
    std::ostringstream oss;
    if (!vec.empty()) {
        auto it = vec.begin();
        oss << *it;  // First element
        for (++it; it != vec.end(); ++it) {
            oss << delimiter << *it;
        }
    }
    return oss.str();
}

#endif // S62_LOGGER_HPP