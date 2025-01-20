#include "parallel/thread_context.hpp"

#include "main/client_context.hpp"

namespace s62 {

ThreadContext::ThreadContext(ClientContext &context) : profiler(QueryProfiler::Get(context).IsEnabled()) {
}

} // namespace s62
