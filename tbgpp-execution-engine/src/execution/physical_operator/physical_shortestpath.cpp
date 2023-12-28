
#include "typedef.hpp"

#include "execution/physical_operator/physical_shortestpath.hpp"

namespace duckdb {

class PhysicalShortestPath : public PhysicalOperator {
public:
    PhysicalShortestPath(OperatorType type, vector<LogicalType> &types, idx_t sid_col_idx);

    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
    OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &output, OperatorState &state) const override;

private:
    void ProcessBFS(ExecutionContext &context, DataChunk &input, DataChunk &output, OperatorState &state) const;
    idx_t sid_col_idx; // Source ID column index
};

class ShortestPathState : public OperatorState {
public:
    BFSIterator *bfs_it;
    // additional state variables as needed
};

}
