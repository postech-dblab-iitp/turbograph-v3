#pragma once

#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace s62 {

class PhysicalProduceResults : public CypherPhysicalOperator {

   public:
    PhysicalProduceResults(Schema &sch)
        : CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch)
    {}
    PhysicalProduceResults(Schema &sch, vector<uint64_t> projection_mapping)
        : CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch),
          projection_mapping(projection_mapping)
    {}
    PhysicalProduceResults(Schema &sch,
                           vector<vector<uint64_t>> projection_mappings)
        : CypherPhysicalOperator(PhysicalOperatorType::PRODUCE_RESULTS, sch),
          projection_mappings(projection_mappings)
    {}
    ~PhysicalProduceResults() {}

   public:
    SinkResultType Sink(ExecutionContext &context, DataChunk &input,
                        LocalSinkState &lstate) const override;
    unique_ptr<LocalSinkState> GetLocalSinkState(
        ExecutionContext &context) const override;
    void Combine(ExecutionContext &context,
                 LocalSinkState &lstate) const override;
    bool IsSink() const override { return true; }
    DataChunk &GetLastSinkedData(LocalSinkState &lstate) const override;
    void IdentifyRowVectors(
        DataChunk &input, idx_t num_columns, vector<bool> &column_has_rowvec,
        vector<vector<uint32_t>> &rowstore_idx_list_per_depth) const;
    bool IdentifyIfBaseVectorTypeIsRowVector(const Vector &vector,
                                             int &depth) const;
    void CopyRowVectors(
        DataChunk &input, DataChunk &output, vector<bool> &column_has_rowvec,
        vector<vector<uint32_t>> &rowstore_idx_list_per_depth) const;
    Vector &GetRowVectorAtSpecificDepth(Vector &input, int depth,
                                        int current_depth, idx_t source_count,
                                        SelectionVector *&target_sel) const;
    Vector &GetRowVectorAtSpecificDepth(Vector &input,
                                        const SelectionVector &sel_p, int depth,
                                        int current_depth, idx_t source_count,
                                        SelectionVector *&target_sel) const;

    std::string ParamsToString() const override;
    std::string ToString() const override;

   private:
    vector<uint64_t> projection_mapping;
    vector<vector<uint64_t>> projection_mappings;
    mutable uint64_t num_nulls = 0;
};

}  // namespace s62