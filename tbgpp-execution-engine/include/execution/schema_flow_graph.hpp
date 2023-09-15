#ifndef SCHEMA_FLOW_GRAPH_H
#define SCHEMA_FLOW_GRAPH_H

#include "common/common.hpp"
#include "common/assert.hpp"
#include "common/vector.hpp"
#include "common/enums/physical_operator_type.hpp"

#include "typedef.hpp"

namespace duckdb {

class SchemaFlowGraph { // for each pipeline
public:
    SchemaFlowGraph() {}

    SchemaFlowGraph(size_t pipeline_length, vector<OperatorType> &pipeline_operator_types, vector<vector<uint64_t>> &num_schemas_of_childs,
        vector<vector<Schema>> &pipeline_schemas, vector<Schema> &pipeline_union_schemas)
        : pipeline_length(pipeline_length), pipeline_operator_types(std::move(pipeline_operator_types)), num_schemas_of_childs(std::move(num_schemas_of_childs)),
        schema_per_operator(std::move(pipeline_schemas)), union_schema_per_operator(std::move(pipeline_union_schemas)) {

        // flow_graph.resize(pipeline_operator_types.size());
        // D_ASSERT(pipeline_operator_types.size() == num_schemas_of_childs.size());

        // for (auto i = 0; i < num_schemas_of_childs.size(); i++) {
        //     if (pipeline_operator_types[i] == OperatorType::UNARY) {
        //         D_ASSERT(num_schemas_of_childs[i].size() == 1);
        //     } else if (pipeline_operator_types[i] == OperatorType::BINARY) {
        //         D_ASSERT(num_schemas_of_childs[i].size() == 2);
        //     }
        //     size_t num_max_possible_output_schemas = 1;
        //     for (auto j = 0; j < num_schemas_of_childs[i].size(); j++) {
        //         num_max_possible_output_schemas *= num_schemas_of_childs[i][j];
        //     }
        //     flow_graph[i].resize(num_max_possible_output_schemas);
        // }
    }
    ~SchemaFlowGraph() {}

    void SetFlowGraph(vector<vector<idx_t>> &flow_graph) {
        this->flow_graph = std::move(flow_graph);
    }

    // Unary operator
    idx_t GetNextSchemaIdx(idx_t operator_idx, idx_t schema_idx) {
        return flow_graph[operator_idx][schema_idx];
    }

    // Binary operator
    idx_t GetNextSchemaIdx(idx_t operator_idx, idx_t schema_idx_1, idx_t schema_idx_2) {
        D_ASSERT(num_schemas_of_childs[operator_idx].size() == 2);
        idx_t schema_idx = (num_schemas_of_childs[operator_idx][1] * schema_idx_1) + schema_idx_2;
        return flow_graph[operator_idx][schema_idx];
    }

    Schema &GetOutputSchema(idx_t operator_idx, idx_t schema_idx) {
        return schema_per_operator[operator_idx][GetNextSchemaIdx(operator_idx, schema_idx)];
    }

    Schema &GetOutputSchema(idx_t operator_idx, idx_t schema_idx_1, idx_t schema_idx_2) {
        return schema_per_operator[operator_idx][GetNextSchemaIdx(operator_idx, schema_idx_1, schema_idx_2)];
    }

    Schema &GetUnionOutputSchema(idx_t operator_idx) {
        return union_schema_per_operator[operator_idx];
    }

private:
    size_t pipeline_length;
    vector<OperatorType> pipeline_operator_types;
    vector<vector<uint64_t>> num_schemas_of_childs;
    vector<vector<idx_t>> flow_graph;
    vector<vector<Schema>> schema_per_operator;
    vector<Schema> union_schema_per_operator;
};

} // namespace duckdb

#endif // SCHEMA_FLOW_GRAPH_H