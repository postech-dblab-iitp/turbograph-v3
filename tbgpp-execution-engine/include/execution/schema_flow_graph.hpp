#ifndef SCHEMA_FLOW_GRAPH_H
#define SCHEMA_FLOW_GRAPH_H

#include "common/assert.hpp"
#include "common/common.hpp"
#include "common/enums/physical_operator_type.hpp"
#include "common/vector.hpp"

#include "typedef.hpp"

namespace duckdb {

class SchemaFlowGraph {  // for each pipeline
   public:
    SchemaFlowGraph() {}

    SchemaFlowGraph(size_t pipeline_length,
                    vector<OperatorType> &pipeline_operator_types,
                    vector<vector<uint64_t>> &num_schemas_of_childs,
                    vector<vector<Schema>> &pipeline_schemas,
                    vector<Schema> &pipeline_union_schemas)
        : pipeline_length(pipeline_length),
          pipeline_operator_types(std::move(pipeline_operator_types)),
          num_schemas_of_childs(std::move(num_schemas_of_childs)),
          schema_per_operator(std::move(pipeline_schemas)),
          union_schema_per_operator(std::move(pipeline_union_schemas))
    {}
    ~SchemaFlowGraph() {}

    void SetFlowGraph(vector<vector<idx_t>> &flow_graph)
    {
        this->flow_graph = std::move(flow_graph);
        is_sfg_exists = true;
    }

    vector<vector<idx_t>> &GetFlowGraph() { return flow_graph; }

    // Unary operator
    idx_t GetNextSchemaIdx(idx_t operator_idx, idx_t schema_idx)
    {
        if (!is_sfg_exists)
            return 0;
        D_ASSERT(operator_idx < flow_graph.size());
        D_ASSERT(schema_idx < flow_graph[operator_idx].size());
        return flow_graph[operator_idx][schema_idx];
    }

    // Binary operator
    idx_t GetNextSchemaIdx(idx_t operator_idx, idx_t schema_idx_1,
                           idx_t schema_idx_2)
    {
        if (!is_sfg_exists)
            return 0;
        D_ASSERT(num_schemas_of_childs[operator_idx].size() == 2);
        idx_t schema_idx =
            (num_schemas_of_childs[operator_idx][1] * schema_idx_1) +
            schema_idx_2;
        D_ASSERT(operator_idx < flow_graph.size());
        D_ASSERT(schema_idx < flow_graph[operator_idx].size());
        return flow_graph[operator_idx][schema_idx];
    }

    Schema &GetOutputSchema(idx_t operator_idx, idx_t schema_idx)
    {
        D_ASSERT(operator_idx < schema_per_operator.size());
        D_ASSERT(schema_idx < schema_per_operator[operator_idx].size());
        return schema_per_operator[operator_idx]
                                  [GetNextSchemaIdx(operator_idx, schema_idx)];
    }

    Schema &GetOutputSchema(idx_t operator_idx, idx_t schema_idx_1,
                            idx_t schema_idx_2)
    {
        D_ASSERT(operator_idx < schema_per_operator.size());
        D_ASSERT(GetNextSchemaIdx(operator_idx, schema_idx_1, schema_idx_2) <
                 num_schemas_of_childs[operator_idx].size());
        return schema_per_operator[operator_idx][GetNextSchemaIdx(
            operator_idx, schema_idx_1, schema_idx_2)];
    }

    Schema &GetUnionOutputSchema(idx_t operator_idx)
    {
        D_ASSERT(operator_idx < union_schema_per_operator.size());
        return union_schema_per_operator[operator_idx];
    }

    vector<vector<uint64_t>> &GetNumSchemasOfChilds()
    {
        return num_schemas_of_childs;
    }

    void printSchemaGraph()
    {
        if (flow_graph.size() == 0)
            std::cout << "SFG is empty" << std::endl;
        for (auto i = 0; i < flow_graph.size(); i++) {
            std::cout << "lv " << i << " :";
            for (auto j = 0; j < flow_graph[i].size(); j++) {
                std::cout << " " << flow_graph[i][j];
            }
            std::cout << std::endl;
        }
    }

    bool IsSFGExists() { return is_sfg_exists; }

    bool IsSchemaChanged()
    {
        if (is_schema_changed_flag) {
            is_schema_changed_flag = false;
            return true;
        }
        return false;
    }

    bool AdvanceCurSourceIdx()
    {
        if (!is_sfg_exists)
            return false;
        if (cur_source_idx + 1 == flow_graph[0].size())
            return false;

        cur_source_idx++;
        is_schema_changed_flag = true;
        return true;
    }

    idx_t GetCurSourceIdx()
    {
        if (!is_sfg_exists)
            return 0;
        return cur_source_idx;
    }

    OperatorType GetOperatorType(idx_t operator_idx)
    {
        if (pipeline_operator_types[operator_idx] == OperatorType::UNARY) {
            return OperatorType::UNARY;
        } else if (pipeline_operator_types[operator_idx] ==
                   OperatorType::BINARY) {
            D_ASSERT(num_schemas_of_childs[operator_idx].size() == 2);
            if (num_schemas_of_childs[operator_idx][1] == 1) {
                return OperatorType::UNARY;
            } else {
                D_ASSERT(num_schemas_of_childs[operator_idx][1] >= 2);
                return OperatorType::BINARY;
            }
        } else {
            D_ASSERT(false);
        }
    }

   private:
    size_t pipeline_length;
    vector<OperatorType> pipeline_operator_types;
    vector<vector<uint64_t>> num_schemas_of_childs;
    vector<vector<idx_t>> flow_graph;
    vector<vector<Schema>> schema_per_operator;
    vector<Schema> union_schema_per_operator;
    bool is_sfg_exists = false;
    bool is_schema_changed_flag = true;
    idx_t cur_source_idx = 0;
};

}  // namespace duckdb

#endif  // SCHEMA_FLOW_GRAPH_H