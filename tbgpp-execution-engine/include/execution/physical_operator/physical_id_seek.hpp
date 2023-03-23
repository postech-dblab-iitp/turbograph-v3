#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalIdSeek: public CypherPhysicalOperator {

public:
	PhysicalIdSeek(CypherSchema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map)
		: CypherPhysicalOperator(sch), id_col_idx(id_col_idx), oids(oids), projection_mapping(projection_mapping),
		  outer_col_map(move(outer_col_map)), inner_col_map(move(inner_col_map)) { 
			
			D_ASSERT(projection_mapping.size() == 1 ); // 230303
		
			// targetTypes => (pid, newcol1, newcol2, ...) // we fetch pid but abandon pids.
			// schema = (original cols, projected cols)
			// 		if (4, 2) => target_types_index starts from: (2+4)-2 = 4
			for (int col_idx = 0; col_idx < this->inner_col_map.size(); col_idx++) {
				target_types.push_back(sch.getStoredTypes()[this->inner_col_map[col_idx]]);
			}
			D_ASSERT( target_types.size() == projection_mapping[0].size() );

		}
	~PhysicalIdSeek() {}

public:

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// parameters
	uint64_t id_col_idx;
	mutable vector<uint64_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;

	mutable vector<LogicalType> target_types;	// used to initialize output chunks. 

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;
};

}