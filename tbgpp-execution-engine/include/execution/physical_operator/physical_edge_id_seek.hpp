// #pragma once
// #include "typedef.hpp"

// #include "execution/physical_operator/cypher_physical_operator.hpp"

// #include "common/types/value.hpp"

// namespace duckdb {

// class PhysicalEdgeIdSeek: public CypherPhysicalOperator {

// public:

// 	// TODO s62 need modification
// 		// TODO need to receive oids
// 	PhysicalEdgeIdSeek(CypherSchema& sch, uint64_t id_col_idx, vector<vector<uint64_t>> projection_mapping)
// 		: CypherPhysicalOperator(sch), id_col_idx(id_col_idx), projection_mapping(projection_mapping) {

// 			D_ASSERT(projection_mapping.size() == 1 );

// 			D_ASSERT( target_types.size() != 0);

// 				// vector<LogicalType> targetTypes;
// 	// targetTypes.push_back(LogicalType::ID); // for node ids
// 	// for( auto& key: propertyKeys ) {
// 	// 	targetTypes.push_back( outputEdgeSchema.getType(key) );
// 	// }
// 		}
// 	~PhysicalEdgeIdSeek() {}

// public:

// 	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
// 	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

// 	std::string ParamsToString() const override;
// 	std::string ToString() const override;

// 	// parameters
// 	uint64_t id_col_idx;
// 	vector<vector<uint64_t>> projection_mapping;


// 	vector<LogicalType> target_types;	// used to initialize output chunks. 
// };

// }