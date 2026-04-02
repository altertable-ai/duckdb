//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_streaming_top_n_per_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! Emits the first K rows per partition from input sorted by (partition keys, order keys).
class PhysicalStreamingTopNPerGroup : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::STREAMING_TOP_N_PER_GROUP;

	PhysicalStreamingTopNPerGroup(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                              vector<idx_t> group_column_indices_p, idx_t limit_p, bool include_row_number_p,
	                              idx_t estimated_cardinality);

	vector<idx_t> group_column_indices;
	idx_t limit;
	bool include_row_number;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	OrderPreservationType OperatorOrder() const override;
	bool ParallelOperator() const override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
