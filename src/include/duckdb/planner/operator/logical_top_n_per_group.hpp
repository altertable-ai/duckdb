//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_top_n_per_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Keeps the first K rows per partition key from input sorted by (partition keys, order keys).
class LogicalTopNPerGroup : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TOP_N_PER_GROUP;

public:
	LogicalTopNPerGroup(idx_t table_index_p, vector<unique_ptr<Expression>> groups_p, idx_t limit_p,
	                    bool include_row_number_p);

	idx_t table_index;
	vector<unique_ptr<Expression>> groups;
	idx_t limit;
	bool include_row_number;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	vector<idx_t> GetTableIndex() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
