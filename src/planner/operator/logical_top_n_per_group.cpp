#include "duckdb/planner/operator/logical_top_n_per_group.hpp"

namespace duckdb {

LogicalTopNPerGroup::LogicalTopNPerGroup(idx_t table_index_p, vector<unique_ptr<Expression>> groups_p, idx_t limit_p,
                                         bool include_row_number_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N_PER_GROUP), table_index(table_index_p),
      groups(std::move(groups_p)), limit(limit_p), include_row_number(include_row_number_p) {
}

vector<ColumnBinding> LogicalTopNPerGroup::GetColumnBindings() {
	auto result = children[0]->GetColumnBindings();
	if (include_row_number) {
		result.emplace_back(table_index, 0);
	}
	return result;
}

vector<idx_t> LogicalTopNPerGroup::GetTableIndex() const {
	return {table_index};
}

void LogicalTopNPerGroup::ResolveTypes() {
	types = children[0]->types;
	if (include_row_number) {
		types.push_back(LogicalType::BIGINT);
	}
}

idx_t LogicalTopNPerGroup::EstimateCardinality(ClientContext &context) {
	auto child_card = LogicalOperator::EstimateCardinality(context);
	if (limit == 0) {
		return 0;
	}
	// Upper bound: at most one row per group per limit rows; without NDV use child cardinality
	return child_card;
}

} // namespace duckdb
