#include "duckdb/execution/operator/order/physical_streaming_top_n_per_group.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_top_n_per_group.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalTopNPerGroup &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY);

	auto &plan = CreatePlan(*op.children[0]);

	vector<idx_t> group_indices;
	group_indices.reserve(op.groups.size());
	const auto child_bindings = op.children[0]->GetColumnBindings();
	for (auto &g : op.groups) {
		if (g->GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &br = g->Cast<BoundReferenceExpression>();
			group_indices.push_back(br.index);
			continue;
		}
		if (g->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &bcr = g->Cast<BoundColumnRefExpression>();
			auto it = std::find(child_bindings.begin(), child_bindings.end(), bcr.binding);
			if (it == child_bindings.end()) {
				throw InternalException("Failed to map TopNPerGroup partition binding to child column index");
			}
			group_indices.push_back(NumericCast<idx_t>(it - child_bindings.begin()));
			continue;
		}
		throw InternalException("Unsupported TopNPerGroup partition expression class");
	}

	auto types = op.types;
	auto &stream = Make<PhysicalStreamingTopNPerGroup>(std::move(types), std::move(group_indices), op.limit,
	                                                   op.include_row_number, op.estimated_cardinality);
	stream.children.push_back(plan);
	return stream;
}

} // namespace duckdb
