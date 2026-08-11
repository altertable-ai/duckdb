#include "catch.hpp"
#include "duckdb/common/sorting/hashed_sort_group_scan.hpp"
#include "duckdb/common/sorting/hashed_sort_lazy.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <type_traits>

using namespace duckdb;

namespace {

struct HashedSortRow {
	int32_t partition;
	int32_t order;
	int32_t id;

	bool operator==(const HashedSortRow &other) const {
		return partition == other.partition && order == other.order && id == other.id;
	}
};

static vector<vector<HashedSortRow>> RunLazyHashedSort() {
	static constexpr idx_t CHUNK_COUNT = 4;
	static constexpr idx_t ROW_COUNT = CHUNK_COUNT * STANDARD_VECTOR_SIZE;

	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	vector<unique_ptr<Expression>> partition_bys;
	partition_bys.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	vector<BoundOrderByNode> order_bys;
	order_bys.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1));
	const vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	const vector<unique_ptr<BaseStatistics>> partition_stats;
	HashedSort hashed_sort(*connection.context, partition_bys, order_bys, types, partition_stats, ROW_COUNT);

	ThreadContext thread(*connection.context);
	ExecutionContext context(*connection.context, thread, nullptr);
	InterruptState interrupt;

	auto global_sink = CreateLazyHashedSortSinkState(hashed_sort, *connection.context);
	auto local = hashed_sort.GetLocalSinkState(context);

	for (idx_t chunk_idx = 0; chunk_idx < CHUNK_COUNT; chunk_idx++) {
		DataChunk input;
		input.Initialize(*connection.context, types);
		for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
			const auto id = NumericCast<int32_t>(chunk_idx * STANDARD_VECTOR_SIZE + row_idx);
			input.SetValue(0, row_idx, Value::INTEGER(id % 4));
			input.SetValue(1, row_idx, Value::INTEGER(NumericCast<int32_t>(ROW_COUNT) - id));
			input.SetValue(2, row_idx, Value::INTEGER(id));
		}
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		OperatorSinkInput sink {*global_sink, *local, interrupt};
		REQUIRE(hashed_sort.Sink(context, input, sink) == SinkResultType::NEED_MORE_INPUT);
	}

	OperatorSinkCombineInput combine {*global_sink, *local, interrupt};
	REQUIRE(hashed_sort.Combine(context, combine) == SinkCombineResultType::FINISHED);
	OperatorSinkFinalizeInput finalize {*global_sink, interrupt};
	REQUIRE(hashed_sort.Finalize(*connection.context, finalize) == SinkFinalizeType::READY);

	auto global_source = hashed_sort.GetGlobalSourceState(*connection.context, *global_sink);
	const auto &groups = hashed_sort.GetHashGroups(*global_source);

	vector<vector<HashedSortRow>> rows(groups.size());
	idx_t scanned_count = 0;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		if (groups[group_idx].count == 0) {
			continue;
		}
		hashed_sort.SortColumnData(context, group_idx, finalize);
		auto scan = CreateHashedSortGroupScan(hashed_sort, context, group_idx, *global_source, interrupt);

		DataChunk output;
		output.Initialize(*connection.context, types);
		while (scan->GetData(output) != SourceResultType::FINISHED) {
			for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
				rows[group_idx].push_back({output.GetValue(0, row_idx).GetValue<int32_t>(),
				                           output.GetValue(1, row_idx).GetValue<int32_t>(),
				                           output.GetValue(2, row_idx).GetValue<int32_t>()});
			}
			output.Reset();
		}
		REQUIRE(rows[group_idx].size() == groups[group_idx].count);
		scanned_count += rows[group_idx].size();

		ReleaseHashedSortGroup(hashed_sort, *global_sink, group_idx);
		REQUIRE_THROWS_AS(CreateHashedSortGroupScan(hashed_sort, context, group_idx, *global_source, interrupt),
		                  InvalidInputException);
		// Idempotent release after the group is gone.
		ReleaseHashedSortGroup(hashed_sort, *global_sink, group_idx);
	}

	REQUIRE(scanned_count == ROW_COUNT);
	return rows;
}

} // namespace

TEST_CASE("Lazy HashedSort creates groups on claim and releases them after scan", "[sort][api]") {
	using FactorySignature = unique_ptr<GlobalSinkState> (*)(const HashedSort &, ClientContext &);
	static_assert(std::is_same<decltype(&CreateLazyHashedSortSinkState), FactorySignature>::value,
	              "unexpected lazy HashedSort sink-state factory signature");
	using ReleaseSignature = void (*)(const HashedSort &, GlobalSinkState &, hash_t);
	static_assert(std::is_same<decltype(&ReleaseHashedSortGroup), ReleaseSignature>::value,
	              "unexpected ReleaseHashedSortGroup signature");

	const auto rows = RunLazyHashedSort();
	idx_t nonempty = 0;
	for (auto &group : rows) {
		if (group.empty()) {
			continue;
		}
		nonempty++;
		for (idx_t row_idx = 1; row_idx < group.size(); row_idx++) {
			const auto &previous = group[row_idx - 1];
			const auto &current = group[row_idx];
			REQUIRE((previous.partition < current.partition ||
			         (previous.partition == current.partition && previous.order <= current.order)));
		}
	}
	REQUIRE(nonempty >= 2);
}
