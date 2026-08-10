#include "catch.hpp"
#include "duckdb/common/sorting/hashed_sort_eager.hpp"
#include "duckdb/common/sorting/hashed_sort_group_scan.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>
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

struct HashedSortResult {
	vector<vector<HashedSortRow>> rows;
	vector<idx_t> counts;
	idx_t nonempty_groups = 0;
	idx_t empty_groups = 0;
};

static HashedSortResult RunHashedSort(bool eager) {
	static constexpr idx_t CHUNK_COUNT = 8;
	static constexpr idx_t ROW_COUNT = CHUNK_COUNT * STANDARD_VECTOR_SIZE;

	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection connection(db);
	connection.BeginTransaction();
	connection.ForceParallelism();

	vector<unique_ptr<Expression>> partition_bys;
	partition_bys.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	vector<BoundOrderByNode> order_bys;
	order_bys.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1));
	const vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	const vector<unique_ptr<BaseStatistics>> partition_stats;
	HashedSort hashed_sort(*connection.context, partition_bys, order_bys, types, partition_stats, ROW_COUNT);

	ThreadContext thread_a(*connection.context);
	ThreadContext thread_b(*connection.context);
	ThreadContext thread_c(*connection.context);
	ExecutionContext context_a(*connection.context, thread_a, nullptr);
	ExecutionContext context_b(*connection.context, thread_b, nullptr);
	ExecutionContext context_c(*connection.context, thread_c, nullptr);
	InterruptState interrupt_a;
	InterruptState interrupt_b;
	InterruptState interrupt_c;

	unique_ptr<GlobalSinkState> global_sink;
	if (eager) {
		global_sink = CreateEagerHashedSortSinkState(hashed_sort, *connection.context);
	} else {
		global_sink = hashed_sort.GetGlobalSinkState(*connection.context);
	}
	auto local_a = hashed_sort.GetLocalSinkState(context_a);
	auto local_b = hashed_sort.GetLocalSinkState(context_b);
	auto local_c = hashed_sort.GetLocalSinkState(context_c);

	auto sink_chunk = [&](idx_t chunk_idx, ExecutionContext &context, LocalSinkState &local,
	                      InterruptState &interrupt) {
		DataChunk input;
		input.Initialize(*connection.context, types);
		for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
			const auto id = NumericCast<int32_t>(chunk_idx * STANDARD_VECTOR_SIZE + row_idx);
			input.SetValue(0, row_idx, Value::INTEGER((id * 5 + 3) % 8));
			input.SetValue(1, row_idx, Value::INTEGER(NumericCast<int32_t>(ROW_COUNT) - id));
			input.SetValue(2, row_idx, Value::INTEGER(id));
		}
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		OperatorSinkInput sink {*global_sink, local, interrupt};
		REQUIRE(hashed_sort.Sink(context, input, sink) == SinkResultType::NEED_MORE_INPUT);
	};

	// Hold local A open across the first Combines. B and C freeze the global radix-bit count while A still owns
	// pre-freeze local radix data; only then does A resume sinking additional chunks.
	sink_chunk(0, context_a, *local_a, interrupt_a);
	sink_chunk(1, context_b, *local_b, interrupt_b);
	sink_chunk(2, context_c, *local_c, interrupt_c);

	SinkCombineResultType combine_result_b = SinkCombineResultType::FINISHED;
	SinkCombineResultType combine_result_c = SinkCombineResultType::FINISHED;
	std::exception_ptr combine_error_b;
	std::exception_ptr combine_error_c;
	std::mutex combine_lock;
	std::condition_variable combine_cv;
	idx_t combine_threads_ready = 0;
	bool combine_threads_start = false;
	auto wait_for_combine_start = [&]() {
		std::unique_lock<std::mutex> guard(combine_lock);
		combine_threads_ready++;
		combine_cv.notify_all();
		combine_cv.wait(guard, [&]() { return combine_threads_start; });
	};
	std::thread combine_thread_b([&]() {
		try {
			wait_for_combine_start();
			OperatorSinkCombineInput combine {*global_sink, *local_b, interrupt_b};
			combine_result_b = hashed_sort.Combine(context_b, combine);
		} catch (...) {
			combine_error_b = std::current_exception();
		}
	});
	std::thread combine_thread_c([&]() {
		try {
			wait_for_combine_start();
			OperatorSinkCombineInput combine {*global_sink, *local_c, interrupt_c};
			combine_result_c = hashed_sort.Combine(context_c, combine);
		} catch (...) {
			combine_error_c = std::current_exception();
		}
	});
	{
		std::unique_lock<std::mutex> guard(combine_lock);
		combine_cv.wait(guard, [&]() { return combine_threads_ready == 2; });
		combine_threads_start = true;
	}
	combine_cv.notify_all();
	combine_thread_b.join();
	combine_thread_c.join();
	if (combine_error_b) {
		std::rethrow_exception(combine_error_b);
	}
	if (combine_error_c) {
		std::rethrow_exception(combine_error_c);
	}
	REQUIRE(combine_result_b == SinkCombineResultType::FINISHED);
	REQUIRE(combine_result_c == SinkCombineResultType::FINISHED);

	for (idx_t chunk_idx = 3; chunk_idx < CHUNK_COUNT; chunk_idx++) {
		sink_chunk(chunk_idx, context_a, *local_a, interrupt_a);
	}
	OperatorSinkCombineInput combine_a {*global_sink, *local_a, interrupt_a};
	REQUIRE(hashed_sort.Combine(context_a, combine_a) == SinkCombineResultType::FINISHED);

	OperatorSinkFinalizeInput finalize {*global_sink, interrupt_a};
	REQUIRE(hashed_sort.Finalize(*connection.context, finalize) == SinkFinalizeType::READY);
	auto global_source = hashed_sort.GetGlobalSourceState(*connection.context, *global_sink);
	const auto &groups = hashed_sort.GetHashGroups(*global_source);

	HashedSortResult result;
	result.rows.resize(groups.size());
	result.counts.resize(groups.size());
	if (eager) {
		LocalSourceState local_source;
		OperatorSourceInput source {*global_source, local_source, interrupt_a};
		for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
			if (!groups[group_idx].count) {
				continue;
			}
			REQUIRE_THROWS_AS(hashed_sort.MaterializeColumnData(context_a, group_idx, source), InvalidInputException);
			REQUIRE_THROWS_AS(hashed_sort.MaterializeSortedRun(context_a, group_idx, source), InvalidInputException);
			REQUIRE_THROWS_AS(hashed_sort.GetColumnData(group_idx, source), InvalidInputException);
			break;
		}
	}
	idx_t scanned_count = 0;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		const auto &group = groups[group_idx];
		result.counts[group_idx] = group.count;
		if (group.count == 0) {
			result.empty_groups++;
			continue;
		}
		result.nonempty_groups++;

		hashed_sort.SortColumnData(context_a, group_idx, finalize);
		auto scan = CreateHashedSortGroupScan(hashed_sort, context_a, group_idx, *global_source, interrupt_a);
		REQUIRE_THROWS_AS(CreateHashedSortGroupScan(hashed_sort, context_a, group_idx, *global_source, interrupt_a),
		                  InvalidInputException);

		DataChunk output;
		output.Initialize(*connection.context, types);
		while (scan->GetData(output) != SourceResultType::FINISHED) {
			for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
				result.rows[group_idx].push_back({output.GetValue(0, row_idx).GetValue<int32_t>(),
				                                  output.GetValue(1, row_idx).GetValue<int32_t>(),
				                                  output.GetValue(2, row_idx).GetValue<int32_t>()});
			}
			output.Reset();
		}
		REQUIRE(scan->GetData(output) == SourceResultType::FINISHED);
		REQUIRE(result.rows[group_idx].size() == group.count);
		scanned_count += result.rows[group_idx].size();

		for (idx_t row_idx = 1; row_idx < result.rows[group_idx].size(); row_idx++) {
			const auto &previous = result.rows[group_idx][row_idx - 1];
			const auto &current = result.rows[group_idx][row_idx];
			REQUIRE((previous.partition < current.partition ||
			         (previous.partition == current.partition && previous.order <= current.order)));
		}
	}

	REQUIRE(scanned_count == ROW_COUNT);
	REQUIRE(result.nonempty_groups >= 2);
	REQUIRE(result.empty_groups >= 1);
	return result;
}

} // namespace

TEST_CASE("Eager HashedSort combines local partitions directly into sorted hash groups", "[sort][api]") {
	using FactorySignature = unique_ptr<GlobalSinkState> (*)(const HashedSort &, ClientContext &);
	static_assert(std::is_same<decltype(&CreateEagerHashedSortSinkState), FactorySignature>::value,
	              "unexpected eager HashedSort sink-state factory signature");

	const auto normal = RunHashedSort(false);
	const auto eager = RunHashedSort(true);
	REQUIRE(eager.counts == normal.counts);
	REQUIRE(eager.rows == normal.rows);
}

TEST_CASE("Eager HashedSort handles empty input", "[sort][api]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	vector<unique_ptr<Expression>> partition_bys;
	partition_bys.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	vector<BoundOrderByNode> order_bys;
	order_bys.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1));
	const vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER};
	const vector<unique_ptr<BaseStatistics>> partition_stats;
	HashedSort hashed_sort(*connection.context, partition_bys, order_bys, types, partition_stats, 0);
	auto global_sink = CreateEagerHashedSortSinkState(hashed_sort, *connection.context);
	InterruptState interrupt;
	OperatorSinkFinalizeInput finalize {*global_sink, interrupt};
	REQUIRE(hashed_sort.Finalize(*connection.context, finalize) == SinkFinalizeType::NO_OUTPUT_POSSIBLE);
	auto global_source = hashed_sort.GetGlobalSourceState(*connection.context, *global_sink);
	REQUIRE(hashed_sort.GetHashGroups(*global_source).empty());
}

TEST_CASE("Eager HashedSort checks interruption before consuming local radix data", "[sort][api]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();

	vector<unique_ptr<Expression>> partition_bys;
	partition_bys.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	vector<BoundOrderByNode> order_bys;
	order_bys.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                       make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1));
	const vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER};
	const vector<unique_ptr<BaseStatistics>> partition_stats;
	HashedSort hashed_sort(*connection.context, partition_bys, order_bys, types, partition_stats,
	                       2 * STANDARD_VECTOR_SIZE);

	ThreadContext thread(*connection.context);
	ExecutionContext context(*connection.context, thread, nullptr);
	InterruptState interrupt;
	auto global_sink = CreateEagerHashedSortSinkState(hashed_sort, *connection.context);
	auto local_sink = hashed_sort.GetLocalSinkState(context);
	for (idx_t chunk_idx = 0; chunk_idx < 2; chunk_idx++) {
		DataChunk input;
		input.Initialize(*connection.context, types);
		for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
			const auto id = NumericCast<int32_t>(chunk_idx * STANDARD_VECTOR_SIZE + row_idx);
			input.SetValue(0, row_idx, Value::INTEGER(42));
			input.SetValue(1, row_idx, Value::INTEGER(id));
		}
		input.SetCardinality(STANDARD_VECTOR_SIZE);
		OperatorSinkInput sink {*global_sink, *local_sink, interrupt};
		REQUIRE(hashed_sort.Sink(context, input, sink) == SinkResultType::NEED_MORE_INPUT);
	}

	connection.context->interrupted.store(true, std::memory_order_relaxed);
	OperatorSinkCombineInput combine {*global_sink, *local_sink, interrupt};
	REQUIRE_THROWS_AS(hashed_sort.Combine(context, combine), InterruptException);
	connection.context->interrupted.store(false, std::memory_order_relaxed);
	REQUIRE(hashed_sort.Combine(context, combine) == SinkCombineResultType::FINISHED);

	OperatorSinkFinalizeInput finalize {*global_sink, interrupt};
	REQUIRE(hashed_sort.Finalize(*connection.context, finalize) == SinkFinalizeType::READY);
	auto global_source = hashed_sort.GetGlobalSourceState(*connection.context, *global_sink);
	const auto &groups = hashed_sort.GetHashGroups(*global_source);
	idx_t scanned_count = 0;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		if (!groups[group_idx].count) {
			continue;
		}
		auto scan = CreateHashedSortGroupScan(hashed_sort, context, group_idx, *global_source, interrupt);
		DataChunk output;
		output.Initialize(*connection.context, types);
		while (scan->GetData(output) != SourceResultType::FINISHED) {
			scanned_count += output.size();
			output.Reset();
		}
	}
	REQUIRE(scanned_count == 2 * STANDARD_VECTOR_SIZE);
}
