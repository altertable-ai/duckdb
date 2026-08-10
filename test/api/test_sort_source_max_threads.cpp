#include "catch.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "test_helpers.hpp"

#include <type_traits>

using namespace duckdb;

static void PopulateSortSource(Sort &sort, GlobalSinkState &global_sink, ClientContext &client,
                               ExecutionContext &context, InterruptState &interrupt) {
	auto local_sink = sort.GetLocalSinkState(context);

	DataChunk input;
	input.Initialize(client, {LogicalType::INTEGER});
	for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
		input.SetValue(0, row_idx, Value::INTEGER(NumericCast<int32_t>(row_idx)));
	}
	input.SetCardinality(STANDARD_VECTOR_SIZE);

	OperatorSinkInput sink {global_sink, *local_sink, interrupt};
	for (idx_t chunk_idx = 0; chunk_idx < 3; chunk_idx++) {
		REQUIRE(sort.Sink(context, input, sink) == SinkResultType::NEED_MORE_INPUT);
	}
	OperatorSinkCombineInput combine {global_sink, *local_sink, interrupt};
	REQUIRE(sort.Combine(context, combine) == SinkCombineResultType::FINISHED);
	OperatorSinkFinalizeInput finalize {global_sink, interrupt};
	REQUIRE(sort.Finalize(client, finalize) == SinkFinalizeType::READY);
}

TEST_CASE("Sort source consumer count can be configured before scanning", "[sort][api]") {
	using SetSourceMaxThreadsSignature = void (Sort::*)(GlobalSourceState &, idx_t) const;
	static_assert(!std::is_polymorphic<Sort>::value, "Sort must not gain a vtable");
	static_assert(std::is_same<decltype(&Sort::SetSourceMaxThreads), SetSourceMaxThreadsSignature>::value,
	              "unexpected sort source thread configuration signature");

	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection connection(db);
	connection.BeginTransaction();
	connection.ForceParallelism();

	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	Sort sort(*connection.context, orders, {LogicalType::INTEGER}, {});
	ThreadContext thread(*connection.context);
	ExecutionContext context(*connection.context, thread, nullptr);
	InterruptState interrupt;

	auto global_sink = sort.GetGlobalSinkState(*connection.context);
	PopulateSortSource(sort, *global_sink, *connection.context, context, interrupt);
	auto source = sort.GetGlobalSourceState(*connection.context, *global_sink);
	REQUIRE(source->MaxThreads() == 3);

	sort.SetSourceMaxThreads(*source, 1);
	REQUIRE(source->MaxThreads() == 1);

	sort.SetSourceMaxThreads(*source, 0);
	REQUIRE(source->MaxThreads() == 1);

	auto local_source = sort.GetLocalSourceState(context, *source);
	DataChunk output;
	output.Initialize(*connection.context, {LogicalType::INTEGER});
	OperatorSourceInput source_input {*source, *local_source, interrupt};
	REQUIRE(sort.GetData(context, output, source_input) == SourceResultType::HAVE_MORE_OUTPUT);
	REQUIRE_THROWS_AS(sort.SetSourceMaxThreads(*source, 2), InvalidInputException);

	sort.DestroySource(*source, *local_source);
}
