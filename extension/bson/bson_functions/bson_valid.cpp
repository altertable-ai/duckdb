#include "bson_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

static void BSONValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bson_vec = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, bool>(bson_vec, result, count, [&](string_t bson) {
		const auto data = reinterpret_cast<const uint8_t *>(bson.GetData());
		const auto size = bson.GetSize();
		return BSONCommon::ValidateDocument(data, size);
	});
}

ScalarFunctionSet BSONFunctions::GetValidFunction() {
	ScalarFunctionSet set("bson_valid");
	set.AddFunction(ScalarFunction({LogicalType::BSON()}, LogicalType::BOOLEAN, BSONValidFunction));
	return set;
}

} // namespace duckdb
