#include "bson_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void BSONExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bson_vec = args.data[0];
	auto &path_vec = args.data[1];
	auto count = args.size();

	// Always parse path dynamically for simplicity (v1)
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    bson_vec, path_vec, result, count, [&](string_t bson, string_t path) {
		    const auto data = reinterpret_cast<const uint8_t *>(bson.GetData());
		    const auto size = bson.GetSize();

		    vector<BSONCommon::PathSegment> segments;
		    BSONCommon::ParsePath(path.GetData(), path.GetSize(), segments);

		    if (segments.empty()) {
			    return BSONCommon::ValidateDocument(data, size);
		    }

		    BSONElement elem;
		    return BSONCommon::TraversePath(data, size, segments, elem);
	    });
}

ScalarFunctionSet BSONFunctions::GetExistsFunction() {
	ScalarFunctionSet set("bson_exists");
	set.AddFunction(
	    ScalarFunction({LogicalType::BSON(), LogicalType::VARCHAR}, LogicalType::BOOLEAN, BSONExistsFunction));
	return set;
}

} // namespace duckdb
