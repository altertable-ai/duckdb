#include "bson_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void BSONTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bson_vec = args.data[0];
	auto &path_vec = args.data[1];
	auto count = args.size();

	// Always parse path dynamically for simplicity (v1)
	BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
	    bson_vec, path_vec, result, count, [&](string_t bson, string_t path, ValidityMask &mask, idx_t idx) {
		    const auto data = reinterpret_cast<const uint8_t *>(bson.GetData());
		    const auto size = bson.GetSize();

		    vector<BSONCommon::PathSegment> segments;
		    BSONCommon::ParsePath(path.GetData(), path.GetSize(), segments);

		    if (segments.empty()) {
			    return string_t(BSONCommon::TYPE_STRING_DOCUMENT);
		    }

		    BSONElement elem;
		    if (!BSONCommon::TraversePath(data, size, segments, elem)) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    return string_t(BSONCommon::TypeToString(elem.type));
	    });
}

ScalarFunctionSet BSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("bson_type");
	set.AddFunction(
	    ScalarFunction({LogicalType::BSON(), LogicalType::VARCHAR}, LogicalType::VARCHAR, BSONTypeFunction));
	return set;
}

} // namespace duckdb
