#include "bson_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void BSONExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
			    return bson;
		    }

		    BSONElement elem;
		    if (!BSONCommon::TraversePath(data, size, segments, elem)) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    if (elem.type == BSONType::DOCUMENT || elem.type == BSONType::ARRAY) {
			    return string_t((const char *)elem.value, elem.value_len);
		    } else {
			    mask.SetInvalid(idx);
			    return string_t();
		    }
	    });
}

ScalarFunctionSet BSONFunctions::GetExtractFunction() {
	ScalarFunctionSet set("bson_extract");
	set.AddFunction(
	    ScalarFunction({LogicalType::BSON(), LogicalType::VARCHAR}, LogicalType::BSON(), BSONExtractFunction));
	return set;
}

} // namespace duckdb
