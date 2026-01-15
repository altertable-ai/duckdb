#include "bson_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

static void BSONExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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

		    BSONElement elem;
		    if (!BSONCommon::TraversePath(data, size, segments, elem)) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    if (elem.type != BSONType::STRING) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    int32_t str_len = BSONCommon::ReadInt32(elem.value);
		    if (str_len < 1) {
			    mask.SetInvalid(idx);
			    return string_t();
		    }

		    return string_t((const char *)(elem.value + 4), str_len - 1);
	    });
}

ScalarFunctionSet BSONFunctions::GetExtractStringFunction() {
	ScalarFunctionSet set("bson_extract_string");
	set.AddFunction(
	    ScalarFunction({LogicalType::BSON(), LogicalType::VARCHAR}, LogicalType::VARCHAR, BSONExtractStringFunction));
	return set;
}

} // namespace duckdb
