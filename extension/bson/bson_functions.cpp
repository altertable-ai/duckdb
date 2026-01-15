#include "bson_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

vector<ScalarFunctionSet> BSONFunctions::GetScalarFunctions() {
	vector<ScalarFunctionSet> functions;

	functions.push_back(GetValidFunction());
	functions.push_back(GetExistsFunction());
	functions.push_back(GetTypeFunction());
	functions.push_back(GetExtractFunction());
	functions.push_back(GetExtractStringFunction());
	functions.push_back(GetJSONToBSONFunction());

	return functions;
}

static bool CastBlobToBSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	// Validate BSON documents
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
		    const auto data = reinterpret_cast<const uint8_t *>(input.GetData());
		    const auto size = input.GetSize();

		    if (!BSONCommon::ValidateDocument(data, size)) {
			    mask.SetInvalid(idx);
			    HandleCastError::AssignError("Invalid BSON document", parameters);
		    }

		    return input;
	    });

	// Share the buffer since we didn't modify anything
	if (source.GetType().InternalType() == PhysicalType::VARCHAR) {
		StringVector::AddHeapReference(result, source);
	}
	return true;
}

void BSONFunctions::RegisterSimpleCastFunctions(ExtensionLoader &loader) {
	// BSON to BLOB is free (reinterpret)
	loader.RegisterCastFunction(LogicalType::BSON(), LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);

	// BLOB to BSON requires validation
	BoundCastInfo blob_to_bson_info(CastBlobToBSON);
	loader.RegisterCastFunction(LogicalType::BLOB, LogicalType::BSON(), std::move(blob_to_bson_info), 100);
}

} // namespace duckdb
