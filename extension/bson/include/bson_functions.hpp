//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bson_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"
#include "bson_common.hpp"

namespace duckdb {

struct CastParameters;

class BSONFunctions {
public:
	static vector<ScalarFunctionSet> GetScalarFunctions();
	static void RegisterSimpleCastFunctions(ExtensionLoader &loader);
	static void RegisterJSONToBSONCast(ExtensionLoader &loader);

private:
	// Scalar functions
	static ScalarFunctionSet GetValidFunction();
	static ScalarFunctionSet GetExistsFunction();
	static ScalarFunctionSet GetTypeFunction();
	static ScalarFunctionSet GetExtractFunction();
	static ScalarFunctionSet GetExtractStringFunction();
	static ScalarFunctionSet GetJSONToBSONFunction();
};

} // namespace duckdb
