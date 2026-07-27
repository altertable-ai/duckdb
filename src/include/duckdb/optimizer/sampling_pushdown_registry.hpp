//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sampling_pushdown_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class FunctionData;
struct SampleOptions;

//! Optional per-table-function callback for sampling pushdown eligibility.
//! Registered out-of-band to preserve TableFunction ABI with prebuilt extensions.
typedef bool (*table_function_supports_sampling_pushdown_t)(const FunctionData &bind_data,
                                                            const SampleOptions &sample_options);

class SamplingPushdownRegistry {
public:
	DUCKDB_API static void Register(const string &function_name, table_function_supports_sampling_pushdown_t callback);
	DUCKDB_API static table_function_supports_sampling_pushdown_t Lookup(const string &function_name);

private:
	static mutex registry_lock;
	static unordered_map<string, table_function_supports_sampling_pushdown_t> callbacks;
};

} // namespace duckdb
