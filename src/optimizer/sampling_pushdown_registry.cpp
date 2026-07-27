#include "duckdb/optimizer/sampling_pushdown_registry.hpp"

namespace duckdb {

mutex SamplingPushdownRegistry::registry_lock;
unordered_map<string, table_function_supports_sampling_pushdown_t> SamplingPushdownRegistry::callbacks;

void SamplingPushdownRegistry::Register(const string &function_name,
                                        table_function_supports_sampling_pushdown_t callback) {
	lock_guard<mutex> guard(registry_lock);
	callbacks[function_name] = callback;
}

table_function_supports_sampling_pushdown_t SamplingPushdownRegistry::Lookup(const string &function_name) {
	lock_guard<mutex> guard(registry_lock);
	auto entry = callbacks.find(function_name);
	if (entry == callbacks.end()) {
		return nullptr;
	}
	return entry->second;
}

} // namespace duckdb
