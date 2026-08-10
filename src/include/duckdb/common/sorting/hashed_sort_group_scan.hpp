//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort_group_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/hashed_sort.hpp"

namespace duckdb {

class HashedSortGroupScan;

//! Claims and scans one finalized hash group. Each group can only be claimed once.
//! The HashedSort, its sink and source states, the execution context, and the interrupt state must outlive the scan.
DUCKDB_API unique_ptr<HashedSortGroupScan> CreateHashedSortGroupScan(const HashedSort &hashed_sort,
                                                                     ExecutionContext &context, hash_t hash_bin,
                                                                     GlobalSourceState &global_source,
                                                                     InterruptState &interrupt_state);

//! A single-consumer scan over one finalized HashedSort hash group.
class HashedSortGroupScan {
public:
	DUCKDB_API ~HashedSortGroupScan() noexcept;

	DUCKDB_API SourceResultType GetData(DataChunk &chunk);

private:
	struct Impl;

	HashedSortGroupScan();

	unique_ptr<Impl> impl;

	friend unique_ptr<HashedSortGroupScan> CreateHashedSortGroupScan(const HashedSort &hashed_sort,
	                                                                 ExecutionContext &context, hash_t hash_bin,
	                                                                 GlobalSourceState &global_source,
	                                                                 InterruptState &interrupt_state);
};

} // namespace duckdb
