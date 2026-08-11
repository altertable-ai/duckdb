//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort_lazy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/hashed_sort.hpp"

namespace duckdb {

//! Creates an opt-in HashedSort sink state that defers per-bin Sort construction until SortColumnData.
//! Consume groups with CreateHashedSortGroupScan, then ReleaseHashedSortGroup to drop bin storage.
DUCKDB_API unique_ptr<GlobalSinkState> CreateLazyHashedSortSinkState(const HashedSort &hashed_sort,
                                                                     ClientContext &client);

//! Drops a hash bin's Sort state (and any leftover radix partition). Safe to call after the group
//! has been scanned, or repeatedly once the group is already absent.
DUCKDB_API void ReleaseHashedSortGroup(const HashedSort &hashed_sort, GlobalSinkState &global_sink, hash_t hash_bin);

} // namespace duckdb
