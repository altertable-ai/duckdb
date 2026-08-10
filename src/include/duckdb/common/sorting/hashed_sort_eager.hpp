//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort_eager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/hashed_sort.hpp"

namespace duckdb {

//! Creates an opt-in HashedSort sink state that sorts each local radix bin during Combine.
//! This state must only be consumed through CreateHashedSortGroupScan.
DUCKDB_API unique_ptr<GlobalSinkState> CreateEagerHashedSortSinkState(const HashedSort &hashed_sort,
                                                                      ClientContext &client);

} // namespace duckdb
