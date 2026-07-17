//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/attachment_scope.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! GLOBAL attachments are visible to every connection on the DatabaseInstance.
//! SESSION attachments are visible only to the creating ClientContext (and explicit clones).
enum class AttachmentScope : uint8_t { GLOBAL = 0, SESSION = 1 };

} // namespace duckdb
