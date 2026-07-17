//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/attachment_namespace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/enums/attachment_scope.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

class ClientContext;

//! Immutable view of a session-scoped attachment binding.
struct AttachmentBinding {
	string alias;
	shared_ptr<AttachedDatabase> database;
	AccessMode access_mode = AccessMode::READ_WRITE;
	idx_t generation = 0;
	AttachVisibility visibility = AttachVisibility::SHOWN;

	bool IsReadOnly() const {
		return access_mode == AccessMode::READ_ONLY;
	}
};

//! Per-connection attachment namespace. Owns session-scoped aliases that resolve to
//! instance-managed physical AttachedDatabase objects.
//!
//! Lock order (must be respected by all callers):
//!   ClientContext::context_lock -> AttachmentNamespace::lock -> DatabaseManager::databases_lock
//!   -> DatabaseFilePathManager::db_paths_lock
class AttachmentNamespace {
public:
	AttachmentNamespace() = default;

	//! Register or replace a session binding. Returns the inserted binding generation.
	idx_t Attach(const string &alias, shared_ptr<AttachedDatabase> database, AccessMode access_mode,
	             AttachVisibility visibility = AttachVisibility::SHOWN);
	//! Remove a session binding. Returns the detached physical database if one was present.
	shared_ptr<AttachedDatabase> Detach(const string &alias);
	//! Lookup a binding by alias (returns a copy; empty alias means not found).
	bool TryGetBinding(const string &alias, AttachmentBinding &out) const;
	//! Owning lookup of the physical database for an alias.
	shared_ptr<AttachedDatabase> GetDatabase(const string &alias) const;
	//! Snapshot of all session bindings (for metadata / cloning).
	vector<AttachmentBinding> List() const;
	//! Copy all bindings into another namespace (used by Connection::Clone).
	void CopyTo(AttachmentNamespace &other) const;
	//! Drop every session binding and return the physical databases that were held.
	vector<shared_ptr<AttachedDatabase>> DetachAll();
	bool Empty() const;
	idx_t Count() const;

private:
	mutable mutex lock;
	case_insensitive_map_t<AttachmentBinding> bindings;
	atomic<idx_t> next_generation {1};
};

} // namespace duckdb
