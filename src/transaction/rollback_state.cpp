#include "duckdb/transaction/rollback_state.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/update_info.hpp"

#include "duckdb/storage/table/chunk_info.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/attachment_namespace.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/attachment_scope.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {

RollbackState::RollbackState(DuckTransaction &transaction_p) : transaction(transaction_p) {
}

void RollbackState::RollbackEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// Load and undo the catalog entry.
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->Undo(*catalog_entry);
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// revert the append in the base table
		info->table->RevertAppend(transaction, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = reinterpret_cast<DeleteInfo *>(data);
		// reset the deleted flag on rollback
		info->version_info->CommitDelete(info->vector_idx, NOT_DELETED_ID, *info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->segment->RollbackUpdate(*info);
		break;
	}
	case UndoFlags::ATTACHED_DATABASE: {
		auto ptr = data;
		auto scope = Load<AttachmentScope>(ptr);
		ptr += sizeof(AttachmentScope);
		auto alias_size = Load<uint32_t>(ptr);
		ptr += sizeof(uint32_t);
		string alias(char_ptr_cast(ptr), alias_size);
		ptr += alias_size;
		auto db = Load<AttachedDatabase *>(ptr);
		auto &db_manager = DatabaseManager::Get(db->GetDatabase());
		if (scope == AttachmentScope::SESSION) {
			// Do not call DetachDatabase here: MetaTransaction is already being torn down.
			auto context = transaction.context.lock();
			if (context) {
				auto attached_db = ClientData::Get(*context).attachment_namespace->Detach(alias);
				if (attached_db) {
					auto path = attached_db->StoredPath();
					if (attached_db.use_count() == 1) {
						db_manager.UnregisterPhysicalPath(path, *attached_db);
					}
					AttachedDatabase::InvokeCloseIfLastReference(attached_db);
				}
			}
		} else {
			db_manager.DetachInternal(db->name);
		}
		break;
	}
	case UndoFlags::SEQUENCE_VALUE:
		break;
	default: // LCOV_EXCL_START
		D_ASSERT(type == UndoFlags::EMPTY_ENTRY);
		break;
	} // LCOV_EXCL_STOP
}

} // namespace duckdb
