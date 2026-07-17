#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/attachment_namespace.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalDetach::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &db_manager = DatabaseManager::Get(context.client);

	// Reject detach if the current transaction already has outstanding work on the database.
	// Important: do not use GetDatabase(context, ...) here — that pins the database in the
	// meta-transaction and prevents the subsequent detach from closing the file handle.
	// Lookup mirrors DETACH resolution: session binding first, then global.
	{
		shared_ptr<AttachedDatabase> attached_db =
		    ClientData::Get(context.client).attachment_namespace->GetDatabase(info->name);
		if (!attached_db) {
			attached_db = db_manager.GetDatabase(info->name);
		}
		if (attached_db) {
			auto &meta_transaction = MetaTransaction::Get(context.client);
			if (meta_transaction.TryGetTransaction(*attached_db)) {
				throw TransactionException(
				    "Cannot detach database \"%s\" because the current transaction has outstanding "
				    "work on it - commit or rollback first",
				    info->name);
			}
		}
	}

	db_manager.DetachDatabase(context.client, info->name, info->if_not_found);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
