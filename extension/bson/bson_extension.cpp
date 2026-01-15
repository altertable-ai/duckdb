#include "bson_extension.hpp"

#include "bson_common.hpp"
#include "bson_functions.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// BSON type
	auto bson_type = LogicalType::BSON();
	loader.RegisterType(LogicalType::BSON_TYPE_NAME, std::move(bson_type));

	// BSON casts
	BSONFunctions::RegisterSimpleCastFunctions(loader);
	BSONFunctions::RegisterJSONToBSONCast(loader);

	// BSON scalar functions
	for (auto &fun : BSONFunctions::GetScalarFunctions()) {
		loader.RegisterFunction(fun);
	}
}

void BsonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string BsonExtension::Name() {
	return "bson";
}

std::string BsonExtension::Version() const {
#ifdef EXT_VERSION_BSON
	return EXT_VERSION_BSON;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bson, loader) {
	duckdb::LoadInternal(loader);
}
}
