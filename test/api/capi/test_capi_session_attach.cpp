#include "capi_tester.hpp"

using namespace duckdb;

TEST_CASE("C API session attach and clone", "[capi][session_attach]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	auto path = TestCreatePath("capi_session_attach.db");
	auto options = duckdb_create_attach_options();
	REQUIRE(options);
	duckdb_attach_options_set_scope(options, DUCKDB_ATTACHMENT_SCOPE_SESSION);
	duckdb_attach_options_set_access_mode(options, DUCKDB_ATTACH_ACCESS_READ_WRITE);
	REQUIRE(duckdb_attach(tester.connection, path.c_str(), "capi_local", options) == DuckDBSuccess);
	duckdb_destroy_attach_options(&options);

	auto create = tester.Query("CREATE TABLE capi_local.t AS SELECT 3 AS i");
	REQUIRE_NO_FAIL(*create);

	duckdb_connection cloned_empty = nullptr;
	REQUIRE(duckdb_connection_clone(tester.connection, DUCKDB_CONNECTION_CLONE_EMPTY, &cloned_empty) == DuckDBSuccess);
	REQUIRE(cloned_empty);
	{
		duckdb_result result;
		REQUIRE(duckdb_query(cloned_empty, "SELECT * FROM capi_local.t", &result) == DuckDBError);
		duckdb_destroy_result(&result);
	}
	duckdb_disconnect(&cloned_empty);

	duckdb_connection cloned_inherit = nullptr;
	REQUIRE(duckdb_connection_clone(tester.connection, DUCKDB_CONNECTION_CLONE_INHERIT_SESSION_ATTACHMENTS,
	                                &cloned_inherit) == DuckDBSuccess);
	REQUIRE(cloned_inherit);
	{
		duckdb_result result;
		REQUIRE(duckdb_query(cloned_inherit, "SELECT i FROM capi_local.t", &result) == DuckDBSuccess);
		REQUIRE(duckdb_value_int32(&result, 0, 0) == 3);
		duckdb_destroy_result(&result);
	}
	duckdb_disconnect(&cloned_inherit);

	// Invalid arguments
	REQUIRE(duckdb_attach(nullptr, path.c_str(), "x", nullptr) == DuckDBError);
	REQUIRE(duckdb_connection_clone(nullptr, DUCKDB_CONNECTION_CLONE_EMPTY, &cloned_empty) == DuckDBError);
	REQUIRE(duckdb_connection_clone(tester.connection, DUCKDB_CONNECTION_CLONE_EMPTY, nullptr) == DuckDBError);
}
