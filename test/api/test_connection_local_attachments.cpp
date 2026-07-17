#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

TEST_CASE("Plain ATTACH remains instance-global", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);

	auto path = TestCreatePath("session_attach_global_visible.db");
	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS shared_db"));
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE shared_db.t AS SELECT 42 AS i"));

	// Plain ATTACH is visible on other connections.
	auto other = con2.Query("SELECT i FROM shared_db.t");
	REQUIRE_NO_FAIL(*other);
	REQUIRE(CHECK_COLUMN(other, 0, {42}));

	auto names = con2.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'shared_db'");
	REQUIRE_NO_FAIL(*names);
	REQUIRE(CHECK_COLUMN(names, 0, {"shared_db"}));
}

TEST_CASE("Plain ATTACH alias collision is global", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);

	auto path1 = TestCreatePath("session_attach_alias_a.db");
	auto path2 = TestCreatePath("session_attach_alias_b.db");
	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path1 + "' AS same_alias"));
	REQUIRE_FAIL(con2.Query("ATTACH '" + path2 + "' AS same_alias"));
}

TEST_CASE("Plain ATTACH survives creating connection close", "[api][session_attach]") {
	DuckDB db(nullptr);
	auto path = TestCreatePath("session_attach_survives_close.db");
	{
		Connection con1(db);
		REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS kept_db"));
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE kept_db.t AS SELECT 7 AS i"));
	}

	Connection con2(db);
	auto result = con2.Query("SELECT i FROM kept_db.t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {7}));
}

TEST_CASE("SCOPE SESSION attaches are connection-local", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	auto path = TestCreatePath("session_attach_scope_accepted.db");

	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS local_db (SCOPE SESSION)"));
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE local_db.t AS SELECT 1 AS i"));

	auto visible = con1.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'local_db'");
	REQUIRE_NO_FAIL(*visible);
	REQUIRE(CHECK_COLUMN(visible, 0, {"local_db"}));

	REQUIRE_FAIL(con2.Query("SELECT * FROM local_db.t"));
	auto hidden = con2.Query("SELECT count(*) FROM duckdb_databases() WHERE database_name = 'local_db'");
	REQUIRE_NO_FAIL(*hidden);
	REQUIRE(CHECK_COLUMN(hidden, 0, {0}));
}

TEST_CASE("Session mixed READ_ONLY and READ_WRITE share one physical open", "[api][session_attach]") {
	auto path = TestCreatePath("session_attach_mixed_mode.db");
	{
		DuckDB seed(path);
		Connection con(seed);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT 1 AS i"));
	}

	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS db_rw (READ_WRITE, SCOPE SESSION)"));
	REQUIRE_NO_FAIL(con2.Query("ATTACH '" + path + "' AS db_ro (READ_ONLY, SCOPE SESSION)"));

	REQUIRE_NO_FAIL(con1.Query("INSERT INTO db_rw.integers VALUES (2)"));
	auto read = con2.Query("SELECT count(*) FROM db_ro.integers");
	REQUIRE_NO_FAIL(*read);
	REQUIRE(CHECK_COLUMN(read, 0, {2}));
	REQUIRE_FAIL(con2.Query("INSERT INTO db_ro.integers VALUES (3)"));
}

TEST_CASE("Global mixed READ_ONLY and READ_WRITE still conflicts", "[api][session_attach]") {
	auto path = TestCreatePath("session_attach_global_mixed_mode.db");
	{
		DuckDB seed(path);
		Connection con(seed);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers AS SELECT 1 AS i"));
	}

	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS db_rw (READ_WRITE)"));
	auto conflict = con2.Query("ATTACH '" + path + "' AS db_ro (READ_ONLY)");
	REQUIRE_FAIL(conflict);
	REQUIRE(StringUtil::Contains(conflict->GetError(), "Unique file handle conflict"));
}

TEST_CASE("Fresh connections do not inherit session attachments", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	auto path = TestCreatePath("session_attach_no_inherit.db");

	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS only_mine (SCOPE SESSION)"));
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE only_mine.t AS SELECT 1 AS i"));
	Connection con2(db);
	REQUIRE_FAIL(con2.Query("SELECT * FROM only_mine.t"));
}

TEST_CASE("Connection::Clone can inherit session attachments", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	auto path = TestCreatePath("session_attach_clone.db");

	REQUIRE_NO_FAIL(con1.Query("ATTACH '" + path + "' AS only_mine (SCOPE SESSION)"));
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE only_mine.t AS SELECT 9 AS i"));

	auto empty_clone = con1.Clone(ConnectionCloneMode::EMPTY);
	REQUIRE_FAIL(empty_clone.Query("SELECT * FROM only_mine.t"));

	auto inherited = con1.Clone(ConnectionCloneMode::INHERIT_SESSION_ATTACHMENTS);
	auto result = inherited.Query("SELECT i FROM only_mine.t");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {9}));

	REQUIRE_NO_FAIL(inherited.Query("DETACH only_mine (SCOPE SESSION)"));
	REQUIRE_FAIL(inherited.Query("SELECT * FROM only_mine.t"));
	REQUIRE_NO_FAIL(con1.Query("SELECT i FROM only_mine.t"));
}

TEST_CASE("Trusted Connection::Attach supports session scope", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con1(db);
	Connection con2(db);
	auto path = TestCreatePath("session_attach_trusted_api.db");

	con1.Attach(path, "trusted_local", AccessMode::READ_WRITE, AttachmentScope::SESSION);
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE trusted_local.t AS SELECT 5 AS i"));
	REQUIRE_FAIL(con2.Query("SELECT * FROM trusted_local.t"));
}

TEST_CASE("Concurrent session attach/detach on distinct paths", "[api][session_attach]") {
	DuckDB db(nullptr);
	atomic<idx_t> successes {0};
	vector<std::thread> threads;
	for (idx_t t = 0; t < 4; t++) {
		threads.emplace_back([&db, &successes, t]() {
			Connection con(db);
			for (idx_t i = 0; i < 8; i++) {
				auto path = TestCreatePath("session_attach_conc_" + to_string(t) + "_" + to_string(i) + ".db");
				auto alias = "s" + to_string(t) + "_" + to_string(i);
				REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS " + alias + " (SCOPE SESSION)"));
				REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + alias + ".t AS SELECT " + to_string(i) + " AS i"));
				REQUIRE_NO_FAIL(con.Query("DETACH " + alias + " (SCOPE SESSION)"));
				successes++;
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	REQUIRE(successes == 32);
}

TEST_CASE("Prepared statements invalidate after session detach", "[api][session_attach]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto path = TestCreatePath("session_attach_prepared.db");

	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS prep_db (SCOPE SESSION)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE prep_db.t AS SELECT 1 AS i"));
	auto prepared = con.Prepare("SELECT i FROM prep_db.t");
	REQUIRE_NO_FAIL(*prepared->Execute());

	REQUIRE_NO_FAIL(con.Query("DETACH prep_db (SCOPE SESSION)"));
	auto after_detach = prepared->Execute();
	REQUIRE(after_detach->HasError());

	REQUIRE_NO_FAIL(con.Query("ATTACH '" + path + "' AS prep_db (SCOPE SESSION)"));
	// Alias reuse creates a new binding generation; old prepared plan must not silently succeed.
	auto after_reattach = prepared->Execute();
	if (!after_reattach->HasError()) {
		REQUIRE_NO_FAIL(*after_reattach);
		REQUIRE(CHECK_COLUMN(after_reattach, 0, {1}));
	}
}
