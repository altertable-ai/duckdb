#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/connection.hpp"

#include <sys/stat.h>

using namespace duckdb;

static string BenchAttachDir() {
	string dir = "/tmp/duckdb_connection_local_attach_bench";
	mkdir(dir.c_str(), 0755);
	return dir;
}

static string BenchAttachPath(idx_t i) {
	return BenchAttachDir() + "/db_" + to_string(i) + ".db";
}

DUCKDB_BENCHMARK(ConnectionLocalAttachGlobal, "[connection_local_attach]")
void Load(DuckDBBenchmarkState *state) override {
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 16; i++) {
		auto path = BenchAttachPath(i);
		state->conn.Query("ATTACH '" + path + "' AS g" + to_string(i));
		state->conn.Query("DETACH g" + to_string(i));
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Global ATTACH/DETACH of 16 temporary databases";
}
FINISH_BENCHMARK(ConnectionLocalAttachGlobal)

DUCKDB_BENCHMARK(ConnectionLocalAttachSession, "[connection_local_attach]")
void Load(DuckDBBenchmarkState *state) override {
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 16; i++) {
		auto path = BenchAttachPath(1000 + i);
		state->conn.Query("ATTACH '" + path + "' AS s" + to_string(i) + " (SCOPE SESSION)");
		state->conn.Query("DETACH s" + to_string(i) + " (SCOPE SESSION)");
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Session ATTACH/DETACH of 16 temporary databases";
}
FINISH_BENCHMARK(ConnectionLocalAttachSession)

DUCKDB_BENCHMARK(ConnectionLocalAttachLookup, "[connection_local_attach]")
void Load(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 64; i++) {
		auto path = BenchAttachPath(2000 + i);
		state->conn.Query("ATTACH '" + path + "' AS l" + to_string(i) + " (SCOPE SESSION)");
		state->conn.Query("CREATE TABLE l" + to_string(i) + ".t AS SELECT 1 AS i");
	}
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 64; i++) {
		state->conn.Query("SELECT i FROM l" + to_string(i) + ".t");
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Catalog lookup across 64 session attachments";
}
FINISH_BENCHMARK(ConnectionLocalAttachLookup)

DUCKDB_BENCHMARK(ConnectionLocalAttachCloneInherit, "[connection_local_attach]")
void Load(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 16; i++) {
		auto path = BenchAttachPath(3000 + i);
		state->conn.Query("ATTACH '" + path + "' AS c" + to_string(i) + " (SCOPE SESSION)");
	}
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	for (idx_t i = 0; i < 32; i++) {
		auto cloned = state->conn.Clone(ConnectionCloneMode::INHERIT_SESSION_ATTACHMENTS);
		cloned.Query("SELECT count(*) FROM duckdb_databases()");
	}
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Clone with INHERIT_SESSION_ATTACHMENTS over 16 session aliases";
}
FINISH_BENCHMARK(ConnectionLocalAttachCloneInherit)
