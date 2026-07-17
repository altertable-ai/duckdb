# Session-Scoped Attachments

DuckDB keeps plain `ATTACH` / `DETACH` **instance-global** for compatibility. This fork adds an explicit session scope for connection-local aliases over instance-managed physical databases.

## SQL syntax

```sql
ATTACH 'file.db' AS alias (SCOPE SESSION);
DETACH alias (SCOPE SESSION);
```

`SCOPE GLOBAL` is the default and matches historical DuckDB behavior.

Session attachments may also combine with access-mode options:

```sql
ATTACH 'file.db' AS ro (READ_ONLY, SCOPE SESSION);
ATTACH 'file.db' AS rw (READ_WRITE, SCOPE SESSION);
```

For native DuckDB files, compatible session bindings reuse one physical open. Binding-level `READ_ONLY` is enforced even when the physical handle is read-write.

## Resolution and metadata

Catalog lookup is **session-first, then global**:

1. TEMP / system catalogs
2. The connection’s session attachment namespace
3. The instance-global attachment registry

A session alias may shadow a global alias of the same name. Effective metadata (`duckdb_databases()`, `duckdb_schemas()`, `duckdb_tables()`, …) contains:

- all session aliases for the current connection
- global aliases not shadowed by the session

Another connection never observes a peer’s session bindings.

## Ownership and lifetime

| Object | Owner |
| --- | --- |
| Session alias / binding | `ClientData::attachment_namespace` |
| Physical `AttachedDatabase` | `DatabaseManager` (+ shared_ptr pins from bindings / transactions) |
| Path lease | `DatabaseFilePathManager` |

`DETACH … (SCOPE SESSION)` removes one local binding. `DETACH alias` (plain / global) removes one global binding. Checkpoint / close / path release run only when the final pin disappears.

Connection destruction rolls back the active transaction, clears prepared plans, then releases all session bindings. Clones and other connections are unaffected.

## Lock order

1. `ClientContext::context_lock`
2. `AttachmentNamespace` lock
3. `DatabaseManager::databases_lock`
4. `DatabaseFilePathManager` lock

Extension callbacks and checkpoints must not run while registry locks are held.

## Clone semantics

C++:

```cpp
auto empty = con.Clone(ConnectionCloneMode::EMPTY);
auto inherit = con.Clone(ConnectionCloneMode::INHERIT_SESSION_ATTACHMENTS);
```

C:

```c
duckdb_connection_clone(source, DUCKDB_CONNECTION_CLONE_INHERIT_SESSION_ATTACHMENTS, &out);
```

Inherited clones receive a snapshot of session bindings and a compatible search path. They always share global attachments from the instance. Transactions, TEMP catalogs, and prepared statements are not copied.

## Trusted attach API

Host code may attach without SQL:

```cpp
con.Attach(path, "alias", AccessMode::READ_ONLY, AttachmentScope::SESSION);
```

```c
duckdb_attach_options opts = duckdb_create_attach_options();
duckdb_attach_options_set_scope(opts, DUCKDB_ATTACHMENT_SCOPE_SESSION);
duckdb_attach(conn, path, "alias", opts);
duckdb_destroy_attach_options(&opts);
```

This API does **not** bypass filesystem authorization, allowed paths/directories, or `lock_configuration`. It also does not autoload storage extensions; the type must already be available.

## Prepared statements

`StatementProperties::CatalogIdentity` records catalog OID/version plus attachment scope and session binding generation. Detach, replace, or alias reuse that changes the generation forces rebind / failure on execute. Write checks use the binding access mode, not only the physical open mode.

## Extension catalogs

Unrelated connections do not share extension-provided physical catalogs created via session attach. Explicit clones may share the already-authorized physical object through inherited session bindings.

## Benchmark notes

Microbenchmarks live in `benchmark/micro/connection_local_attach.cpp` (group / name prefix `ConnectionLocalAttach*`).

Example release timings on a local machine (seconds, 5 runs each; lower is better):

| Benchmark | Median (approx.) |
| --- | --- |
| `ConnectionLocalAttachGlobal` | ~0.0024s |
| `ConnectionLocalAttachSession` | ~0.0024s |
| `ConnectionLocalAttachLookup` | ~0.0038s |
| `ConnectionLocalAttachCloneInherit` | ~0.0040s |

Run with:

```bash
BUILD_BENCHMARK=1 make release
build/release/benchmark/benchmark_runner 'ConnectionLocalAttach.*' --out=<artifact>
```

## Upgrade-sensitive maintenance

When changing attach plumbing, audit:

- `DatabaseManager` / `DatabaseFilePathManager` / `AttachedDatabase`
- attach undo records (`DuckTransaction::PushAttach`, rollback)
- catalog identity / versioning (`StatementProperties::CatalogIdentity`)
- metadata enumeration (`GetEffectiveDatabases`, `GetEffectiveAlias`)
- `StorageExtension` attach hooks
- C API header generation (`scripts/generate_c_api.py`)
- external duckdb-rs bindgen against the regenerated `duckdb.h`
