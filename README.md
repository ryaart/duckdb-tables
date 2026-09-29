# duckdb-tables

Shared code for the read-only DuckDB extensions here: `os`, `binaries` and `k8s`. It's written for extensions that agents use to investigate systems.

```rust
use duckdb_tables::{Bind, BoxError, Cell, ColType, Column, Extension, ListTable};

struct Users;

impl ListTable for Users {
    type Item = User;
    type Args = ();
    fn doc() -> &'static str { "Local user accounts." }

    fn columns() -> &'static [Column<User>] {
        static COLUMNS: &[Column<User>] = &[
            Column { name: "uid", ty: ColType::Bigint, doc: "User ID.", get: |u| Cell::Int(u.uid.into()) },
            Column { name: "name", ty: ColType::Varchar, doc: "User name.", get: |u| u.name.clone().into() },
        ];
        COLUMNS
    }
    fn bind(_: &Bind) -> Result<(), BoxError> { Ok(()) }
    fn list(_: &()) -> Result<Vec<User>, BoxError> { /* ... */ }
}

duckdb_tables::entrypoint!(myext_init_c_api, init);

fn init(ext: &Extension) -> Result<(), BoxError> {
    ext.register_bool_setting("myext_redact", true, "Redact secrets.")?;
    ext.register_describe("myext_describe")?;
    ext.register::<Users>("myext_users")
}
```

What it provides:
- **`ListTable` and `ScanTable`.** A table declares its columns as (name, type, description, getter), and getters run only for the columns the query selects, so expensive columns cost nothing unless used. Both kinds support positional and named parameters.
  - A `ListTable` gets all its rows from one call (`os_processes`, `k8s_pods`).
  - A `ScanTable` gets rows from many units of work, such as files. Units are scanned in parallel batches of 64 on a rayon pool, and rows are streamed, so large scans don't need to fit in memory. Failing units can be skipped (`binaries` does this with globs). A `ListTable` is a `ScanTable` with one unit.
- **`<prefix>_describe()`.** Lists every function, parameter, column and setting with its description, so an agent can discover the schema from SQL.
- **Settings.** `register_bool_setting` creates a `SET`-able option, and `Bind::setting` reads it at bind time. A harness can freeze settings with `SET lock_configuration = true`.
- **`entrypoint!`.** Works like duckdb-rs's `#[duckdb_entrypoint_c_api]`, but also keeps the raw database handle, which registering settings needs.
- **`cell`.** `ColType`/`Cell` and chunked output, including `VARCHAR[]` and `TIMESTAMP`. It combines the versions from `duckdb-binaries` and `duckdb-k8s`.

Caveat: duckdb-rs doesn't expose the raw bind-info pointer, so `Bind::setting` reads it from `BindInfo`'s single private field. A compile-time size check guards that, and duckdb-rs is pinned (`~1.10505.0`).

The extensions use this crate as a path dependency (`../duckdb-tables`). Each extension is its own repository, so CI needs a git dependency instead once this crate has a remote.
