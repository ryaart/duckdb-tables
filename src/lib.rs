//! Read-only DuckDB table functions from Rust, for extensions meant to be used by agents.
//!
//! - `ListTable` (rows from one call) and `ScanTable` (rows from many units, such as files,
//!   scanned in parallel batches and streamed): declare columns as (name, type, doc, getter);
//!   only selected columns are computed.
//! - `Extension::register` / `register_scan`: register a table and record its docs.
//! - `Extension::register_describe`: a `<prefix>_describe()` table listing every function,
//!   column, parameter and setting with its description, so an agent can discover the schema.
//! - `Extension::register_bool_setting`: a `SET`-able option, read at bind with `Bind::setting`.
//!   Settings can be frozen with `SET lock_configuration = true`.
//! - `entrypoint!`: like duckdb-rs's `#[duckdb_entrypoint_c_api]`, but keeps the raw
//!   database handle, which registering settings needs.

pub mod cell;
pub mod table;

pub use cell::{Cell, ColType};
pub use duckdb;
pub use table::{Bind, BoxError, Column, ListTable, Listed, Param, ScanTable, ScanVTab};

use duckdb::{Connection, ffi};
use std::{ffi::CString, sync::Mutex};

/// One row of `<prefix>_describe()`.
#[derive(Clone)]
pub struct Doc {
    pub function: Option<String>,
    pub kind: &'static str,
    pub name: String,
    pub ty: &'static str,
    pub description: &'static str,
}

/// Each extension is its own cdylib, so each has its own registry.
static DOCS: Mutex<Vec<Doc>> = Mutex::new(Vec::new());

/// The registry is per process, but the extension is initialised once per database that
/// loads it, so entries that are already recorded are skipped.
fn document(doc: Doc) {
    let mut docs = DOCS.lock().unwrap();
    if !docs.iter().any(|d| d.function == doc.function && d.kind == doc.kind && d.name == doc.name) {
        docs.push(doc);
    }
}

pub struct Extension {
    pub con: Connection,
    db: ffi::duckdb_database,
}

impl Extension {
    pub fn register<T: ListTable>(&self, name: &str) -> Result<(), BoxError> {
        self.register_scan::<Listed<T>>(name)
    }

    pub fn register_scan<T: ScanTable>(&self, name: &str) -> Result<(), BoxError> {
        self.con.register_table_function::<ScanVTab<T>>(name)?;
        let row = |kind, n: &str, ty: ColType, description| Doc {
            function: Some(name.to_string()),
            kind,
            name: n.to_string(),
            ty: ty.sql(),
            description,
        };
        document(Doc {
            function: Some(name.to_string()),
            kind: "function",
            name: name.to_string(),
            ty: "TABLE",
            description: T::doc(),
        });
        for p in T::positional() {
            document(row("parameter", p.name, p.ty, p.doc));
        }
        for p in T::named() {
            document(row("named_parameter", p.name, p.ty, p.doc));
        }
        for c in T::columns() {
            document(row("column", c.name, c.ty, c.doc));
        }
        Ok(())
    }

    pub fn register_describe(&self, name: &str) -> Result<(), BoxError> {
        self.register::<Describe>(name)
    }

    /// Registers a global BOOLEAN setting.
    pub fn register_bool_setting(&self, name: &str, default: bool, description: &'static str) -> Result<(), BoxError> {
        let c_name = CString::new(name)?;
        let c_description = CString::new(description)?;
        unsafe {
            let mut con: ffi::duckdb_connection = std::ptr::null_mut();
            if ffi::duckdb_connect(self.db, &mut con) != ffi::DuckDBSuccess {
                return Err("could not connect to register a setting".into());
            }
            let mut option = ffi::duckdb_create_config_option();
            let mut ty = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN);
            let mut value = ffi::duckdb_create_bool(default);
            ffi::duckdb_config_option_set_name(option, c_name.as_ptr());
            ffi::duckdb_config_option_set_type(option, ty);
            ffi::duckdb_config_option_set_default_value(option, value);
            ffi::duckdb_config_option_set_description(option, c_description.as_ptr());
            let state = ffi::duckdb_register_config_option(con, option);
            ffi::duckdb_destroy_value(&mut value);
            ffi::duckdb_destroy_logical_type(&mut ty);
            ffi::duckdb_destroy_config_option(&mut option);
            ffi::duckdb_disconnect(&mut con);
            if state != ffi::DuckDBSuccess {
                return Err(format!("could not register setting {name}").into());
            }
        }
        document(Doc {
            function: None,
            kind: "setting",
            name: name.to_string(),
            ty: "BOOLEAN",
            description,
        });
        Ok(())
    }
}

struct Describe;

impl ListTable for Describe {
    type Item = Doc;
    type Args = ();
    fn doc() -> &'static str {
        "Every function, parameter, column and setting in this extension, with descriptions."
    }

    fn columns() -> &'static [Column<Doc>] {
        static COLUMNS: &[Column<Doc>] = &[
            Column { name: "function", ty: ColType::Varchar, doc: "Table function name; NULL for settings.", get: |d| d.function.clone().into() },
            Column { name: "kind", ty: ColType::Varchar, doc: "function, parameter, named_parameter, column or setting.", get: |d| d.kind.into() },
            Column { name: "name", ty: ColType::Varchar, doc: "Name of the function, parameter, column or setting.", get: |d| d.name.clone().into() },
            Column { name: "type", ty: ColType::Varchar, doc: "SQL type.", get: |d| d.ty.into() },
            Column { name: "description", ty: ColType::Varchar, doc: "What it means.", get: |d| d.description.into() },
        ];
        COLUMNS
    }

    fn bind(_: &Bind) -> Result<(), BoxError> {
        Ok(())
    }

    fn list(_: &()) -> Result<Vec<Doc>, BoxError> {
        Ok(DOCS.lock().unwrap().clone())
    }
}

#[doc(hidden)]
pub unsafe fn __entrypoint(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
    min_version: &str,
    init: fn(&Extension) -> Result<(), BoxError>,
) -> bool {
    let result = unsafe {
        (|| -> Result<bool, BoxError> {
            if !ffi::duckdb_rs_extension_api_init(info, access, min_version)? {
                return Ok(false); // API version mismatch; DuckDB reports it.
            }
            let get_database = (*access).get_database.ok_or("get_database is null in duckdb_extension_access")?;
            let db_ptr = get_database(info);
            if db_ptr.is_null() {
                return Ok(false);
            }
            let db = *db_ptr;
            let ext = Extension {
                con: Connection::open_from_raw(db.cast())?,
                db,
            };
            init(&ext)?;
            Ok(true)
        })()
    };
    match result {
        Ok(v) => v,
        Err(e) => {
            if let Some(set_error) = unsafe { (*access).set_error } {
                let msg = CString::new(e.to_string()).unwrap_or_else(|_| c"extension init failed".into());
                unsafe { set_error(info, msg.as_ptr()) };
            }
            false
        }
    }
}

/// Defines the extension's C entrypoint. `symbol` must be `<extension name>_init_c_api`.
///
/// ```ignore
/// duckdb_tables::entrypoint!(os_init_c_api, init);
/// // init: fn(&Extension) -> Result<(), BoxError>
/// fn init(ext: &Extension) -> Result<(), BoxError> { ext.register::<Processes>("os_processes") }
/// ```
#[macro_export]
macro_rules! entrypoint {
    ($symbol:ident, $init:path) => {
        /// # Safety
        /// Called by DuckDB when the extension is loaded.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $symbol(
            info: $crate::duckdb::ffi::duckdb_extension_info,
            access: *const $crate::duckdb::ffi::duckdb_extension_access,
        ) -> bool {
            let min_version = option_env!("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION").unwrap_or("v1.2.0");
            unsafe { $crate::__entrypoint(info, access, min_version, $init) }
        }
    };
}
