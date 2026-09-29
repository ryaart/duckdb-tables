//! Table functions built from typed column declarations.
//!
//! A table declares its columns as (name, type, doc, getter). Getters run only for the
//! columns a query selects, so expensive ones cost nothing when unused.
//!
//! - `ListTable`: all rows come from one call (processes, users, a Kubernetes list).
//! - `ScanTable`: rows come from many units of work (files), scanned in parallel batches
//!   and streamed, so large scans don't need to fit in memory.

use crate::cell::{Cell, ColType, emit};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle},
    ffi,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, Value},
};
use rayon::prelude::*;
use std::{error::Error, ffi::CString, marker::PhantomData, sync::Mutex};

/// Units run on the rayon pool, so errors must be sendable.
pub type BoxError = Box<dyn Error + Send + Sync>;

/// Units scanned per parallel batch. Bounds memory while keeping all cores busy.
const BATCH: usize = 64;

pub struct Column<T: 'static> {
    pub name: &'static str,
    pub ty: ColType,
    pub doc: &'static str,
    pub get: fn(&T) -> Cell,
}

/// A table function parameter.
pub struct Param {
    pub name: &'static str,
    pub ty: ColType,
    pub doc: &'static str,
}

pub trait ScanTable: 'static {
    type Item: 'static;
    /// One unit of work, such as a file.
    type Unit: Send + Sync + 'static;
    /// What `bind` extracts from the call: parameters and settings.
    type Args: Send + Sync + 'static;
    fn doc() -> &'static str;
    fn columns() -> &'static [Column<Self::Item>];
    fn positional() -> &'static [Param] {
        &[]
    }
    fn named() -> &'static [Param] {
        &[]
    }
    fn bind(bind: &Bind) -> Result<Self::Args, BoxError>;
    fn units(args: &Self::Args) -> Result<Vec<Self::Unit>, BoxError>;
    /// Calls `emit` with each item of one unit. Runs on the rayon pool.
    fn scan(unit: &Self::Unit, args: &Self::Args, emit: &mut dyn FnMut(&Self::Item)) -> Result<(), BoxError>;
    /// Whether a unit that fails is skipped instead of failing the query.
    fn skip_errors(_args: &Self::Args) -> bool {
        false
    }
}

pub trait ListTable: 'static {
    type Item: 'static;
    type Args: Send + Sync + 'static;
    fn doc() -> &'static str;
    fn columns() -> &'static [Column<Self::Item>];
    fn positional() -> &'static [Param] {
        &[]
    }
    fn named() -> &'static [Param] {
        &[]
    }
    fn bind(bind: &Bind) -> Result<Self::Args, BoxError>;
    fn list(args: &Self::Args) -> Result<Vec<Self::Item>, BoxError>;
}

/// A `ListTable` is a `ScanTable` with a single unit.
pub struct Listed<T>(PhantomData<T>);

impl<T: ListTable> ScanTable for Listed<T> {
    type Item = T::Item;
    type Unit = ();
    type Args = T::Args;

    fn doc() -> &'static str {
        T::doc()
    }
    fn columns() -> &'static [Column<T::Item>] {
        T::columns()
    }
    fn positional() -> &'static [Param] {
        T::positional()
    }
    fn named() -> &'static [Param] {
        T::named()
    }
    fn bind(bind: &Bind) -> Result<T::Args, BoxError> {
        T::bind(bind)
    }
    fn units(_: &T::Args) -> Result<Vec<()>, BoxError> {
        Ok(vec![()])
    }
    fn scan(_: &(), args: &T::Args, emit: &mut dyn FnMut(&T::Item)) -> Result<(), BoxError> {
        T::list(args)?.iter().for_each(emit);
        Ok(())
    }
}

/// Parameters and settings for one call, available while binding.
pub struct Bind<'a>(&'a BindInfo);

impl Bind<'_> {
    pub fn positional(&self, index: u64) -> Value {
        self.0.get_parameter(index)
    }

    /// A named parameter, or None if it wasn't given or is NULL.
    pub fn named(&self, name: &str) -> Option<Value> {
        self.0.get_named_parameter(name).filter(|v| !v.is_null())
    }

    pub fn named_str(&self, name: &str) -> Option<String> {
        self.named(name).map(|v| v.to_string())
    }

    pub fn named_i64(&self, name: &str) -> Option<i64> {
        self.named(name).map(|v| v.to_int64())
    }

    pub fn named_bool(&self, name: &str) -> Option<bool> {
        self.named(name).map(|v| v.to_bool())
    }

    /// A setting's current value, or None if it isn't registered.
    pub fn setting(&self, name: &str) -> Option<Value> {
        // duckdb-rs doesn't expose the raw bind info, which the client context needs.
        // BindInfo is a single-pointer struct; the assert guards the layout at compile time.
        const _: () = assert!(size_of::<BindInfo>() == size_of::<ffi::duckdb_bind_info>());
        let name = CString::new(name).ok()?;
        unsafe {
            let info: ffi::duckdb_bind_info = std::mem::transmute_copy(self.0);
            let mut ctx: ffi::duckdb_client_context = std::ptr::null_mut();
            ffi::duckdb_table_function_get_client_context(info, &mut ctx);
            if ctx.is_null() {
                return None;
            }
            let value = ffi::duckdb_client_context_get_config_option(ctx, name.as_ptr(), std::ptr::null_mut());
            ffi::duckdb_destroy_client_context(&mut ctx);
            (!value.is_null()).then(|| Value::from(value))
        }
    }
}

struct Scan<U> {
    units: Vec<U>,
    next: usize,
    buffer: Vec<Vec<Cell>>,
    pos: usize,
}

pub struct ScanState<T: ScanTable> {
    /// The selected columns in output order; None for indices we don't define (row id).
    projected: Vec<Option<&'static Column<T::Item>>>,
    types: Vec<ColType>,
    scan: Mutex<Scan<T::Unit>>,
}

pub struct ScanVTab<T: ScanTable>(PhantomData<T>);

impl<T: ScanTable> VTab for ScanVTab<T> {
    type BindData = T::Args;
    type InitData = ScanState<T>;

    fn bind(bind: &BindInfo) -> Result<T::Args, Box<dyn Error>> {
        for c in T::columns() {
            bind.add_result_column(c.name, c.ty.logical_type());
        }
        T::bind(&Bind(bind)).map_err(|e| e as Box<dyn Error>)
    }

    fn init(init: &InitInfo) -> Result<ScanState<T>, Box<dyn Error>> {
        let args = unsafe { &*init.get_bind_data::<T::Args>() };
        let columns = T::columns();
        let projected: Vec<_> = init.get_column_indices().into_iter().map(|i| columns.get(i as usize)).collect();
        let types = projected.iter().map(|c| c.map_or(ColType::Bigint, |c| c.ty)).collect();
        let units = T::units(args).map_err(|e| e as Box<dyn Error>)?;
        init.set_max_threads(1);
        Ok(ScanState {
            projected,
            types,
            scan: Mutex::new(Scan {
                units,
                next: 0,
                buffer: Vec::new(),
                pos: 0,
            }),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let state = func.get_init_data();
        let args = func.get_bind_data();
        let mut scan = state.scan.lock().unwrap();

        // Refill from the next batch of units until there are rows or no units left.
        while scan.pos >= scan.buffer.len() && scan.next < scan.units.len() {
            let end = (scan.next + BATCH).min(scan.units.len());
            let results: Vec<Result<Vec<Vec<Cell>>, BoxError>> = scan.units[scan.next..end]
                .par_iter()
                .map(|unit| {
                    let mut rows = Vec::new();
                    T::scan(unit, args, &mut |item| {
                        rows.push(state.projected.iter().map(|c| c.map_or(Cell::Null, |c| (c.get)(item))).collect());
                    })?;
                    Ok(rows)
                })
                .collect();
            scan.next = end;
            scan.buffer.clear();
            scan.pos = 0;
            for result in results {
                match result {
                    Ok(rows) => scan.buffer.extend(rows),
                    Err(_) if T::skip_errors(args) => {}
                    Err(e) => return Err(e.to_string().into()),
                }
            }
        }

        let capacity = unsafe { ffi::duckdb_vector_size() } as usize;
        let start = scan.pos;
        let end = (start + capacity).min(scan.buffer.len());
        emit(output, &state.types, &scan.buffer[start..end]);
        scan.pos = end;
        Ok(())
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(T::positional().iter().map(|p| p.ty.logical_type()).collect())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(T::named().iter().map(|p| (p.name.to_string(), p.ty.logical_type())).collect())
    }
}
