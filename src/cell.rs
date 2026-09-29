//! Typed cells, and writing rows of them into DuckDB output chunks.

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
    Varchar,
    Bigint,
    /// Addresses and sizes that can exceed i64::MAX.
    Ubigint,
    Double,
    Boolean,
    /// Microseconds since the Unix epoch, UTC.
    Timestamp,
    VarcharList,
}

impl ColType {
    pub fn logical_type(self) -> LogicalTypeHandle {
        match self {
            ColType::Varchar => LogicalTypeId::Varchar.into(),
            ColType::Bigint => LogicalTypeId::Bigint.into(),
            ColType::Ubigint => LogicalTypeId::UBigint.into(),
            ColType::Double => LogicalTypeId::Double.into(),
            ColType::Boolean => LogicalTypeId::Boolean.into(),
            ColType::Timestamp => LogicalTypeId::Timestamp.into(),
            ColType::VarcharList => LogicalTypeHandle::list(&LogicalTypeId::Varchar.into()),
        }
    }

    /// The SQL spelling, for self-description.
    pub fn sql(self) -> &'static str {
        match self {
            ColType::Varchar => "VARCHAR",
            ColType::Bigint => "BIGINT",
            ColType::Ubigint => "UBIGINT",
            ColType::Double => "DOUBLE",
            ColType::Boolean => "BOOLEAN",
            ColType::Timestamp => "TIMESTAMP",
            ColType::VarcharList => "VARCHAR[]",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Null,
    Str(String),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    /// Microseconds since the Unix epoch, UTC.
    Ts(i64),
    List(Vec<String>),
}

impl From<String> for Cell {
    fn from(v: String) -> Self {
        Cell::Str(v)
    }
}

impl From<&str> for Cell {
    fn from(v: &str) -> Self {
        Cell::Str(v.to_string())
    }
}

impl From<i64> for Cell {
    fn from(v: i64) -> Self {
        Cell::Int(v)
    }
}

impl From<u64> for Cell {
    fn from(v: u64) -> Self {
        Cell::UInt(v)
    }
}

impl From<f64> for Cell {
    fn from(v: f64) -> Self {
        Cell::Float(v)
    }
}

impl From<bool> for Cell {
    fn from(v: bool) -> Self {
        Cell::Bool(v)
    }
}

impl From<Vec<String>> for Cell {
    fn from(v: Vec<String>) -> Self {
        Cell::List(v)
    }
}

impl<T: Into<Cell>> From<Option<T>> for Cell {
    fn from(v: Option<T>) -> Self {
        v.map_or(Cell::Null, Into::into)
    }
}

/// Writes `rows` (already projected, in output column order) into `output`.
/// A cell whose variant doesn't match its column type is written as NULL.
pub fn emit(output: &mut DataChunkHandle, types: &[ColType], rows: &[Vec<Cell>]) {
    for (col, ty) in types.iter().enumerate() {
        match ty {
            ColType::VarcharList => emit_list(output, col, rows),
            _ => emit_flat(output, col, *ty, rows),
        }
    }
    output.set_len(rows.len());
}

fn emit_flat(output: &mut DataChunkHandle, col: usize, ty: ColType, rows: &[Vec<Cell>]) {
    let mut vector = output.flat_vector(col);
    for (i, row) in rows.iter().enumerate() {
        match (&row[col], ty) {
            (Cell::Str(s), ColType::Varchar) => vector.insert(i, s.as_str()),
            (Cell::Int(v), ColType::Bigint) | (Cell::Ts(v), ColType::Timestamp) => unsafe {
                vector.as_mut_slice::<i64>()[i] = *v
            },
            (Cell::UInt(v), ColType::Ubigint) => unsafe { vector.as_mut_slice::<u64>()[i] = *v },
            (Cell::Float(v), ColType::Double) => unsafe { vector.as_mut_slice::<f64>()[i] = *v },
            (Cell::Bool(b), ColType::Boolean) => unsafe { vector.as_mut_slice::<bool>()[i] = *b },
            _ => vector.set_null(i),
        }
    }
}

fn emit_list(output: &mut DataChunkHandle, col: usize, rows: &[Vec<Cell>]) {
    let mut list = output.list_vector(col);
    let total: usize = rows
        .iter()
        .map(|r| match &r[col] {
            Cell::List(items) => items.len(),
            _ => 0,
        })
        .sum();
    let child = list.child(total);
    let mut offset = 0;
    for (i, row) in rows.iter().enumerate() {
        match &row[col] {
            Cell::List(items) => {
                for (j, item) in items.iter().enumerate() {
                    child.insert(offset + j, item.as_str());
                }
                list.set_entry(i, offset, items.len());
                offset += items.len();
            }
            _ => list.set_null(i),
        }
    }
    list.set_len(total);
}
