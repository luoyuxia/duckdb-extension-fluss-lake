// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema};
use arrow_array::ffi::to_ffi;
use arrow::array::StructArray;
use futures_util::TryStreamExt;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::io::FileIOBuilder;
use paimon::Result as PaimonResult;
use paimon::spec::DataType as PaimonDataType;

/// Convert Paimon DataType to Arrow DataType
fn paimon_to_arrow_type(dt: &PaimonDataType) -> ArrowDataType {
    match dt {
        PaimonDataType::Boolean(_) => ArrowDataType::Boolean,
        PaimonDataType::TinyInt(_) => ArrowDataType::Int8,
        PaimonDataType::SmallInt(_) => ArrowDataType::Int16,
        PaimonDataType::Int(_) => ArrowDataType::Int32,
        PaimonDataType::BigInt(_) => ArrowDataType::Int64,
        PaimonDataType::Float(_) => ArrowDataType::Float32,
        PaimonDataType::Double(_) => ArrowDataType::Float64,
        PaimonDataType::Decimal(d) => ArrowDataType::Decimal128(d.precision() as u8, d.scale() as i8),
        PaimonDataType::Char(_) | PaimonDataType::VarChar(_) => ArrowDataType::Utf8,
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) => ArrowDataType::Binary,
        PaimonDataType::Date(_) => ArrowDataType::Date32,
        PaimonDataType::Time(_) => ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond),
        PaimonDataType::Timestamp(_) | PaimonDataType::LocalZonedTimestamp(_) => {
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        }
        PaimonDataType::Array(_) => {
            // For now, use Utf8 as element type since we can't access private fields
            // TODO: Add public accessor methods to Paimon types or use pattern matching
            ArrowDataType::List(arrow_schema::Field::new("element", ArrowDataType::Utf8, true).into())
        }
        PaimonDataType::Map(_) => {
            // For now, use simple struct since we can't access private fields
            // TODO: Add public accessor methods to Paimon types
            let struct_fields: Fields = vec![
                Field::new("key", ArrowDataType::Utf8, false),
                Field::new("value", ArrowDataType::Utf8, true),
            ].into();
            ArrowDataType::Map(
                arrow_schema::Field::new(
                    "entries",
                    ArrowDataType::Struct(struct_fields),
                    false,
                ).into(),
                false,
            )
        }
        PaimonDataType::Multiset(_) => {
            // For now, use Utf8 as element type since we can't access private fields
            // TODO: Add public accessor methods to Paimon types
            ArrowDataType::List(arrow_schema::Field::new("element", ArrowDataType::Utf8, true).into())
        }
        _ => ArrowDataType::Utf8, // Default to Utf8 for unknown types
    }
}

/// Opaque handle for a Paimon catalog
#[repr(C)]
pub struct PaimonCatalog {
    catalog: Arc<dyn Catalog>,
    rt: tokio::runtime::Runtime,
}

/// Opaque handle for a Paimon table (schema only, no scan)
#[repr(C)]
pub struct PaimonTable {
    schema: Arc<ArrowSchema>,
}

/// Opaque handle for a Paimon table scan
#[repr(C)]
pub struct PaimonScan {
    batches: Vec<RecordBatch>,
    current_index: usize,
    schema: Arc<ArrowSchema>,
}

/// Error information
#[repr(C)]
pub struct PaimonError {
    message: *mut c_char,
}

/// Create a new Paimon catalog from a warehouse path
#[no_mangle]
pub extern "C" fn paimon_catalog_new(warehouse_path: *const c_char) -> *mut PaimonCatalog {
    if warehouse_path.is_null() {
        return ptr::null_mut();
    }

    let path = unsafe {
        match CStr::from_ptr(warehouse_path).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };

    let file_io = match FileIOBuilder::new("file").build() {
        Ok(io) => io,
        Err(_) => return ptr::null_mut(),
    };

    let catalog = FileSystemCatalog::new(path, file_io);
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    Box::into_raw(Box::new(PaimonCatalog {
        catalog,
        rt,
    }))
}

/// Free a Paimon catalog
#[no_mangle]
pub extern "C" fn paimon_catalog_free(catalog: *mut PaimonCatalog) {
    if !catalog.is_null() {
        unsafe {
            let _ = Box::from_raw(catalog);
        }
    }
}

/// Get table schema without scanning
#[no_mangle]
pub extern "C" fn paimon_table_get_schema(
    catalog: *mut PaimonCatalog,
    database: *const c_char,
    table: *const c_char,
    error_out: *mut *mut PaimonError,
) -> *mut PaimonTable {
    if catalog.is_null() || database.is_null() || table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let catalog_ref = unsafe { &*catalog };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    unsafe {
                        *error_out = create_error("Invalid database name encoding");
                    }
                }
                return ptr::null_mut();
            }
        }
    };
    let table_str = unsafe {
        match CStr::from_ptr(table).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    unsafe {
                        *error_out = create_error("Invalid table name encoding");
                    }
                }
                return ptr::null_mut();
            }
        }
    };

    let identifier = Identifier::new(db_str, table_str);

    // Get table schema without scanning data
    // We get the schema by getting current snapshot and using schema_by_snapshot
    let result: PaimonResult<Arc<ArrowSchema>> = catalog_ref.rt.block_on(async {
        let table = catalog_ref.catalog.get_table(&identifier).await?;
        // Get current snapshot and use schema_by_snapshot to get schema
        let snapshot = table.current_snapshot().await?;
        let snapshot_ref = snapshot.ok_or_else(|| paimon::Error::DataInvalid {
            message: "Table has no snapshot".to_string(),
            source: None,
        })?;
        let snapshot_ref_arc = Arc::new(snapshot_ref);
        let table_schema = table.schema_by_snapshot(snapshot_ref_arc).await
            .ok_or_else(|| paimon::Error::DataInvalid {
                message: "Failed to get table schema from snapshot".to_string(),
                source: None,
            })?;
        // Convert TableSchema fields to Arrow Schema fields
        let arrow_fields: Vec<Field> = table_schema.fields().iter().map(|field| {
            // Convert Paimon DataType to Arrow DataType
            let arrow_data_type = paimon_to_arrow_type(field.data_type());
            Field::new(field.name(), arrow_data_type, true)
        }).collect();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        Ok(Arc::new(arrow_schema))
    });

    match result {
        Ok(schema) => {
            Box::into_raw(Box::new(PaimonTable {
                schema,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get column count from table schema
#[no_mangle]
pub extern "C" fn paimon_table_get_column_count(table: *mut PaimonTable) -> c_int {
    if table.is_null() {
        return 0;
    }
    unsafe {
        let table_ref = &*table;
        table_ref.schema.fields().len() as c_int
    }
}

/// Get column name from table schema
#[no_mangle]
pub extern "C" fn paimon_table_get_column_name(
    table: *mut PaimonTable,
    index: c_int,
) -> *const c_char {
    if table.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let table_ref = &*table;
        let fields = table_ref.schema.fields();
        if index < 0 || index as usize >= fields.len() {
            return ptr::null_mut();
        }
        let field = &fields[index as usize];
        let name = CString::new(field.name().as_str()).unwrap();
        name.into_raw()
    }
}

/// Get column data type as string from table schema
#[no_mangle]
pub extern "C" fn paimon_table_get_column_type(
    table: *mut PaimonTable,
    index: c_int,
) -> *const c_char {
    if table.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let table_ref = &*table;
        let fields = table_ref.schema.fields();
        if index < 0 || index as usize >= fields.len() {
            return ptr::null_mut();
        }
        let field = &fields[index as usize];
        let type_str = format!("{:?}", field.data_type());
        let c_str = CString::new(type_str).unwrap();
        c_str.into_raw()
    }
}

/// Free a Paimon table handle
#[no_mangle]
pub extern "C" fn paimon_table_free(table: *mut PaimonTable) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

/// Scan a Paimon table and return a handle
#[no_mangle]
pub extern "C" fn paimon_table_scan(
    catalog: *mut PaimonCatalog,
    database: *const c_char,
    table: *const c_char,
    error_out: *mut *mut PaimonError,
) -> *mut PaimonScan {
    if catalog.is_null() || database.is_null() || table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let catalog_ref = unsafe { &*catalog };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    unsafe {
                        *error_out = create_error("Invalid database name encoding");
                    }
                }
                return ptr::null_mut();
            }
        }
    };
    let table_str = unsafe {
        match CStr::from_ptr(table).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    unsafe {
                        *error_out = create_error("Invalid table name encoding");
                    }
                }
                return ptr::null_mut();
            }
        }
    };

    let identifier = Identifier::new(db_str, table_str);

    // Scan the table
    let result: PaimonResult<(Vec<RecordBatch>, Arc<ArrowSchema>)> = catalog_ref.rt.block_on(async {
        let table = catalog_ref.catalog.get_table(&identifier).await?;
        let scan = table.scan().build().await?;
        let arrow_stream = scan.to_arrow().await?;
        
        // Collect all batches
        let batches: Vec<RecordBatch> = arrow_stream.try_collect().await?;
        let schema = if let Some(first_batch) = batches.first() {
            first_batch.schema()
        } else {
            // Return empty schema if no batches
            Arc::new(ArrowSchema::empty())
        };
        
        Ok((batches, schema))
    });

    match result {
        Ok((batches, schema)) => {
            Box::into_raw(Box::new(PaimonScan {
                batches,
                current_index: 0,
                schema,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get the schema of a scan
#[no_mangle]
pub extern "C" fn paimon_scan_get_schema(scan: *mut PaimonScan) -> *const ArrowSchema {
    if scan.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let scan_ref = &*scan;
        Arc::as_ptr(&scan_ref.schema) as *const ArrowSchema
    }
}

/// Get the number of columns in the schema
#[no_mangle]
pub extern "C" fn paimon_scan_get_column_count(scan: *mut PaimonScan) -> c_int {
    if scan.is_null() {
        return 0;
    }
    unsafe {
        let scan_ref = &*scan;
        scan_ref.schema.fields().len() as c_int
    }
}

/// Get column name
#[no_mangle]
pub extern "C" fn paimon_scan_get_column_name(
    scan: *mut PaimonScan,
    index: c_int,
) -> *const c_char {
    if scan.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let scan_ref = &*scan;
        let fields = scan_ref.schema.fields();
        if index < 0 || index as usize >= fields.len() {
            return ptr::null_mut();
        }
        let field = &fields[index as usize];
        let name = CString::new(field.name().as_str()).unwrap();
        name.into_raw()
    }
}

/// Get column data type as string
#[no_mangle]
pub extern "C" fn paimon_scan_get_column_type(
    scan: *mut PaimonScan,
    index: c_int,
) -> *const c_char {
    if scan.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let scan_ref = &*scan;
        let fields = scan_ref.schema.fields();
        if index < 0 || index as usize >= fields.len() {
            return ptr::null_mut();
        }
        let field = &fields[index as usize];
        let type_str = format!("{:?}", field.data_type());
        let c_str = CString::new(type_str).unwrap();
        c_str.into_raw()
    }
}

/// Get the number of batches
#[no_mangle]
pub extern "C" fn paimon_scan_get_batch_count(scan: *mut PaimonScan) -> c_int {
    if scan.is_null() {
        return 0;
    }
    unsafe {
        let scan_ref = &*scan;
        scan_ref.batches.len() as c_int
    }
}

/// Export Arrow schema to Arrow C Data Interface
#[no_mangle]
pub extern "C" fn paimon_scan_export_schema(
    scan: *mut PaimonScan,
    out_schema: *mut FFI_ArrowSchema,
) -> c_int {
    if scan.is_null() || out_schema.is_null() {
        return 1; // Error
    }
    unsafe {
        let scan_ref = &*scan;
        // Export schema to FFI
        match FFI_ArrowSchema::try_from(scan_ref.schema.as_ref()) {
            Ok(ffi_schema) => {
                *out_schema = ffi_schema;
                0 // Success
            }
            Err(_) => 1, // Error
        }
    }
}

/// Export a batch by index to Arrow C Data Interface
#[no_mangle]
pub extern "C" fn paimon_scan_export_batch(
    scan: *mut PaimonScan,
    index: c_int,
    out_array: *mut FFI_ArrowArray,
) -> c_int {
    if scan.is_null() || out_array.is_null() {
        return 1; // Error
    }
    unsafe {
        let scan_ref = &*scan;
        if index < 0 || index as usize >= scan_ref.batches.len() {
            return 1; // Error
        }
        let batch = &scan_ref.batches[index as usize];
        
        // Convert RecordBatch to StructArray, then to ArrayData for FFI export
        // RecordBatch is essentially a StructArray where each field is a column
        let struct_array = StructArray::from(batch.clone());
        let array_data = struct_array.to_data();
        
        // Export ArrayData to FFI
        match to_ffi(&array_data) {
            Ok((ffi_array, _ffi_schema)) => {
                *out_array = ffi_array;
                0 // Success
            }
            Err(_) => 1, // Error
        }
    }
}

/// Get a batch by index (returns a pointer to RecordBatch) - deprecated, use paimon_scan_export_batch
#[no_mangle]
pub extern "C" fn paimon_scan_get_batch(
    scan: *mut PaimonScan,
    index: c_int,
) -> *const RecordBatch {
    if scan.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let scan_ref = &*scan;
        if index < 0 || index as usize >= scan_ref.batches.len() {
            return ptr::null_mut();
        }
        &scan_ref.batches[index as usize] as *const RecordBatch
    }
}

/// Free a scan handle
#[no_mangle]
pub extern "C" fn paimon_scan_free(scan: *mut PaimonScan) {
    if !scan.is_null() {
        unsafe {
            let _ = Box::from_raw(scan);
        }
    }
}

/// Free an error
#[no_mangle]
pub extern "C" fn paimon_error_free(error: *mut PaimonError) {
    if !error.is_null() {
        unsafe {
            let error_ref = Box::from_raw(error);
            if !error_ref.message.is_null() {
                let _ = CString::from_raw(error_ref.message);
            }
        }
    }
}

/// Get error message
#[no_mangle]
pub extern "C" fn paimon_error_get_message(error: *const PaimonError) -> *const c_char {
    if error.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        (*error).message
    }
}

fn create_error(msg: &str) -> *mut PaimonError {
    let c_msg = match CString::new(msg) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    };
    Box::into_raw(Box::new(PaimonError { message: c_msg }))
}

