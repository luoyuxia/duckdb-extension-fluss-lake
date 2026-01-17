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

// Use arrow 57 types (now matching both paimon and fluss)
use arrow::array::StructArray;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi::to_ffi;
use arrow_array::Array;
use arrow_schema::Schema as ArrowSchema;
use futures_util::TryStreamExt;
use paimon::arrow::schema_to_arrow_schema;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier, Table};
use paimon::io::FileIOBuilder;
use paimon::scan::{ArrowRecordBatchStream, TableScan};
use paimon::Result as PaimonResult;

/// Opaque handle for a Paimon catalog
#[repr(C)]
pub struct PaimonCatalog {
    catalog: Arc<dyn Catalog>,
    rt: tokio::runtime::Runtime,
}

/// Opaque handle for a Paimon table (schema only, no scan)
#[repr(C)]
pub struct PaimonTable {
    table: Arc<Table>,
    schema: Arc<ArrowSchema>,
    rt_handle: tokio::runtime::Handle,
}

/// Opaque handle for a Paimon table scan
/// Contains TableScan which is created in paimon_table_scan
/// Arrow stream is created lazily when paimon_scan_export_batch is first called
/// Note: No mutex needed since DuckDB guarantees single-threaded access to GlobalTableFunctionState
#[repr(C)]
pub struct PaimonScan {
    table_scan: TableScan,
    arrow_stream: Option<ArrowRecordBatchStream>,
    schema: Arc<ArrowSchema>,
    rt_handle: tokio::runtime::Handle
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

/// Get Paimon table handle (with schema, without scanning)
#[no_mangle]
pub extern "C" fn get_paimon_table(
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
                    *error_out = create_error("Invalid database name encoding");
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
                    *error_out = create_error("Invalid table name encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let identifier = Identifier::new(db_str, table_str);

    // schema_to_arrow_schema returns Schema (not Arc<Schema>)
    let result: PaimonResult<(Arc<Table>, Arc<ArrowSchema>)> = catalog_ref.rt.block_on(async {
        let table = catalog_ref.catalog.get_table(&identifier).await?;
        let arrow_schema = schema_to_arrow_schema(&table.table_schema())?;
        let table_arc: Arc<Table> = Arc::new(table);
        // Wrap Schema in Arc
        Ok((table_arc, Arc::new(arrow_schema)))
    });

    match result {
        Ok((table, schema)) => {
            // schema is already Arc<ArrowSchema> from paimon's arrow version
            Box::into_raw(Box::new(PaimonTable {
                table,
                schema,
                rt_handle: catalog_ref.rt.handle().clone(),
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

/// Export table schema to Arrow C Data Interface
#[no_mangle]
pub extern "C" fn paimon_table_export_schema(
    table: *mut PaimonTable,
    out_schema: *mut FFI_ArrowSchema,
) -> c_int {
    if table.is_null() || out_schema.is_null() {
        return 1; // Error
    }
    unsafe {
        let table_ref = &*table;
        // Export schema to FFI
        match FFI_ArrowSchema::try_from(table_ref.schema.as_ref()) {
            Ok(ffi_schema) => {
                *out_schema = ffi_schema;
                0 // Success
            }
            Err(_) => 1, // Error
        }
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
/// Uses the Table object stored in PaimonTable
#[no_mangle]
pub extern "C" fn paimon_table_scan(
    paimon_table: *mut PaimonTable,
    error_out: *mut *mut PaimonError,
) -> *mut PaimonScan {
    if paimon_table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let table_ref = unsafe { &*paimon_table };
    let table = table_ref.table.clone();
    let schema = table_ref.schema.clone();
    let rt_handle = table_ref.rt_handle.clone();

    // Create TableScan here, but don't create arrow stream yet
    // The arrow stream will be created lazily when paimon_scan_export_batch is first called
    let table_scan_result: PaimonResult<TableScan> = rt_handle.block_on(async {
        table.scan().build().await
    });

    let table_scan = match table_scan_result {
        Ok(scan) => scan,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_error(&format!("Failed to create table scan: {}", e));
                }
            }
            return ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(PaimonScan {
        table_scan,
        arrow_stream: None,
        schema,
        rt_handle,
    }))
}

/// Export the next batch to Arrow C Data Interface
/// Gets the next batch from the stream in streaming mode
#[no_mangle]
pub extern "C" fn paimon_scan_export_batch(
    scan: *mut PaimonScan,
    out_array: *mut FFI_ArrowArray,
) -> c_int {
    if scan.is_null() || out_array.is_null() {
        return 1; // Error
    }
    unsafe {
        // Get mutable reference to scan (safe because DuckDB guarantees single-threaded access)
        let scan_ref = &mut *scan;
        
        // Lazy initialization: create stream on first call
        if scan_ref.arrow_stream.is_none() {
            let table_scan = &scan_ref.table_scan;
            let rt_handle = scan_ref.rt_handle.clone();
            
            // Create Arrow stream from TableScan
            let arrow_stream_result: PaimonResult<ArrowRecordBatchStream> = rt_handle.block_on(async {
                table_scan.to_arrow().await
            });
            
            match arrow_stream_result {
                Ok(stream) => {
                    scan_ref.arrow_stream = Some(stream);
                }
                Err(_) => {
                    // Error creating stream
                    return 1;
                }
            }
        }
        
        // Get the stream (we need mutable access to take it out)
        let mut stream = match scan_ref.arrow_stream.take() {
            Some(s) => s,
            None => return 1, // Should not happen - we just created it
        };
        
        // Get next batch from stream (blocking)
        let batch_result = scan_ref.rt_handle.block_on(async {
            stream.try_next().await
        });
        
        // Put the stream back
        scan_ref.arrow_stream = Some(stream);
        
        let batch = match batch_result {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                // Stream ended
                return 1;
            }
            Err(_) => {
                // Error reading batch
                return 1;
            }
        };
        
        // Convert RecordBatch to StructArray, then to ArrayData for FFI export
        // In arrow 57, StructArray implements From<RecordBatch>
        let struct_array = StructArray::from(batch);
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
