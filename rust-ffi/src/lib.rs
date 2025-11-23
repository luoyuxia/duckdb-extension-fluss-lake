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
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_array::ffi::to_ffi;
use arrow::array::{ArrayData, StructArray};
use futures_util::TryStreamExt;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::io::FileIOBuilder;
use paimon::Result as PaimonResult;

/// Opaque handle for a Paimon catalog
#[repr(C)]
pub struct PaimonCatalog {
    catalog: Arc<dyn Catalog>,
    rt: tokio::runtime::Runtime,
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
            Box::from_raw(catalog);
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
            Box::from_raw(scan);
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

