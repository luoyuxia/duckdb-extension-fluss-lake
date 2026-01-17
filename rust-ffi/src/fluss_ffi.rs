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
use std::collections::HashMap;
use std::time::Duration;

// Fluss uses arrow 57, so we use arrow types from fluss's dependencies
// Import through fluss's arrow version
use arrow::array::StructArray;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi::to_ffi;
use arrow_array::Array;
use arrow_array::RecordBatch;

// Fluss imports
use fluss::client::{FlussConnection, FlussAdmin, FlussTable, RecordBatchLogScanner};
use fluss::config::Config;
use fluss::metadata::{TablePath, LakeSnapshot};
use fluss::rpc::message::OffsetSpec;
use fluss::record::{ScanRecordBatches};
use fluss::error::Result as FlussResult;

/// Opaque handle for a Fluss connection
#[repr(C)]
pub struct FlussConnectionHandle {
    connection: FlussConnection,
    rt: tokio::runtime::Runtime,
}

/// Opaque handle for a Fluss admin
#[repr(C)]
pub struct FlussAdminHandle {
    admin: FlussAdmin,
    rt_handle: tokio::runtime::Handle,
}

/// Opaque handle for a Fluss table
/// Note: The connection must remain alive while this handle is in use
#[repr(C)]
pub struct FlussTableHandle {
    table: FlussTable<'static>,  // Note: Using 'static lifetime, actual lifetime managed by connection
    rt_handle: tokio::runtime::Handle,
}

/// Opaque handle for a Fluss RecordBatchLogScanner
#[repr(C)]
pub struct FlussScannerHandle {
    scanner: RecordBatchLogScanner,
    rt_handle: tokio::runtime::Handle,
    batches: Vec<RecordBatch>,  // Buffer for polled batches
    current_batch_index: usize,
    total_rows_read: i64,  // Total rows read so far (for offset tracking)
}

/// Handle for ScanRecordBatches returned from poll_batches
/// Contains batches organized by bucket
#[repr(C)]
pub struct FlussScanRecordBatchesHandle {
    batches_by_bucket: HashMap<i32, Vec<RecordBatch>>,  // bucket_id -> batches
    table_id: i64,
}

/// Error information for Fluss operations
#[repr(C)]
pub struct FlussError {
    message: *mut c_char,
}

// Helper structures
#[repr(C)]
pub struct LakeSnapshotHandle {
    snapshot: LakeSnapshot,
}

#[repr(C)]
pub struct FlussOffsetMap {
    offsets: HashMap<i32, i64>,
}

/// Handle for TableInfo to access properties
#[repr(C)]
pub struct FlussTableInfoHandle {
    table_info: fluss::metadata::TableInfo,
}

/// Property map for FFI (key-value pairs)
#[repr(C)]
pub struct FlussPropertyMap {
    keys: Vec<CString>,
    values: Vec<CString>,
    count: usize,
}

/// Create a new Fluss connection
/// bootstrap_server: e.g., "127.0.0.1:9123"
#[no_mangle]
pub extern "C" fn fluss_connection_new(bootstrap_server: *const c_char) -> *mut FlussConnectionHandle {
    if bootstrap_server.is_null() {
        return ptr::null_mut();
    }

    let server_str = unsafe {
        match CStr::from_ptr(bootstrap_server).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };

    let mut config = Config::default();
    config.bootstrap_server = Some(server_str.to_string());

    let connection = match rt.block_on(FlussConnection::new(config)) {
        Ok(conn) => conn,
        Err(_) => return ptr::null_mut(),
    };

    Box::into_raw(Box::new(FlussConnectionHandle {
        connection,
        rt,
    }))
}

/// Free a Fluss connection
#[no_mangle]
pub extern "C" fn fluss_connection_free(conn: *mut FlussConnectionHandle) {
    if !conn.is_null() {
        unsafe {
            let _ = Box::from_raw(conn);
        }
    }
}

/// Get Fluss admin from connection
#[no_mangle]
pub extern "C" fn fluss_connection_get_admin(
    conn: *mut FlussConnectionHandle,
    error_out: *mut *mut FlussError,
) -> *mut FlussAdminHandle {
    if conn.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null connection pointer");
            }
        }
        return ptr::null_mut();
    }

    let conn_ref = unsafe { &*conn };
    let admin_result: FlussResult<FlussAdmin> = conn_ref.rt.block_on(async {
        conn_ref.connection.get_admin().await
    });

    match admin_result {
        Ok(admin) => {
            Box::into_raw(Box::new(FlussAdminHandle {
            admin,
            rt_handle: conn_ref.rt.handle().clone(),
        }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get Fluss table from connection
#[no_mangle]
pub extern "C" fn fluss_connection_get_table(
    conn: *mut FlussConnectionHandle,
    database: *const c_char,
    table: *const c_char,
    error_out: *mut *mut FlussError,
) -> *mut FlussTableHandle {
    if conn.is_null() || database.is_null() || table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let conn_ref = unsafe { &*conn };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    *error_out = create_fluss_error("Invalid database name encoding");
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
                    *error_out = create_fluss_error("Invalid table name encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let table_path = TablePath::new(db_str.to_string(), table_str.to_string());
    let table_result: FlussResult<FlussTable> = conn_ref.rt.block_on(async {
        conn_ref.connection.get_table(&table_path).await
    });

    match table_result {
        Ok(table) => {
            // Note: We extend the lifetime to 'static, which is safe because
            // the caller is responsible for keeping the connection alive.
            // The connection handle must not be freed while this table handle is in use.
            let table_static: FlussTable<'static> = unsafe { std::mem::transmute(table) };
            Box::into_raw(Box::new(FlussTableHandle {
                table: table_static,
                rt_handle: conn_ref.rt.handle().clone(),
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get table info from Fluss admin
/// Returns TableInfoHandle for accessing properties and schema
#[no_mangle]
pub extern "C" fn fluss_admin_get_table_info(
    admin: *mut FlussAdminHandle,
    database: *const c_char,
    table: *const c_char,
    error_out: *mut *mut FlussError,
) -> *mut FlussTableInfoHandle {
    if admin.is_null() || database.is_null() || table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let admin_ref = unsafe { &*admin };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    *error_out = create_fluss_error("Invalid database name encoding");
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
                    *error_out = create_fluss_error("Invalid table name encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let table_path = TablePath::new(db_str.to_string(), table_str.to_string());
    let table_info_result: FlussResult<fluss::metadata::TableInfo> = admin_ref.rt_handle.block_on(async {
        admin_ref.admin.get_table(&table_path).await
    });

    match table_info_result {
        Ok(table_info) => {
            Box::into_raw(Box::new(FlussTableInfoHandle {
                table_info,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get Arrow schema from TableInfoHandle
#[no_mangle]
pub extern "C" fn fluss_table_info_export_schema(
    table_info: *mut FlussTableInfoHandle,
    out_schema: *mut FFI_ArrowSchema,
    error_out: *mut *mut FlussError,
) -> c_int {
    if table_info.is_null() || out_schema.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return 1; // Error
    }

    let table_info_ref = unsafe { &*table_info };
    let data_type = table_info_ref.table_info.get_row_type();
    let arrow_schema = fluss::record::to_arrow_schema(data_type);

    // Export to FFI
    match FFI_ArrowSchema::try_from(arrow_schema.as_ref()) {
        Ok(ffi_schema) => {
            unsafe {
                *out_schema = ffi_schema;
            }
            0 // Success
        }
        Err(_) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error("Failed to export Arrow schema");
                }
            }
            1
        }
    }
}

/// Get table properties from TableInfoHandle
/// Returns a map of properties filtered by prefix (e.g., "table.datalake.paimon.")
#[no_mangle]
pub extern "C" fn fluss_table_info_get_properties_with_prefix(
    table_info: *mut FlussTableInfoHandle,
    prefix: *const c_char,
    error_out: *mut *mut FlussError,
) -> *mut FlussPropertyMap {
    if table_info.is_null() || prefix.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let table_info_ref = unsafe { &*table_info };
    let prefix_str = unsafe {
        match CStr::from_ptr(prefix).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    *error_out = create_fluss_error("Invalid prefix encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let properties = table_info_ref.table_info.get_properties();
    let mut filtered_keys = Vec::new();
    let mut filtered_values = Vec::new();

    for (key, value) in properties.iter() {
        if key.starts_with(prefix_str) {
            // Remove prefix from key
            let key_without_prefix = &key[prefix_str.len()..];
            filtered_keys.push(match CString::new(key_without_prefix) {
                Ok(s) => s,
                Err(_) => continue,
            });
            filtered_values.push(match CString::new(value.as_str()) {
                Ok(s) => s,
                Err(_) => continue,
            });
        }
    }

    let count = filtered_keys.len();
    Box::into_raw(Box::new(FlussPropertyMap {
        keys: filtered_keys,
        values: filtered_values,
        count,
    }))
}

/// Get property count from PropertyMap
#[no_mangle]
pub extern "C" fn fluss_property_map_get_count(map: *const FlussPropertyMap) -> usize {
    if map.is_null() {
        return 0;
    }
    unsafe {
        (*map).count
    }
}

/// Get property key at index
#[no_mangle]
pub extern "C" fn fluss_property_map_get_key(map: *const FlussPropertyMap, index: usize) -> *const c_char {
    if map.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let map_ref = &*map;
        if index >= map_ref.count {
            return ptr::null_mut();
        }
        map_ref.keys[index].as_ptr()
    }
}

/// Get property value at index
#[no_mangle]
pub extern "C" fn fluss_property_map_get_value(map: *const FlussPropertyMap, index: usize) -> *const c_char {
    if map.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let map_ref = &*map;
        if index >= map_ref.count {
            return ptr::null_mut();
        }
        map_ref.values[index].as_ptr()
    }
}

/// Free PropertyMap
#[no_mangle]
pub extern "C" fn fluss_property_map_free(map: *mut FlussPropertyMap) {
    if !map.is_null() {
        unsafe {
            let _ = Box::from_raw(map);
        }
    }
}

/// Get table ID from TableInfoHandle
#[no_mangle]
pub extern "C" fn fluss_table_info_get_table_id(table_info: *const FlussTableInfoHandle) -> i64 {
    if table_info.is_null() {
        return -1;
    }
    unsafe {
        (*table_info).table_info.get_table_id()
    }
}

/// Get number of buckets from TableInfoHandle
#[no_mangle]
pub extern "C" fn fluss_table_info_get_num_buckets(table_info: *const FlussTableInfoHandle) -> i32 {
    if table_info.is_null() {
        return -1;
    }
    unsafe {
        (*table_info).table_info.get_num_buckets()
    }
}

/// Free TableInfoHandle
#[no_mangle]
pub extern "C" fn fluss_table_info_free(table_info: *mut FlussTableInfoHandle) {
    if !table_info.is_null() {
        unsafe {
            let _ = Box::from_raw(table_info);
        }
    }
}

/// Get latest lake snapshot from Fluss admin
#[no_mangle]
pub extern "C" fn fluss_admin_get_latest_lake_snapshot(
    admin: *mut FlussAdminHandle,
    database: *const c_char,
    table: *const c_char,
    error_out: *mut *mut FlussError,
) -> *mut LakeSnapshotHandle {
    if admin.is_null() || database.is_null() || table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let admin_ref = unsafe { &*admin };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    *error_out = create_fluss_error("Invalid database name encoding");
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
                    *error_out = create_fluss_error("Invalid table name encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let table_path = TablePath::new(db_str.to_string(), table_str.to_string());
    let snapshot_result: FlussResult<LakeSnapshot> = admin_ref.rt_handle.block_on(async {
        admin_ref.admin.get_latest_lake_snapshot(&table_path).await
    });

    match snapshot_result {
        Ok(snapshot) => {
            Box::into_raw(Box::new(LakeSnapshotHandle {
                snapshot,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get snapshot ID from LakeSnapshotHandle
#[no_mangle]
pub extern "C" fn fluss_lake_snapshot_get_snapshot_id(snapshot: *const LakeSnapshotHandle) -> i64 {
    if snapshot.is_null() {
        return -1;
    }
    unsafe {
        (*snapshot).snapshot.snapshot_id()
    }
}

/// Get offset for a specific bucket from LakeSnapshotHandle
/// Returns -1 if bucket not found
#[no_mangle]
pub extern "C" fn fluss_lake_snapshot_get_bucket_offset(
    snapshot: *const LakeSnapshotHandle,
    table_id: i64,
    bucket_id: i32,
) -> i64 {
    if snapshot.is_null() {
        return -1;
    }
    unsafe {
        let snapshot_ref = &*snapshot;
        let table_bucket = fluss::metadata::TableBucket::new(table_id, bucket_id);
        snapshot_ref.snapshot.table_buckets_offset()
            .get(&table_bucket)
            .copied()
            .unwrap_or(-1)
    }
}

/// Free LakeSnapshotHandle
#[no_mangle]
pub extern "C" fn fluss_lake_snapshot_free(snapshot: *mut LakeSnapshotHandle) {
    if !snapshot.is_null() {
        unsafe {
            let _ = Box::from_raw(snapshot);
        }
    }
}

/// List offsets from Fluss admin
#[no_mangle]
pub extern "C" fn fluss_admin_list_offsets(
    admin: *mut FlussAdminHandle,
    database: *const c_char,
    table: *const c_char,
    bucket_ids: *const c_int,
    bucket_count: c_int,
    offset_spec_type: c_int,  // 0=Earliest, 1=Latest, 2=Timestamp (use offset_spec_timestamp)
    offset_spec_timestamp: i64,  // Only used if offset_spec_type == 2
    error_out: *mut *mut FlussError,
) -> *mut FlussOffsetMap {
    if admin.is_null() || database.is_null() || table.is_null() || bucket_ids.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return ptr::null_mut();
    }

    let admin_ref = unsafe { &*admin };
    let db_str = unsafe {
        match CStr::from_ptr(database).to_str() {
            Ok(s) => s,
            Err(_) => {
                if !error_out.is_null() {
                    *error_out = create_fluss_error("Invalid database name encoding");
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
                    *error_out = create_fluss_error("Invalid table name encoding");
                }
                return ptr::null_mut();
            }
        }
    };

    let table_path = TablePath::new(db_str.to_string(), table_str.to_string());
    
    // Convert bucket_ids array to Vec
    let buckets: Vec<i32> = unsafe {
        std::slice::from_raw_parts(bucket_ids, bucket_count as usize).to_vec()
    };

    // Create OffsetSpec
    let offset_spec = match offset_spec_type {
        0 => OffsetSpec::Earliest,
        1 => OffsetSpec::Latest,
        2 => OffsetSpec::Timestamp(offset_spec_timestamp),
        _ => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error("Invalid offset_spec_type");
                }
            }
            return ptr::null_mut();
        }
    };

    let offsets_result: FlussResult<HashMap<i32, i64>> = admin_ref.rt_handle.block_on(async {
        admin_ref.admin.list_offsets(&table_path, &buckets, offset_spec).await
    });

    match offsets_result {
        Ok(offsets) => {
            Box::into_raw(Box::new(FlussOffsetMap {
                offsets,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("{}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get offset for a bucket from FlussOffsetMap
/// Returns -1 if bucket not found
#[no_mangle]
pub extern "C" fn fluss_offset_map_get_offset(map: *const FlussOffsetMap, bucket_id: i32) -> i64 {
    if map.is_null() {
        return -1;
    }
    unsafe {
        (*map).offsets.get(&bucket_id).copied().unwrap_or(-1)
    }
}

/// Get bucket count from FlussOffsetMap
#[no_mangle]
pub extern "C" fn fluss_offset_map_get_count(map: *const FlussOffsetMap) -> usize {
    if map.is_null() {
        return 0;
    }
    unsafe {
        (*map).offsets.len()
    }
}

/// Get all bucket IDs from FlussOffsetMap
/// Caller must provide a buffer large enough to hold all bucket IDs
#[no_mangle]
pub extern "C" fn fluss_offset_map_get_bucket_ids(
    map: *const FlussOffsetMap,
    out_bucket_ids: *mut i32,
    max_count: usize,
) -> usize {
    if map.is_null() || out_bucket_ids.is_null() {
        return 0;
    }
    unsafe {
        let map_ref = &*map;
        let count = map_ref.offsets.len().min(max_count);
        let mut bucket_ids: Vec<i32> = map_ref.offsets.keys().copied().collect();
        bucket_ids.sort();
        for (i, &bucket_id) in bucket_ids.iter().take(count).enumerate() {
            *out_bucket_ids.add(i) = bucket_id;
        }
        count
    }
}

/// Free FlussOffsetMap
#[no_mangle]
pub extern "C" fn fluss_offset_map_free(map: *mut FlussOffsetMap) {
    if !map.is_null() {
        unsafe {
            let _ = Box::from_raw(map);
        }
    }
}

/// Create RecordBatchLogScanner from Fluss table
#[no_mangle]
pub extern "C" fn fluss_table_create_record_batch_log_scanner(
    table: *mut FlussTableHandle,
    error_out: *mut *mut FlussError,
) -> *mut FlussScannerHandle {
    if table.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null table pointer");
            }
        }
        return ptr::null_mut();
    }

    let table_ref = unsafe { &*table };
    // Create scanner using the table's new_scan method
    let scan = table_ref.table.new_scan();
    let scanner_result = scan.create_record_batch_log_scanner();

    match scanner_result {
        Ok(scanner) => {
            Box::into_raw(Box::new(FlussScannerHandle {
                scanner,
                rt_handle: table_ref.rt_handle.clone(),
                batches: Vec::new(),
                current_batch_index: 0,
                total_rows_read: 0,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("Failed to create scanner: {}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Subscribe to multiple buckets with their starting offsets
#[no_mangle]
pub extern "C" fn fluss_scanner_subscribe_batch(
    scanner: *mut FlussScannerHandle,
    bucket_ids: *const i32,
    offsets: *const i64,
    count: i32,
    error_out: *mut *mut FlussError,
) -> c_int {
    if scanner.is_null() || bucket_ids.is_null() || offsets.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null pointer provided");
            }
        }
        return 1; // Error
    }

    let scanner_ref = unsafe { &*scanner };
    let bucket_ids_slice = unsafe { std::slice::from_raw_parts(bucket_ids, count as usize) };
    let offsets_slice = unsafe { std::slice::from_raw_parts(offsets, count as usize) };

    let mut bucket_offsets = HashMap::new();
    for i in 0..(count as usize) {
        bucket_offsets.insert(bucket_ids_slice[i], offsets_slice[i]);
    }

    let subscribe_result: FlussResult<()> = scanner_ref.rt_handle.block_on(async {
        scanner_ref.scanner.subscribe_batch(&bucket_offsets).await
    });

    match subscribe_result {
        Ok(_) => 0, // Success
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("Failed to subscribe: {}", e));
                }
            }
            1 // Error
        }
    }
}

/// Unsubscribe from a single bucket
#[no_mangle]
pub extern "C" fn fluss_scanner_unsubscribe_batch(
    scanner: *mut FlussScannerHandle,
    bucket_id: i32,
    _table_id: i64,
    error_out: *mut *mut FlussError,
) -> c_int {
    if scanner.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null scanner pointer provided");
            }
        }
        return 1; // Error
    }

    let scanner_ref = unsafe { &*scanner };
    
    let unsubscribe_result: FlussResult<()> = scanner_ref.rt_handle.block_on(async {
        scanner_ref.scanner.unsubscribe(bucket_id).await
    });

    match unsubscribe_result {
        Ok(_) => 0, // Success
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("Failed to unsubscribe bucket {}: {}", bucket_id, e));
                }
            }
            1 // Error
        }
    }
}

/// Poll batches from RecordBatchLogScanner
/// Returns a handle to ScanRecordBatches containing batches organized by bucket
/// Returns null on error/no more batches
#[no_mangle]
pub extern "C" fn fluss_scanner_poll_batches(
    scanner: *mut FlussScannerHandle,
    timeout_ms: i64,
    error_out: *mut *mut FlussError,
) -> *mut FlussScanRecordBatchesHandle {
    if scanner.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = create_fluss_error("Null scanner pointer");
            }
        }
        return ptr::null_mut();
    }

    let scanner_ref = unsafe { &*scanner };
    let timeout = Duration::from_millis(timeout_ms as u64);

    let batches_result: FlussResult<ScanRecordBatches> = scanner_ref.rt_handle.block_on(async {
        scanner_ref.scanner.poll(timeout).await
    });

    match batches_result {
        Ok(scan_record_batches) => {
            if scan_record_batches.is_empty() {
                return ptr::null_mut(); // No more batches
            }

            // Convert ScanRecordBatches to our handle format
            // Extract bucket_id from TableBucket and organize by bucket_id
            let mut batches_by_bucket: HashMap<i32, Vec<RecordBatch>> = HashMap::new();
            let mut table_id = 0i64;

            for (table_bucket, batches) in scan_record_batches.record_batches.iter() {
                table_id = table_bucket.table_id();
                let bucket_id = table_bucket.bucket_id();
                batches_by_bucket.insert(bucket_id, batches.clone());
            }

            Box::into_raw(Box::new(FlussScanRecordBatchesHandle {
                batches_by_bucket,
                table_id,
            }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = create_fluss_error(&format!("Failed to poll batches: {}", e));
                }
            }
            ptr::null_mut()
        }
    }
}

/// Get the number of buckets in ScanRecordBatches
#[no_mangle]
pub extern "C" fn fluss_scan_record_batches_get_bucket_count(
    batches: *const FlussScanRecordBatchesHandle,
) -> usize {
    if batches.is_null() {
        return 0;
    }
    unsafe {
        (*batches).batches_by_bucket.len()
    }
}

/// Get bucket IDs from ScanRecordBatches
/// Returns the number of bucket IDs written
#[no_mangle]
pub extern "C" fn fluss_scan_record_batches_get_bucket_ids(
    batches: *const FlussScanRecordBatchesHandle,
    out_bucket_ids: *mut i32,
    max_count: usize,
) -> usize {
    if batches.is_null() || out_bucket_ids.is_null() {
        return 0;
    }
    unsafe {
        let batches_ref = &*batches;
        let count = batches_ref.batches_by_bucket.len().min(max_count);
        let mut bucket_ids: Vec<i32> = batches_ref.batches_by_bucket.keys().copied().collect();
        bucket_ids.sort();
        for (i, &bucket_id) in bucket_ids.iter().take(count).enumerate() {
            *out_bucket_ids.add(i) = bucket_id;
        }
        count
    }
}

/// Get the first batch for a specific bucket
/// Returns 0 on success, 1 on error/bucket not found
#[no_mangle]
pub extern "C" fn fluss_scan_record_batches_get_batch_for_bucket(
    batches: *mut FlussScanRecordBatchesHandle,
    bucket_id: i32,
    out_array: *mut FFI_ArrowArray,
) -> c_int {
    if batches.is_null() || out_array.is_null() {
        return 1; // Error
    }

    let batches_ref = unsafe { &mut *batches };
    
    if let Some(bucket_batches) = batches_ref.batches_by_bucket.get_mut(&bucket_id) {
        if bucket_batches.is_empty() {
            return 1; // No batches for this bucket
        }

        // Get the first batch and remove it from the vector
        let batch = bucket_batches.remove(0);
        
        // Convert to FFI
        let struct_array = StructArray::from(batch);
        let array_data = struct_array.to_data();

        match to_ffi(&array_data) {
            Ok((ffi_array, _ffi_schema)) => {
                unsafe {
                    *out_array = ffi_array;
                }
                0 // Success
            }
            Err(_) => 1, // Error
        }
    } else {
        1 // Bucket not found
    }
}

/// Free FlussScanRecordBatchesHandle
#[no_mangle]
pub extern "C" fn fluss_scan_record_batches_free(batches: *mut FlussScanRecordBatchesHandle) {
    if !batches.is_null() {
        unsafe {
            let _ = Box::from_raw(batches);
        }
    }
}

/// Get total rows read from scanner (for offset tracking)
#[no_mangle]
pub extern "C" fn fluss_scanner_get_total_rows_read(scanner: *const FlussScannerHandle) -> i64 {
    if scanner.is_null() {
        return 0;
    }
    unsafe {
        (*scanner).total_rows_read
    }
}

/// Free Fluss admin
#[no_mangle]
pub extern "C" fn fluss_admin_free(admin: *mut FlussAdminHandle) {
    if !admin.is_null() {
        unsafe {
            let _ = Box::from_raw(admin);
        }
    }
}

/// Free Fluss table
#[no_mangle]
pub extern "C" fn fluss_table_free(table: *mut FlussTableHandle) {
    if !table.is_null() {
        unsafe {
            let _ = Box::from_raw(table);
        }
    }
}

/// Free Fluss scanner
#[no_mangle]
pub extern "C" fn fluss_scanner_free(scanner: *mut FlussScannerHandle) {
    if !scanner.is_null() {
        unsafe {
            let _ = Box::from_raw(scanner);
        }
    }
}

/// Free Fluss error
#[no_mangle]
pub extern "C" fn fluss_error_free(error: *mut FlussError) {
    if !error.is_null() {
        unsafe {
            let error_ref = Box::from_raw(error);
            if !error_ref.message.is_null() {
                let _ = CString::from_raw(error_ref.message);
            }
        }
    }
}

/// Get Fluss error message
#[no_mangle]
pub extern "C" fn fluss_error_get_message(error: *const FlussError) -> *const c_char {
    if error.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        (*error).message
    }
}

fn create_fluss_error(msg: &str) -> *mut FlussError {
    let c_msg = match CString::new(msg) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    };
    Box::into_raw(Box::new(FlussError { message: c_msg }))
}
