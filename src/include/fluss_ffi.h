#pragma once

#include <cstdint>

// Forward declarations for Arrow C Data Interface
struct ArrowSchema;
struct ArrowArray;

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
struct PaimonCatalog;
struct PaimonTable;
struct PaimonScan;
struct PaimonError;

// Catalog functions
PaimonCatalog* paimon_catalog_new(const char* warehouse_path);
void paimon_catalog_free(PaimonCatalog* catalog);

// Table functions (without scanning)
PaimonTable* get_paimon_table(PaimonCatalog* catalog, const char* database, const char* table, PaimonError** error_out);
int32_t paimon_table_get_column_count(PaimonTable* table);
const char* paimon_table_get_column_name(PaimonTable* table, int32_t index);
const char* paimon_table_get_column_type(PaimonTable* table, int32_t index);
int32_t paimon_table_export_schema(PaimonTable* table, struct ArrowSchema* out_schema);
void paimon_table_free(PaimonTable* table);

// Table scan functions
// Uses the Table object stored in PaimonTable
PaimonScan* paimon_table_scan(PaimonTable* paimon_table, PaimonError** error_out);

// Arrow C Data Interface export functions
int32_t paimon_scan_export_batch(PaimonScan* scan, struct ArrowArray* out_array);

// Cleanup functions
void paimon_scan_free(PaimonScan* scan);
void paimon_error_free(PaimonError* error);
const char* paimon_error_get_message(const PaimonError* error);

// ============================================================================
// Fluss FFI functions
// ============================================================================

// Forward declarations for Fluss
struct FlussConnectionHandle;
struct FlussAdminHandle;
struct FlussTableHandle;
struct FlussTableInfoHandle;
struct FlussScannerHandle;
struct FlussScanRecordBatchesHandle;
struct FlussError;
struct LakeSnapshotHandle;
struct FlussOffsetMap;
struct FlussPropertyMap;

// Fluss Connection functions
FlussConnectionHandle* fluss_connection_new(const char* bootstrap_server);
void fluss_connection_free(FlussConnectionHandle* conn);

// Fluss Admin functions
FlussAdminHandle* fluss_connection_get_admin(FlussConnectionHandle* conn, FlussError** error_out);
void fluss_admin_free(FlussAdminHandle* admin);
int32_t fluss_admin_get_table(FlussAdminHandle* admin, const char* database, const char* table, struct ArrowSchema* out_schema, FlussError** error_out);
FlussTableInfoHandle* fluss_admin_get_table_info(FlussAdminHandle* admin, const char* database, const char* table, FlussError** error_out);
LakeSnapshotHandle* fluss_admin_get_latest_lake_snapshot(FlussAdminHandle* admin, const char* database, const char* table, FlussError** error_out);
FlussOffsetMap* fluss_admin_list_offsets(FlussAdminHandle* admin, const char* database, const char* table, const int32_t* bucket_ids, int32_t bucket_count, int32_t offset_spec_type, int64_t offset_spec_timestamp, FlussError** error_out);

// Fluss Table functions
FlussTableHandle* fluss_connection_get_table(FlussConnectionHandle* conn, const char* database, const char* table, FlussError** error_out);
void fluss_table_free(FlussTableHandle* table);
FlussScannerHandle* fluss_table_create_record_batch_log_scanner(FlussTableHandle* table, FlussError** error_out);

// Fluss Scanner functions
int32_t fluss_scanner_subscribe_batch(FlussScannerHandle* scanner, const int32_t* bucket_ids, const int64_t* offsets, int32_t count, FlussError** error_out);
int32_t fluss_scanner_unsubscribe_batch(FlussScannerHandle* scanner, int32_t bucket_id, int64_t table_id, FlussError** error_out);
FlussScanRecordBatchesHandle* fluss_scanner_poll_batches(FlussScannerHandle* scanner, int64_t timeout_ms, FlussError** error_out);
int64_t fluss_scanner_get_total_rows_read(FlussScannerHandle* scanner);
void fluss_scanner_free(FlussScannerHandle* scanner);

// Fluss ScanRecordBatches functions
struct FlussScanRecordBatchesHandle;
size_t fluss_scan_record_batches_get_bucket_count(const FlussScanRecordBatchesHandle* batches);
size_t fluss_scan_record_batches_get_bucket_ids(const FlussScanRecordBatchesHandle* batches, int32_t* out_bucket_ids, size_t max_count);
int32_t fluss_scan_record_batches_get_batch_for_bucket(FlussScanRecordBatchesHandle* batches, int32_t bucket_id, struct ArrowArray* out_array);
void fluss_scan_record_batches_free(FlussScanRecordBatchesHandle* batches);

// Fluss TableInfo functions
int32_t fluss_table_info_export_schema(FlussTableInfoHandle* table_info, struct ArrowSchema* out_schema, FlussError** error_out);
int64_t fluss_table_info_get_table_id(FlussTableInfoHandle* table_info);
int32_t fluss_table_info_get_num_buckets(FlussTableInfoHandle* table_info);
FlussPropertyMap* fluss_table_info_get_properties_with_prefix(FlussTableInfoHandle* table_info, const char* prefix, FlussError** error_out);
void fluss_table_info_free(FlussTableInfoHandle* table_info);

// Fluss PropertyMap functions
size_t fluss_property_map_get_count(const FlussPropertyMap* map);
const char* fluss_property_map_get_key(const FlussPropertyMap* map, size_t index);
const char* fluss_property_map_get_value(const FlussPropertyMap* map, size_t index);
void fluss_property_map_free(FlussPropertyMap* map);

// Fluss LakeSnapshot functions
int64_t fluss_lake_snapshot_get_snapshot_id(const LakeSnapshotHandle* snapshot);
int64_t fluss_lake_snapshot_get_bucket_offset(const LakeSnapshotHandle* snapshot, int64_t table_id, int32_t bucket_id);
void fluss_lake_snapshot_free(LakeSnapshotHandle* snapshot);

// Fluss OffsetMap functions
int64_t fluss_offset_map_get_offset(const FlussOffsetMap* map, int32_t bucket_id);
size_t fluss_offset_map_get_count(const FlussOffsetMap* map);
size_t fluss_offset_map_get_bucket_ids(const FlussOffsetMap* map, int32_t* out_bucket_ids, size_t max_count);
void fluss_offset_map_free(FlussOffsetMap* map);

// Fluss Error functions
void fluss_error_free(FlussError* error);
const char* fluss_error_get_message(const FlussError* error);

#ifdef __cplusplus
}
#endif
