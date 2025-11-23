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

// Table schema functions (without scanning)
PaimonTable* paimon_table_get_schema(PaimonCatalog* catalog, const char* database, const char* table, PaimonError** error_out);
int32_t paimon_table_get_column_count(PaimonTable* table);
const char* paimon_table_get_column_name(PaimonTable* table, int32_t index);
const char* paimon_table_get_column_type(PaimonTable* table, int32_t index);
void paimon_table_free(PaimonTable* table);

// Table scan functions
PaimonScan* paimon_table_scan(PaimonCatalog* catalog, const char* database, const char* table, PaimonError** error_out);

// Schema functions (for scan)
int32_t paimon_scan_get_column_count(PaimonScan* scan);
const char* paimon_scan_get_column_name(PaimonScan* scan, int32_t index);
const char* paimon_scan_get_column_type(PaimonScan* scan, int32_t index);

// Arrow C Data Interface export functions
int32_t paimon_scan_export_schema(PaimonScan* scan, struct ArrowSchema* out_schema);
int32_t paimon_scan_export_batch(PaimonScan* scan, int32_t index, struct ArrowArray* out_array);

// Batch functions
int32_t paimon_scan_get_batch_count(PaimonScan* scan);
const void* paimon_scan_get_batch(PaimonScan* scan, int32_t index); // Deprecated

// Cleanup functions
void paimon_scan_free(PaimonScan* scan);
void paimon_error_free(PaimonError* error);
const char* paimon_error_get_message(const PaimonError* error);

#ifdef __cplusplus
}
#endif

